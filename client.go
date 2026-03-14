package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/zwiron/pkg/logger"
	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Agent is the main runtime that connects to Atlas and processes commands.
type Agent struct {
	log       *logger.Logger
	token     string
	atlasAddr string
	insecure  bool
	dataDir   string
	keys      *KeyPair

	// Runtime state — set after successful registration.
	agentID string
	stream  agentv1.AgentService_ConnectClient

	// Active jobs — keyed by job ID.
	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

// Run connects to Atlas, registers, and enters the main command loop.
// It blocks until the context is cancelled or the connection drops.
func (a *Agent) Run(ctx context.Context) error {
	a.cancels = make(map[string]context.CancelFunc)

	// Dial Atlas gRPC.
	opts := []grpc.DialOption{grpc.WithDefaultCallOptions()}
	if a.insecure {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	a.log.Info(ctx, "agent.connecting", "addr", a.atlasAddr)

	conn, err := grpc.NewClient(a.atlasAddr, opts...)
	if err != nil {
		return fmt.Errorf("dial atlas: %w", err)
	}
	defer conn.Close()

	client := agentv1.NewAgentServiceClient(conn)

	stream, err := client.Connect(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	a.stream = stream

	// Send registration.
	hostname, _ := os.Hostname()
	if err := stream.Send(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_Registration{
			Registration: &agentv1.AgentRegistration{
				AgentToken: a.token,
				PublicKey:  a.keys.PublicPEM,
				Version:    "0.1.0",
				Os:         runtime.GOOS,
				Arch:       runtime.GOARCH,
				Hostname:   hostname,
			},
		},
	}); err != nil {
		return fmt.Errorf("send registration: %w", err)
	}

	a.log.Info(ctx, "agent.registration.sent")

	// Wait for RegistrationAck.
	msg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("recv registration ack: %w", err)
	}

	ack := msg.GetRegistrationAck()
	if ack == nil {
		return fmt.Errorf("expected RegistrationAck, got %T", msg.GetPayload())
	}
	if !ack.GetAccepted() {
		return fmt.Errorf("registration rejected: %s", ack.GetRejectReason())
	}

	a.agentID = ack.GetAgentId()
	heartbeatInterval := time.Duration(ack.GetHeartbeatIntervalSec()) * time.Second
	if heartbeatInterval <= 0 {
		heartbeatInterval = 10 * time.Second
	}

	a.log.Info(ctx, "agent.registered",
		"agent_id", a.agentID,
		"heartbeat_interval", heartbeatInterval,
	)

	// Start heartbeat goroutine.
	go a.heartbeatLoop(ctx, heartbeatInterval)

	// Main receive loop — dispatch incoming commands.
	return a.recvLoop(ctx)
}

// heartbeatLoop sends periodic heartbeats to Atlas with system metrics.
func (a *Agent) heartbeatLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			if err := a.stream.Send(&agentv1.ConnectRequest{
				Payload: &agentv1.ConnectRequest_Heartbeat{
					Heartbeat: &agentv1.Heartbeat{
						Timestamp: timestamppb.Now(),
						Metrics: &agentv1.SystemMetrics{
							MemoryUsedMb:  m.Alloc / (1024 * 1024),
							MemoryTotalMb: m.Sys / (1024 * 1024),
						},
					},
				},
			}); err != nil {
				a.log.Error(ctx, "agent.heartbeat.failed", "error", err)
				return
			}
		}
	}
}

// recvLoop reads commands from Atlas and dispatches them.
func (a *Agent) recvLoop(ctx context.Context) error {
	for {
		msg, err := a.stream.Recv()
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				a.log.Info(ctx, "agent.stream.closed")
				return nil
			}
			return fmt.Errorf("recv: %w", err)
		}

		switch payload := msg.GetPayload().(type) {
		case *agentv1.ConnectResponse_StartJob:
			go a.handleStartJob(ctx, payload.StartJob)
		case *agentv1.ConnectResponse_CancelJob:
			a.handleCancelJob(ctx, payload.CancelJob)
		case *agentv1.ConnectResponse_TestConnection:
			go a.handleTestConnection(ctx, payload.TestConnection)
		case *agentv1.ConnectResponse_DiscoverSchema:
			go a.handleDiscoverSchema(ctx, payload.DiscoverSchema)
		default:
			a.log.Warn(ctx, "agent.unknown_command", "type", fmt.Sprintf("%T", msg.GetPayload()))
		}
	}
}

// sendEvent is a helper to send a message back to Atlas.
func (a *Agent) sendEvent(event *agentv1.ConnectRequest) {
	if err := a.stream.Send(event); err != nil {
		a.log.Error(nil, "agent.send.failed", "error", err)
	}
}
