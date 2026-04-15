package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/zwiron/engine/runner"
	"github.com/zwiron/pkg/logger"
	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Agent is the main runtime that connects to Atlas and processes commands.
type Agent struct {
	log       *logger.Logger
	token     string
	atlasAddr string
	dataDir   string
	keys      *KeyPair

	// Runtime state — set after successful registration.
	agentID string
	stream  agentv1.AgentService_ConnectClient
	sendMu  sync.Mutex // Serializes all stream.Send() calls (gRPC streams are not goroutine-safe).

	// Unified job runner — handles engine execution, checkpointing,
	// progress reporting, CDC persistence, and schema proposals.
	jobRunner *runner.Runner
	reporter  *gRPCReporter

	// Paused state — when true, agent rejects new jobs.
	paused bool
	mu     sync.Mutex

	// Concurrency limit — max simultaneous jobs (0 = unlimited).
	maxJobs int
	jobSem  chan struct{}

	// Atlas CA certificate PEM — received in RegistrationAck and pinned for TLS.
	atlasCAPEM string

	// Prometheus metrics.
	metrics *AgentMetrics
}

// Run connects to Atlas, registers, and enters the main command loop.
// It blocks until the context is cancelled or the connection drops.
func (a *Agent) Run(ctx context.Context) error {
	a.initRunner()

	// Dial Atlas gRPC.
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Load pinned Atlas CA cert if available (received in previous RegistrationAck).
	caPath := filepath.Join(a.dataDir, "atlas-ca.pem")
	hasPinnedCA := false
	if caPEM, err := os.ReadFile(caPath); err == nil {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(caPEM) {
			tlsCfg.RootCAs = pool
			hasPinnedCA = true
			a.log.Info(ctx, "agent.tls.pinned_ca", "path", caPath)
		}
	} else {
		// First connection — no pinned CA yet. Trust the server's cert and
		// pin the CA from RegistrationAck for all future connections.
		tlsCfg.InsecureSkipVerify = true
		a.log.Info(ctx, "agent.tls.first_connect", "msg", "no pinned CA, will trust-on-first-use")
	}

	// Load mTLS client cert+key bundle if previously issued by Atlas.
	// Only load if we have a pinned CA — a stale client cert from a
	// previous CA will cause the server to reject the connection.
	if hasPinnedCA {
		bundlePath := filepath.Join(a.dataDir, "client.pem")
		if bundle, err := os.ReadFile(bundlePath); err == nil {
			if pair, err := tls.X509KeyPair(bundle, bundle); err == nil {
				tlsCfg.Certificates = []tls.Certificate{pair}
				a.log.Info(ctx, "agent.mtls_enabled")
			}
		}
	}

	creds := credentials.NewTLS(tlsCfg)
	opts = append(opts, grpc.WithTransportCredentials(creds))

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

	// Store mTLS client certificate + key if provided.
	if bundle := ack.GetClientCert(); bundle != "" {
		bundlePath := filepath.Join(a.dataDir, "client.pem")
		if err := os.WriteFile(bundlePath, []byte(bundle), 0600); err != nil {
			a.log.Error(ctx, "agent.save_client_cert", "error", err)
		} else {
			a.log.Info(ctx, "agent.client_cert_saved", "path", bundlePath)
		}
	}

	// Pin Atlas CA certificate for TLS verification on future connections.
	if caPEM := ack.GetCaCert(); caPEM != "" {
		caPath := filepath.Join(a.dataDir, "atlas-ca.pem")
		if err := os.WriteFile(caPath, []byte(caPEM), 0600); err != nil {
			a.log.Error(ctx, "agent.save_ca_cert", "error", err)
		} else {
			a.log.Info(ctx, "agent.ca_cert_pinned", "path", caPath)
		}
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

			a.sendMu.Lock()
			err := a.stream.Send(&agentv1.ConnectRequest{
				Payload: &agentv1.ConnectRequest_Heartbeat{
					Heartbeat: &agentv1.Heartbeat{
						Timestamp: timestamppb.Now(),
						Metrics: &agentv1.SystemMetrics{
							MemoryUsedMb:  m.Alloc / (1024 * 1024),
							MemoryTotalMb: m.Sys / (1024 * 1024),
						},
						JobsRunning: int32(len(a.jobSem)),
						MaxJobs:     int32(a.maxJobs),
					},
				},
			})
			a.sendMu.Unlock()
			if err != nil {
				a.log.Error(ctx, "agent.heartbeat.failed", "error", err)
				return
			}
		}
	}
}

// errStreamClosed is returned when the server closes the stream (EOF).
// This is distinct from a context cancellation (clean shutdown) and triggers
// the reconnect loop in main.go.
var errStreamClosed = fmt.Errorf("stream closed by server")

// recvLoop reads commands from Atlas and dispatches them.
func (a *Agent) recvLoop(ctx context.Context) error {
	for {
		msg, err := a.stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				a.log.Info(ctx, "agent.stream.closed", "reason", "shutdown")
				return nil // Clean shutdown — don't reconnect.
			}
			if err == io.EOF {
				a.log.Warn(ctx, "agent.stream.closed", "reason", "server_eof")
				return errStreamClosed // Trigger reconnect.
			}
			return fmt.Errorf("recv: %w", err)
		}

		switch payload := msg.GetPayload().(type) {
		case *agentv1.ConnectResponse_StartJob:
			a.mu.Lock()
			paused := a.paused
			a.mu.Unlock()
			if paused {
				a.log.Warn(ctx, "agent.job.rejected_paused", "job_id", payload.StartJob.GetJobId())
				a.sendJobFailed(payload.StartJob.GetJobId(), payload.StartJob.GetRunId(), "agent is paused", 0, 0)
				continue
			}
			go a.handleStartJob(ctx, payload.StartJob)
		case *agentv1.ConnectResponse_CancelJob:
			a.handleCancelJob(ctx, payload.CancelJob)
		case *agentv1.ConnectResponse_TestConnection:
			go a.handleTestConnection(ctx, payload.TestConnection)
		case *agentv1.ConnectResponse_DiscoverSchema:
			go a.handleDiscoverSchema(ctx, payload.DiscoverSchema)
		case *agentv1.ConnectResponse_PauseAgent:
			a.handlePause(ctx, payload.PauseAgent)
		case *agentv1.ConnectResponse_ResumeAgent:
			a.handleResume(ctx)
		case *agentv1.ConnectResponse_TriggerValidation:
			cmd := payload.TriggerValidation
			a.log.Info(ctx, "agent.trigger_validation", "job_id", cmd.GetJobId())
			go a.runOnDemandValidation(ctx, cmd.GetJobId(), cmd.GetRunId(), cmd)
		case *agentv1.ConnectResponse_ProfileConnection:
			go a.handleProfileConnection(ctx, payload.ProfileConnection)
		case *agentv1.ConnectResponse_CancelValidation:
			a.cancelValidation(ctx, payload.CancelValidation)
		default:
			a.log.Warn(ctx, "agent.unknown_command", "type", fmt.Sprintf("%T", msg.GetPayload()))
		}
	}
}

// sendEvent is a helper to send a message back to Atlas.
// All stream.Send() calls are serialized through sendMu because
// gRPC ClientStream.Send() is not goroutine-safe.
func (a *Agent) sendEvent(event *agentv1.ConnectRequest) {
	a.sendMu.Lock()
	err := a.stream.Send(event)
	a.sendMu.Unlock()
	if err != nil {
		a.log.Error(nil, "agent.send.failed", "error", err)
	}
}

// handlePause stops all running jobs and marks the agent as paused.
func (a *Agent) handlePause(ctx context.Context, cmd *agentv1.PauseAgent) {
	a.log.Info(ctx, "agent.pause", "reason", cmd.GetReason())

	a.mu.Lock()
	a.paused = true
	a.mu.Unlock()

	// Cancel all running jobs through the runner.
	for _, jobID := range a.reporter.runningJobs() {
		a.log.Info(ctx, "agent.pause.cancel_job", "job_id", jobID)
		a.jobRunner.CancelJob(jobID)
	}
}

// handleResume unpauses the agent so it can accept new jobs.
func (a *Agent) handleResume(ctx context.Context) {
	a.log.Info(ctx, "agent.resume")

	a.mu.Lock()
	a.paused = false
	a.mu.Unlock()
}
