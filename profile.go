package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/zwiron/connector"
	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
)

// handleProfileConnection decrypts the config, connects, runs ProfileResult(),
// and sends the result back to Atlas. All operations are read-only.
func (a *Agent) handleProfileConnection(ctx context.Context, cmd *agentv1.ProfileConnection) {
	requestID := cmd.GetRequestId()
	connectorType := cmd.GetConnectorType()

	a.log.Info(ctx, "agent.profile_connection.start",
		"request_id", requestID,
		"connector_type", connectorType,
		"connection_name", cmd.GetConnectionName(),
	)

	start := time.Now()

	// Decrypt config.
	cfg, err := DecryptConfig(a.keys.Private, cmd.GetEncryptedConfig())
	if err != nil {
		a.sendProfileResult(requestID, false, fmt.Sprintf("decrypt config: %v", err), nil)
		return
	}

	// Get the connector (try source first, then destination).
	var conn connector.Connection
	src, err := connector.GetSource(connector.ConnectorType(connectorType))
	if err != nil {
		dst, dstErr := connector.GetDestination(connector.ConnectorType(connectorType))
		if dstErr != nil {
			a.sendProfileResult(requestID, false,
				fmt.Sprintf("unknown connector type: %s", connectorType), nil)
			return
		}
		if c, ok := dst.(connector.Connection); ok {
			conn = c
		}
	} else {
		if c, ok := src.(connector.Connection); ok {
			conn = c
		}
	}

	if conn == nil {
		a.sendProfileResult(requestID, false, "connector does not implement Connection", nil)
		return
	}

	// Connect.
	if err := conn.Connect(ctx, connector.Config(cfg)); err != nil {
		a.sendProfileResult(requestID, false, fmt.Sprintf("connect: %v", err), nil)
		return
	}
	defer conn.Close(ctx)

	// Profile.
	profiler, ok := conn.(connector.ConnectionProfiler)
	if !ok {
		a.sendProfileResult(requestID, false, "connector does not support profiling", nil)
		return
	}

	result, err := profiler.Profile(ctx)
	if err != nil {
		a.sendProfileResult(requestID, false, fmt.Sprintf("profile: %v", err), nil)
		return
	}

	profileJSON, err := json.Marshal(result)
	if err != nil {
		a.sendProfileResult(requestID, false, fmt.Sprintf("marshal profile: %v", err), nil)
		return
	}

	latency := time.Since(start)
	a.log.Info(ctx, "agent.profile_connection.success",
		"request_id", requestID,
		"latency_ms", latency.Milliseconds(),
		"tables", len(result.Tables),
		"capabilities", len(result.Capabilities),
	)

	a.sendProfileResult(requestID, true, "", profileJSON)
}

func (a *Agent) sendProfileResult(requestID string, success bool, errMsg string, profileJSON []byte) {
	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_ProfileResult{
			ProfileResult: &agentv1.ProfileConnectionResult{
				RequestId:    requestID,
				Success:      success,
				ErrorMessage: errMsg,
				ProfileJson:  profileJSON,
			},
		},
	})
}
