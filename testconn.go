package main

import (
	"context"
	"fmt"
	"time"

	"github.com/zwiron/connector"
	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
)

// handleTestConnection decrypts the config, connects to the database,
// discovers tables, and reports the result back to Atlas.
func (a *Agent) handleTestConnection(ctx context.Context, cmd *agentv1.TestConnection) {
	requestID := cmd.GetRequestId()
	connectorType := cmd.GetConnectorType()

	// Recover from panics so the agent always sends a result back.
	defer func() {
		if r := recover(); r != nil {
			a.log.Error(ctx, "agent.test_connection.panic",
				"request_id", requestID,
				"panic", fmt.Sprintf("%v", r),
			)
			a.sendTestResult(requestID, cmd.GetConnectionName(), connectorType, false,
				fmt.Sprintf("internal error: %v", r), 0, nil)
		}
	}()

	// Use a 25s deadline so we always respond within Atlas's 30s timeout.
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	a.log.Info(ctx, "agent.test_connection.start",
		"request_id", requestID,
		"connector_type", connectorType,
		"connection_name", cmd.GetConnectionName(),
		"encrypted_config_len", len(cmd.GetEncryptedConfig()),
	)

	start := time.Now()

	// Decrypt config.
	cfg, err := DecryptConfig(a.keys.Private, cmd.GetEncryptedConfig())
	if err != nil {
		a.sendTestResult(requestID, cmd.GetConnectionName(), connectorType, false,
			fmt.Sprintf("decrypt config: %v", err), 0, nil)
		return
	}

	// Get the source connector (test connections are always source-side).
	src, err := connector.GetSource(connector.ConnectorType(connectorType))
	if err != nil {
		// Try destination if not a source.
		dst, dstErr := connector.GetDestination(connector.ConnectorType(connectorType))
		if dstErr != nil {
			a.sendTestResult(requestID, cmd.GetConnectionName(), connectorType, false,
				fmt.Sprintf("unknown connector type: %s", connectorType), 0, nil)
			return
		}

		// Test destination connectivity.
		if err := dst.Check(ctx, connector.Config(cfg)); err != nil {
			latency := int32(time.Since(start).Milliseconds())
			a.sendTestResult(requestID, cmd.GetConnectionName(), connectorType, false,
				err.Error(), latency, nil)
			return
		}

		latency := int32(time.Since(start).Milliseconds())
		a.sendTestResult(requestID, cmd.GetConnectionName(), connectorType, true, "", latency, nil)
		return
	}

	// Test source connectivity.
	if err := src.Check(ctx, connector.Config(cfg)); err != nil {
		latency := int32(time.Since(start).Milliseconds())
		a.sendTestResult(requestID, cmd.GetConnectionName(), connectorType, false,
			err.Error(), latency, nil)
		return
	}

	// Connect and discover tables.
	var tables []string
	if conn, ok := src.(connector.Connection); ok {
		if err := conn.Connect(ctx, connector.Config(cfg)); err == nil {
			defer conn.Close(ctx)

			catalog, err := src.Discover(ctx, connector.Config(cfg))
			if err == nil {
				for _, s := range catalog.Streams {
					name := s.Name
					if s.Namespace != "" {
						name = s.Namespace + "." + s.Name
					}
					tables = append(tables, name)
				}
			}
		}
	}

	latency := int32(time.Since(start).Milliseconds())

	a.log.Info(ctx, "agent.test_connection.success",
		"request_id", requestID,
		"latency_ms", latency,
		"tables", len(tables),
	)

	a.sendTestResult(requestID, cmd.GetConnectionName(), connectorType, true, "", latency, tables)
}

func (a *Agent) sendTestResult(requestID, connName, connType string, success bool, errMsg string, latencyMs int32, tables []string) {
	a.log.Info(nil, "agent.test_connection.sending_result",
		"request_id", requestID,
		"success", success,
		"error", errMsg,
	)
	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_TestResult{
			TestResult: &agentv1.TestConnectionResult{
				RequestId:      requestID,
				ConnectionName: connName,
				ConnectorType:  connType,
				Success:        success,
				ErrorMessage:   errMsg,
				LatencyMs:      latencyMs,
				Tables:         tables,
			},
		},
	})
}
