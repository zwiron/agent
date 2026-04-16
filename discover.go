package main

import (
	"context"
	"fmt"
	"time"

	"github.com/zwiron/connector"
	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
)

// handleDiscoverSchema introspects a connection and returns full schema info.
func (a *Agent) handleDiscoverSchema(ctx context.Context, cmd *agentv1.DiscoverSchema) {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	requestID := cmd.GetRequestId()
	connectorType := cmd.GetConnectorType()

	a.log.Info(ctx, "agent.discover_schema.start",
		"request_id", requestID,
		"connector_type", connectorType,
	)

	cfg, err := DecryptConfig(a.keys.Private, cmd.GetEncryptedConfig())
	if err != nil {
		a.sendDiscoverResult(requestID, false, fmt.Sprintf("decrypt config: %v", err), nil)
		return
	}

	src, err := connector.GetSource(connector.ConnectorType(connectorType))
	if err != nil {
		a.sendDiscoverResult(requestID, false, fmt.Sprintf("unknown connector type: %s", connectorType), nil)
		return
	}

	// Connect first if the source supports it.
	if conn, ok := src.(connector.Connection); ok {
		if err := conn.Connect(ctx, connector.Config(cfg)); err != nil {
			a.sendDiscoverResult(requestID, false, fmt.Sprintf("connect: %v", err), nil)
			return
		}
		defer conn.Close(ctx)
	}

	catalog, err := src.Discover(ctx, connector.Config(cfg))
	if err != nil {
		a.sendDiscoverResult(requestID, false, fmt.Sprintf("discover: %v", err), nil)
		return
	}

	var tables []*agentv1.TableSchema
	for _, s := range catalog.Streams {
		ts := &agentv1.TableSchema{
			Namespace:  s.Namespace,
			Name:       s.Name,
			PrimaryKey: s.PrimaryKey,
		}

		if s.Schema != nil {
			for i := 0; i < s.Schema.NumFields(); i++ {
				f := s.Schema.Field(i)
				ts.Columns = append(ts.Columns, &agentv1.ColumnSchema{
					Name:     f.Name,
					Type:     f.Type.String(),
					Nullable: f.Nullable,
				})
			}
		}

		tables = append(tables, ts)
	}

	a.log.Info(ctx, "agent.discover_schema.success",
		"request_id", requestID,
		"tables", len(tables),
	)

	a.sendDiscoverResult(requestID, true, "", tables)
}

func (a *Agent) sendDiscoverResult(requestID string, success bool, errMsg string, tables []*agentv1.TableSchema) {
	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_DiscoverSchemaResult{
			DiscoverSchemaResult: &agentv1.DiscoverSchemaResult{
				RequestId:    requestID,
				Success:      success,
				ErrorMessage: errMsg,
				Tables:       tables,
			},
		},
	})
}
