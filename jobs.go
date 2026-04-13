package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zwiron/connector"
	"github.com/zwiron/engine/runner"
	"github.com/zwiron/engine/transform"
	"github.com/zwiron/engine/config"
	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// =========================================================================
// gRPC Reporter — bridges runner.Reporter to the agent's gRPC stream.

type gRPCReporter struct {
	agent *Agent
	mu    sync.Mutex
	runs  map[string]string // jobID → runID
}

func newGRPCReporter(a *Agent) *gRPCReporter {
	return &gRPCReporter{agent: a, runs: make(map[string]string)}
}

func (r *gRPCReporter) register(jobID, runID string) {
	r.mu.Lock()
	r.runs[jobID] = runID
	r.mu.Unlock()
}

func (r *gRPCReporter) unregister(jobID string) {
	r.mu.Lock()
	delete(r.runs, jobID)
	r.mu.Unlock()
}

func (r *gRPCReporter) runID(jobID string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runs[jobID]
}

func (r *gRPCReporter) runningJobs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	jobs := make([]string, 0, len(r.runs))
	for id := range r.runs {
		jobs = append(jobs, id)
	}
	return jobs
}

func (r *gRPCReporter) ReportProgress(jobID string, p runner.Progress) {
	runID := r.runID(jobID)
	r.agent.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobProgress{
			JobProgress: &agentv1.JobProgress{
				JobId:          jobID,
				TotalRows:      p.TotalRows,
				RowsCompleted:  p.TotalRows,
				RowsPerSec:     p.RowsPerSec,
				Pct:            p.Pct,
				TasksTotal:     int32(p.TasksTotal),
				TasksCompleted: int32(p.TasksCompleted),
				TasksRunning:   int32(p.TasksRunning),
				TasksPending:   int32(p.TasksPending),
				TasksFailed:    int32(p.TasksFailed),
				Timestamp:      timestamppb.Now(),
				ReadWorkers:    int32(p.ReadWorkers),
				WriteWorkers:   int32(p.WriteWorkers),
				AvgRps:         p.AvgRps,
				WriteStrategy:  p.WriteStrategy,
				RunId:          runID,
			},
		},
	})
}

func (r *gRPCReporter) ReportEvent(jobID string, ev config.Event) {
	r.agent.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobEvent{
			JobEvent: &agentv1.JobEvent{
				JobId:     jobID,
				EventType: string(ev.Type),
				Table:     ev.Table,
				Rows:      ev.Rows,
				Error:     ev.Error,
				Timestamp: timestamppb.Now(),
			},
		},
	})
}

func (r *gRPCReporter) ReportProposal(jobID string, p runner.SchemaProposal) {
	runID := r.runID(jobID)
	r.agent.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_SchemaProposal{
			SchemaProposal: &agentv1.SchemaChangeProposal{
				ProposalId:    p.ID,
				JobId:         jobID,
				RunId:         runID,
				TableName:     p.TableName,
				Status:        p.Status,
				ChangesJson:   p.ChangesJSON,
				OldSchemaJson: p.OldSchemaJSON,
				NewSchemaJson: p.NewSchemaJSON,
				AutoApplied:   p.AutoApplied,
				CreatedAt:     timestamppb.New(p.CreatedAt),
			},
		},
	})
}

func (r *gRPCReporter) ReportCompleted(jobID string, result runner.Result) {
	runID := r.runID(jobID)
	r.agent.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobCompleted{
			JobCompleted: &agentv1.JobCompleted{
				JobId:       jobID,
				TotalRows:   result.TotalRows,
				RowsPerSec:  result.RowsPerSec,
				DurationMs:  result.DurationMs,
				CompletedAt: timestamppb.Now(),
				RunId:       runID,
			},
		},
	})
}

func (r *gRPCReporter) ReportFailed(jobID string, err error, result runner.Result) {
	runID := r.runID(jobID)
	r.agent.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobFailed{
			JobFailed: &agentv1.JobFailed{
				JobId:         jobID,
				ErrorMessage:  err.Error(),
				RowsCompleted: result.TotalRows,
				DurationMs:    result.DurationMs,
				FailedAt:      timestamppb.Now(),
				RunId:         runID,
			},
		},
	})
}

// =========================================================================
// File-based CDC Store — wraps agent's JSON-file persistence.

type fileCDCStore struct {
	agent *Agent
}

func (s *fileCDCStore) LoadPosition(key string) ([]byte, error) {
	return s.agent.loadCDCPosition(key)
}

func (s *fileCDCStore) SavePosition(key string, position []byte) error {
	return s.agent.saveCDCPosition(key, position)
}

// =========================================================================
// Runner initialization

// initRunner creates the runner.Runner for the agent. Called once during startup.
func (a *Agent) initRunner() {
	a.reporter = newGRPCReporter(a)
	a.jobRunner = runner.New(runner.Config{
		Log:              a.log,
		Reporter:         a.reporter,
		CDCStore:         &fileCDCStore{agent: a},
		CheckpointDir:    a.dataDir,
		ProgressInterval: 3 * time.Second,
	})
}

// sendJobFailed sends a JobFailed event for pre-run errors (decrypt failure, etc.).
func (a *Agent) sendJobFailed(jobID, runID, errMsg string, rowsCompleted, durationMs int64) {
	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobFailed{
			JobFailed: &agentv1.JobFailed{
				JobId:         jobID,
				ErrorMessage:  errMsg,
				RowsCompleted: rowsCompleted,
				DurationMs:    durationMs,
				FailedAt:      timestamppb.Now(),
				RunId:         runID,
			},
		},
	})
}

// =========================================================================
// Job handling

// handleStartJob decrypts connection configs and runs the engine via runner.Runner.
func (a *Agent) handleStartJob(ctx context.Context, cmd *agentv1.StartJob) {
	jobID := cmd.GetJobId()
	runID := cmd.GetRunId()

	// Enforce concurrency limit if configured.
	if a.jobSem != nil {
		select {
		case a.jobSem <- struct{}{}:
			defer func() { <-a.jobSem }()
		default:
			a.log.Warn(ctx, "agent.job.rejected_concurrency_limit",
				"job_id", jobID,
				"max_jobs", a.maxJobs,
			)
			a.sendJobFailed(jobID, runID, fmt.Sprintf("agent at concurrency limit (%d)", a.maxJobs), 0, 0)
			return
		}
	}

	// Track metrics.
	if a.metrics != nil {
		a.metrics.JobsRunning.Inc()
		a.metrics.JobsTotal.Inc()
		defer a.metrics.JobsRunning.Dec()
	}

	tracer := otel.Tracer("zwiron.agent")

	// Continue the trace from Atlas scheduler if traceparent was provided.
	if tp := cmd.GetTraceParent(); tp != "" {
		carrier := propagation.MapCarrier{"traceparent": tp}
		ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	}

	ctx, span := tracer.Start(ctx, "agent.execute_job",
		trace.WithAttributes(
			attribute.String("job_id", jobID),
			attribute.String("run_id", runID),
			attribute.String("source_type", cmd.GetSource().GetConnectorType()),
			attribute.String("dest_type", cmd.GetDest().GetConnectorType()),
			attribute.String("sync_mode", cmd.GetSyncMode()),
		),
	)
	defer span.End()

	a.log.Info(ctx, "agent.job.start",
		"job_id", jobID,
		"run_id", runID,
		"tables", cmd.GetTables(),
		"workers", cmd.GetWorkers(),
	)

	// Decrypt source and destination configs.
	_, decryptSpan := tracer.Start(ctx, "agent.decrypt_config")
	srcConfig, err := DecryptConfig(a.keys.Private, cmd.GetSource().GetEncryptedConfig())
	if err != nil {
		decryptSpan.RecordError(err)
		decryptSpan.SetStatus(otelcodes.Error, err.Error())
		decryptSpan.End()
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		a.sendJobFailed(jobID, runID, fmt.Sprintf("decrypt source config: %v", err), 0, 0)
		return
	}

	dstConfig, err := DecryptConfig(a.keys.Private, cmd.GetDest().GetEncryptedConfig())
	if err != nil {
		decryptSpan.RecordError(err)
		decryptSpan.SetStatus(otelcodes.Error, err.Error())
		decryptSpan.End()
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		a.sendJobFailed(jobID, runID, fmt.Sprintf("decrypt dest config: %v", err), 0, 0)
		return
	}
	decryptSpan.End()

	// Register runID so the reporter can include it in gRPC messages.
	a.reporter.register(jobID, runID)
	defer a.reporter.unregister(jobID)

	// Build job spec for the unified runner.
	spec := runner.JobSpec{
		JobID:        jobID,
		RunID:        runID,
		SourceType:   cmd.GetSource().GetConnectorType(),
		SourceConfig: connector.Config(srcConfig),
		DestType:     cmd.GetDest().GetConnectorType(),
		DestConfig:   connector.Config(dstConfig),
		Tables:       cmd.GetTables(),
		Workers:      int(cmd.GetWorkers()),
		MaxRetries:   int(cmd.GetMaxRetries()),
		SyncMode:     cmd.GetSyncMode(),
		DestSyncMode: cmd.GetDestSyncMode(),
		Transforms:   protoToTransformSpec(cmd.GetTransforms()),
		FreshStart:   cmd.GetFreshStart(),
		CDCKey:       cdcPositionKey(cmd),
	}

	// Run the engine via the unified runner. The runner handles:
	// checkpoint, CDC position, progress reporting, schema proposals,
	// and completion/failure reporting via the gRPCReporter.
	result, runErr := a.jobRunner.Run(ctx, spec)

	// Record metrics.
	if a.metrics != nil {
		a.metrics.JobDuration.Observe(float64(result.DurationMs) / 1000.0)
	}

	if runErr != nil {
		span.RecordError(runErr)
		span.SetStatus(otelcodes.Error, runErr.Error())
		if a.metrics != nil {
			a.metrics.JobsFailed.Inc()
		}
		return
	}

	// Successful completion — record metrics and tracing.
	if a.metrics != nil {
		a.metrics.RowsTotal.Add(float64(result.TotalRows))
	}
	span.SetAttributes(
		attribute.Int64("total_rows", result.TotalRows),
		attribute.Int64("duration_ms", result.DurationMs),
	)

	// Post-sync row count validation (background, best-effort).
	valCtx, valCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	go func() {
		defer valCancel()
		a.validateJob(valCtx, jobID, cmd, srcConfig, dstConfig)
	}()
}

// validateJob opens fresh connections to source and destination, runs
// SELECT COUNT(*) for each synced table, and sends a JobValidation event.
func (a *Agent) validateJob(ctx context.Context, jobID string, cmd *agentv1.StartJob, srcConfig, dstConfig connector.Config) {
	ctx, valSpan := otel.Tracer("zwiron.agent").Start(ctx, "agent.validate_job",
		trace.WithAttributes(attribute.String("job_id", jobID)),
	)
	defer valSpan.End()

	a.log.Info(ctx, "agent.validate.start", "job_id", jobID)

	srcType := connector.ConnectorType(cmd.GetSource().GetConnectorType())
	dstType := connector.ConnectorType(cmd.GetDest().GetConnectorType())

	// Open source connection.
	src, err := connector.GetSource(srcType)
	if err != nil {
		a.log.Error(ctx, "agent.validate.source_registry", "job_id", jobID, "error", err)
		return
	}
	srcConn, ok := src.(connector.Connection)
	if !ok {
		a.log.Error(ctx, "agent.validate.source_not_connection", "job_id", jobID)
		return
	}
	if err := srcConn.Connect(ctx, srcConfig); err != nil {
		a.log.Error(ctx, "agent.validate.source_connect", "job_id", jobID, "error", err)
		return
	}
	defer srcConn.Close(ctx)

	// Open destination connection.
	dst, err := connector.GetDestination(dstType)
	if err != nil {
		a.log.Error(ctx, "agent.validate.dest_registry", "job_id", jobID, "error", err)
		return
	}
	dstConn, ok := dst.(connector.Connection)
	if !ok {
		a.log.Error(ctx, "agent.validate.dest_not_connection", "job_id", jobID)
		return
	}
	if err := dstConn.Connect(ctx, dstConfig); err != nil {
		a.log.Error(ctx, "agent.validate.dest_connect", "job_id", jobID, "error", err)
		return
	}
	defer dstConn.Close(ctx)

	srcCounter, srcOK := src.(connector.RowCounter)
	dstCounter, dstOK := dst.(connector.RowCounter)
	if !srcOK || !dstOK {
		a.log.Warn(ctx, "agent.validate.no_row_counter", "job_id", jobID, "src", srcOK, "dst", dstOK)
		return
	}

	var tables []*agentv1.TableValidation
	for _, tbl := range cmd.GetTables() {
		tv := &agentv1.TableValidation{TableName: tbl}

		srcRows, err := srcCounter.RowCount(ctx, tbl)
		if err != nil {
			errMsg := err.Error()
			tv.Error = errMsg
			tables = append(tables, tv)
			continue
		}
		tv.SourceRows = srcRows

		dstRows, err := dstCounter.RowCount(ctx, tbl)
		if err != nil {
			errMsg := err.Error()
			tv.Error = errMsg
			tv.SourceRows = srcRows
			tables = append(tables, tv)
			continue
		}
		tv.DestRows = dstRows
		tv.Match = srcRows == dstRows
		tables = append(tables, tv)

		a.log.Info(ctx, "agent.validate.table",
			"job_id", jobID,
			"table", tbl,
			"source_rows", srcRows,
			"dest_rows", dstRows,
			"match", tv.Match,
		)
	}

	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobValidation{
			JobValidation: &agentv1.JobValidation{
				JobId:       jobID,
				Tables:      tables,
				ValidatedAt: timestamppb.Now(),
			},
		},
	})

	a.log.Info(ctx, "agent.validate.done", "job_id", jobID, "tables", len(tables))
}

// handleCancelJob cancels a running job.
func (a *Agent) handleCancelJob(ctx context.Context, cmd *agentv1.CancelJob) {
	jobID := cmd.GetJobId()
	a.log.Info(ctx, "agent.job.cancel", "job_id", jobID, "reason", cmd.GetReason())

	if a.jobRunner.CancelJob(jobID) {
		a.log.Info(ctx, "agent.job.cancelled", "job_id", jobID)
	} else {
		a.log.Warn(ctx, "agent.job.cancel.not_found", "job_id", jobID)
	}
}

// protoToTransformSpec converts proto TransformRule messages into an engine transform.Spec.
func protoToTransformSpec(rules []*agentv1.TransformRule) *transform.Spec {
	if len(rules) == 0 {
		return nil
	}
	spec := &transform.Spec{
		Tables: make([]transform.TableTransform, 0, len(rules)),
	}
	for _, r := range rules {
		tt := transform.TableTransform{Table: r.GetTable()}
		for _, c := range r.GetColumns() {
			tt.Columns = append(tt.Columns, transform.ColumnTransform{
				Source: c.GetSource(),
				Dest:   c.GetDest(),
				Cast:   c.GetCast(),
				Drop:   c.GetDrop(),
			})
		}
		for _, f := range r.GetFilterConditions() {
			tt.Filters = append(tt.Filters, transform.FilterCondition{
				Column: f.GetColumn(),
				Op:     f.GetOp(),
				Value:  f.GetValue(),
			})
		}
		spec.Tables = append(spec.Tables, tt)
	}
	return spec
}

// =========================================================================
// CDC Position Persistence
//
// CDC positions are persisted to a JSON file keyed by a hash of the
// source + destination connection. This enables resumption across job
// re-runs (scheduled or manual) without requiring proto changes.

// cdcPositionKey generates a deterministic key for a CDC position based on
// the source and destination connection identifiers.
func cdcPositionKey(cmd *agentv1.StartJob) string {
	h := sha256.New()
	h.Write([]byte(cmd.GetSource().GetConnectionId()))
	h.Write([]byte(":"))
	h.Write([]byte(cmd.GetDest().GetConnectionId()))
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// cdcPositionsFile returns the path to the CDC positions JSON file.
func (a *Agent) cdcPositionsFile() string {
	return filepath.Join(a.dataDir, "cdc-positions.json")
}

// loadCDCPositions loads all CDC positions from the persistent store.
func (a *Agent) loadCDCPositions() (map[string][]byte, error) {
	data, err := os.ReadFile(a.cdcPositionsFile())
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string][]byte), nil
		}
		return nil, err
	}
	var positions map[string][]byte
	if err := json.Unmarshal(data, &positions); err != nil {
		return make(map[string][]byte), nil
	}
	return positions, nil
}

// loadCDCPosition loads a single CDC position by key.
func (a *Agent) loadCDCPosition(key string) ([]byte, error) {
	positions, err := a.loadCDCPositions()
	if err != nil {
		return nil, err
	}
	return positions[key], nil
}

// saveCDCPosition saves a CDC position to the persistent store.
func (a *Agent) saveCDCPosition(key string, position []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	positions, _ := a.loadCDCPositions()
	positions[key] = position

	data, err := json.Marshal(positions)
	if err != nil {
		return fmt.Errorf("marshal cdc positions: %w", err)
	}
	return os.WriteFile(a.cdcPositionsFile(), data, 0600)
}
