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
	"github.com/zwiron/engine/checkpoint"
	"github.com/zwiron/engine/engine"
	"github.com/zwiron/engine/transform"
	"github.com/zwiron/pkg/logger"
	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleStartJob decrypts connection configs and runs the engine.
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

	// Create a cancellable context for this job.
	jobCtx, jobCancel := context.WithCancel(ctx)
	a.mu.Lock()
	if _, alreadyRunning := a.cancels[jobID]; alreadyRunning {
		a.mu.Unlock()
		jobCancel()
		a.log.Warn(ctx, "agent.job.already_running", "job_id", jobID)
		return
	}
	a.cancels[jobID] = jobCancel
	a.mu.Unlock()
	defer func() {
		jobCancel()
		a.mu.Lock()
		delete(a.cancels, jobID)
		a.mu.Unlock()
	}()

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

	// Open checkpoint store for this job.
	cpPath := filepath.Join(a.dataDir, fmt.Sprintf("checkpoint-%s.db", jobID))

	// Fresh start: delete any existing checkpoint so the job starts from scratch.
	if cmd.GetFreshStart() {
		_ = os.Remove(cpPath)
		_ = os.Remove(cpPath + "-wal")
		_ = os.Remove(cpPath + "-shm")
		a.log.Info(ctx, "agent.job.fresh_start", "job_id", jobID)
	}

	store, err := checkpoint.NewSQLiteStore(checkpoint.SQLiteConfig{
		Path:   cpPath,
		Logger: a.log,
	})
	if err != nil {
		a.sendJobFailed(jobID, runID, fmt.Sprintf("open checkpoint: %v", err), 0, 0)
		return
	}
	defer store.Close()

	// Build engine config.
	workers := int(cmd.GetWorkers())
	maxRetries := int(cmd.GetMaxRetries())
	if maxRetries <= 0 {
		maxRetries = 3
	}

	engineCfg := engine.Config{
		SourceType:   cmd.GetSource().GetConnectorType(),
		SourceConfig: connector.Config(srcConfig),
		DestType:     cmd.GetDest().GetConnectorType(),
		DestConfig:   connector.Config(dstConfig),
		Tables:       cmd.GetTables(),
		Workers:      workers,
		MaxRetries:   maxRetries,
		SyncMode:     cmd.GetSyncMode(),
		DestSyncMode: cmd.GetDestSyncMode(),
		Transforms:   protoToTransformSpec(cmd.GetTransforms()),
		OnEvent: func(ev engine.Event) {
			a.sendJobEvent(jobID, ev)

			// Forward full schema proposals to Atlas via gRPC.
			if ev.ProposalID != "" {
				a.forwardSchemaProposal(jobID, runID, ev, store)
			}
		},
	}

	// For CDC mode, load previous position for cross-run resumption.
	cdcKey := cdcPositionKey(cmd)
	if cmd.GetSyncMode() == "cdc" {
		if pos, err := a.loadCDCPosition(cdcKey); err == nil && len(pos) > 0 {
			engineCfg.InitialCDCPosition = pos
			a.log.Info(ctx, "agent.cdc.resume",
				"job_id", jobID,
				"position_bytes", len(pos),
			)
		}
	}

	// Run the engine.
	eng := engine.New(store, a.log)

	// Start progress reporter in background. It reads engine result
	// for metadata (read/write workers, write strategy).
	progressCtx, progressCancel := context.WithCancel(jobCtx)
	var progressWg sync.WaitGroup
	progressWg.Add(1)
	go func() {
		defer progressWg.Done()
		a.reportJobProgress(progressCtx, jobID, runID, store, eng)
	}()

	startTime := time.Now()
	runErr := eng.Run(jobCtx, engineCfg)
	progressCancel()
	progressWg.Wait()
	durationMs := time.Since(startTime).Milliseconds()

	// Record job duration metric.
	if a.metrics != nil {
		a.metrics.JobDuration.Observe(float64(durationMs) / 1000.0)
	}

	// Send final progress snapshot so Atlas has accurate task/worker counts.
	a.sendProgressSnapshot(jobID, runID, store, eng)

	// Send final status.
	if runErr != nil {
		a.log.Error(ctx, "agent.job.failed", "job_id", jobID, "error", runErr)
		span.RecordError(runErr)
		span.SetStatus(otelcodes.Error, runErr.Error())
		if a.metrics != nil {
			a.metrics.JobsFailed.Inc()
		}
		a.sendJobFailed(jobID, runID, runErr.Error(), 0, durationMs)
		return
	}

	// Get final stats from checkpoint.
	jobs, _ := store.ListJobs(context.Background())
	var totalRows int64
	var rowsPerSec float64
	for _, j := range jobs {
		totalRows += j.TotalRows
	}
	if secs := float64(durationMs) / 1000; secs > 0 {
		rowsPerSec = float64(totalRows) / secs
	}

	a.log.Info(ctx, "agent.job.completed",
		"job_id", jobID,
		"total_rows", totalRows,
		"duration_ms", durationMs,
		"rows_per_sec", rowsPerSec,
	)

	if a.metrics != nil {
		a.metrics.RowsTotal.Add(float64(totalRows))
	}

	span.SetAttributes(
		attribute.Int64("total_rows", totalRows),
		attribute.Int64("duration_ms", durationMs),
	)

	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobCompleted{
			JobCompleted: &agentv1.JobCompleted{
				JobId:       jobID,
				TotalRows:   totalRows,
				RowsPerSec:  rowsPerSec,
				DurationMs:  durationMs,
				CompletedAt: timestamppb.Now(),
				RunId:       runID,
			},
		},
	})

	// Persist CDC position for future runs.
	if cmd.GetSyncMode() == "cdc" {
		result := eng.Result()
		if len(result.FinalCDCPosition) > 0 {
			if err := a.saveCDCPosition(cdcKey, result.FinalCDCPosition); err != nil {
				a.log.Error(ctx, "agent.cdc.save_position.error", "error", err)
			} else {
				a.log.Info(ctx, "agent.cdc.position_saved",
					"job_id", jobID,
					"bytes", len(result.FinalCDCPosition),
				)
			}
		}
	}

	// Run post-sync row count validation in the background so it doesn't
	// delay the completed event. Validation results are sent separately.
	// Use a detached context with timeout — the job context is about to be
	// cancelled by the deferred cleanup above, so validation needs its own.
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

	a.mu.Lock()
	cancel, ok := a.cancels[jobID]
	a.mu.Unlock()

	if ok {
		cancel()
		a.log.Info(ctx, "agent.job.cancelled", "job_id", jobID)
	} else {
		a.log.Warn(ctx, "agent.job.cancel.not_found", "job_id", jobID)
	}
}

// reportJobProgress sends an immediate snapshot, then periodically reads checkpoint state.
func (a *Agent) reportJobProgress(ctx context.Context, jobID, runID string, store checkpoint.Store, eng *engine.Engine) {
	// Send first snapshot immediately so Atlas marks the job as running.
	a.sendProgressSnapshot(jobID, runID, store, eng)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.sendProgressSnapshot(jobID, runID, store, eng)
		}
	}
}

func (a *Agent) sendProgressSnapshot(jobID, runID string, store checkpoint.Store, eng *engine.Engine) {
	result := eng.Result()
	workerCount := result.WriteWorkers
	if workerCount <= 0 {
		workerCount = 4 // fallback before engine has tuned
	}
	// The engine creates one checkpoint job per table with its own ID,
	// not the Atlas job ID. Aggregate across all checkpoint jobs.
	cpJobs, err := store.ListJobs(context.Background())
	if err != nil || len(cpJobs) == 0 {
		return
	}

	var totalRows, rowsCompleted int64
	var totalTasks, completedTasks, runningTasks, pendingTasks, failedTasks int64
	var rowsPerSec float64
	var startTime time.Time
	var allWorkerStatuses []*agentv1.WorkerStatus

	for _, cpJob := range cpJobs {
		p, err := store.JobProgress(context.Background(), cpJob.ID)
		if err != nil {
			continue
		}
		totalTasks += p.Total
		completedTasks += p.Completed
		runningTasks += p.Running
		pendingTasks += p.Pending
		failedTasks += p.Failed
		totalRows += cpJob.TotalRows
		rowsCompleted += cpJob.TotalRows
		rowsPerSec += p.RowsPerSec
		if startTime.IsZero() || (!cpJob.CreatedAt.IsZero() && cpJob.CreatedAt.Before(startTime)) {
			startTime = cpJob.CreatedAt
		}

		snapshots, err := store.WorkerSnapshots(context.Background(), cpJob.ID, workerCount)
		if err == nil {
			for _, ws := range snapshots {
				allWorkerStatuses = append(allWorkerStatuses, &agentv1.WorkerStatus{
					WorkerId:  ws.WorkerID,
					Status:    ws.Status,
					TaskId:    ws.TaskID,
					TableName: ws.TableName,
					RangeStr:  ws.RangeStr,
					RowsDone:  ws.RowsDone,
					TasksDone: ws.TasksDone,
					TotalRows: ws.TotalRows,
				})
			}
		}
	}

	pct := float64(0)
	if totalTasks > 0 {
		pct = float64(completedTasks) / float64(totalTasks) * 100
	}

	// Deduplicate workers — merge snapshots by worker ID (pick the active one).
	merged := make(map[string]*agentv1.WorkerStatus, workerCount)
	for _, ws := range allWorkerStatuses {
		existing, ok := merged[ws.WorkerId]
		if !ok || ws.Status == "running" {
			if ok {
				// Accumulate totals from previous entry.
				ws.TasksDone += existing.TasksDone
				ws.TotalRows += existing.TotalRows
			}
			merged[ws.WorkerId] = ws
		} else {
			existing.TasksDone += ws.TasksDone
			existing.TotalRows += ws.TotalRows
		}
	}
	workerStatuses := make([]*agentv1.WorkerStatus, 0, len(merged))
	for i := 0; i < workerCount; i++ {
		wid := fmt.Sprintf("worker-%d", i)
		if ws, ok := merged[wid]; ok {
			workerStatuses = append(workerStatuses, ws)
		}
	}

	// Compute average rps since job start.
	var avgRps float64
	if !startTime.IsZero() {
		elapsed := time.Since(startTime).Seconds()
		if elapsed > 0 {
			avgRps = float64(rowsCompleted) / elapsed
		}
	}

	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobProgress{
			JobProgress: &agentv1.JobProgress{
				JobId:          jobID,
				TotalRows:      totalRows,
				RowsCompleted:  rowsCompleted,
				RowsPerSec:     rowsPerSec,
				Pct:            pct,
				TasksTotal:     int32(totalTasks),
				TasksCompleted: int32(completedTasks),
				TasksRunning:   int32(runningTasks),
				TasksPending:   int32(pendingTasks),
				TasksFailed:    int32(failedTasks),
				Timestamp:      timestamppb.Now(),
				Workers:        workerStatuses,
				ReadWorkers:    int32(result.ReadWorkers),
				WriteWorkers:   int32(result.WriteWorkers),
				AvgRps:         avgRps,
				WriteStrategy:  result.WriteStrategy,
				RunId:          runID,
			},
		},
	})
}

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

func (a *Agent) sendJobEvent(jobID string, ev engine.Event) {
	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobEvent{
			JobEvent: &agentv1.JobEvent{
				JobId:     jobID,
				EventType: string(ev.Type),
				Worker:    ev.Worker,
				TaskId:    ev.TaskID,
				Table:     ev.Table,
				Range:     ev.Range,
				Rows:      ev.Rows,
				ReadMs:    ev.ReadMs,
				WriteMs:   ev.WriteMs,
				Error:     ev.Error,
				Attempt:   int32(ev.Attempt),
				Timestamp: timestamppb.Now(),
			},
		},
	})
}

// forwardSchemaProposal reads a proposal from the local checkpoint store
// and sends the full payload to Atlas via a ConnectRequest_SchemaProposal
// message. This bridges the gap between engine-local proposals and the
// Atlas schema_proposals table shown in the UI.
func (a *Agent) forwardSchemaProposal(jobID, runID string, ev engine.Event, store *checkpoint.SQLiteStore) {
	ctx := context.Background()
	proposal, err := store.GetProposal(ctx, ev.ProposalID)
	if err != nil {
		a.log.Error(ctx, "agent.schema_proposal.read_error",
			"job_id", jobID, "proposal_id", ev.ProposalID, "error", err)
		return
	}
	if proposal == nil {
		a.log.Warn(ctx, "agent.schema_proposal.not_found",
			"job_id", jobID, "proposal_id", ev.ProposalID)
		return
	}

	autoApplied := ev.Type == engine.EventSchemaAutoApplied
	status := "pending"
	if autoApplied {
		status = "auto_applied"
	}

	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_SchemaProposal{
			SchemaProposal: &agentv1.SchemaChangeProposal{
				ProposalId:    proposal.ID,
				JobId:         jobID,
				RunId:         runID,
				TableName:     proposal.TableName,
				Status:        status,
				ChangesJson:   proposal.ChangesJSON,
				OldSchemaJson: proposal.OldSchemaJSON,
				NewSchemaJson: proposal.NewSchemaJSON,
				AutoApplied:   autoApplied,
				CreatedAt:     timestamppb.New(proposal.CreatedAt),
			},
		},
	})

	a.log.Info(ctx, "agent.schema_proposal.forwarded",
		"job_id", jobID, "proposal_id", proposal.ID,
		"table", proposal.TableName, "status", status)
}

func newJobLogger(base *logger.Logger, jobID string) *logger.Logger {
	_ = jobID
	return base
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
