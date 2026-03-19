package main

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/zwiron/connector"
	"github.com/zwiron/engine/checkpoint"
	"github.com/zwiron/engine/engine"
	"github.com/zwiron/pkg/logger"
	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleStartJob decrypts connection configs and runs the engine.
func (a *Agent) handleStartJob(ctx context.Context, cmd *agentv1.StartJob) {
	jobID := cmd.GetJobId()

	a.log.Info(ctx, "agent.job.start",
		"job_id", jobID,
		"tables", cmd.GetTables(),
		"workers", cmd.GetWorkers(),
	)

	// Create a cancellable context for this job.
	jobCtx, jobCancel := context.WithCancel(ctx)
	a.mu.Lock()
	a.cancels[jobID] = jobCancel
	a.mu.Unlock()
	defer func() {
		jobCancel()
		a.mu.Lock()
		delete(a.cancels, jobID)
		a.mu.Unlock()
	}()

	// Decrypt source and destination configs.
	srcConfig, err := DecryptConfig(a.keys.Private, cmd.GetSource().GetEncryptedConfig())
	if err != nil {
		a.sendJobFailed(jobID, fmt.Sprintf("decrypt source config: %v", err), 0, 0)
		return
	}

	dstConfig, err := DecryptConfig(a.keys.Private, cmd.GetDest().GetEncryptedConfig())
	if err != nil {
		a.sendJobFailed(jobID, fmt.Sprintf("decrypt dest config: %v", err), 0, 0)
		return
	}

	// Open checkpoint store for this job.
	cpPath := filepath.Join(a.dataDir, fmt.Sprintf("checkpoint-%s.db", jobID))
	store, err := checkpoint.NewSQLiteStore(checkpoint.SQLiteConfig{
		Path:   cpPath,
		Logger: a.log,
	})
	if err != nil {
		a.sendJobFailed(jobID, fmt.Sprintf("open checkpoint: %v", err), 0, 0)
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
		OnEvent: func(ev engine.Event) {
			a.sendJobEvent(jobID, ev)
		},
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
		a.reportJobProgress(progressCtx, jobID, store, eng)
	}()

	startTime := time.Now()
	runErr := eng.Run(jobCtx, engineCfg)
	progressCancel()
	progressWg.Wait()
	durationMs := time.Since(startTime).Milliseconds()

	// Send final progress snapshot so Atlas has accurate task/worker counts.
	a.sendProgressSnapshot(jobID, store, eng)

	// Send final status.
	if runErr != nil {
		a.log.Error(ctx, "agent.job.failed", "job_id", jobID, "error", runErr)
		a.sendJobFailed(jobID, runErr.Error(), 0, durationMs)
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

	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobCompleted{
			JobCompleted: &agentv1.JobCompleted{
				JobId:       jobID,
				TotalRows:   totalRows,
				RowsPerSec:  rowsPerSec,
				DurationMs:  durationMs,
				CompletedAt: timestamppb.Now(),
			},
		},
	})
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
func (a *Agent) reportJobProgress(ctx context.Context, jobID string, store checkpoint.Store, eng *engine.Engine) {
	// Send first snapshot immediately so Atlas marks the job as running.
	a.sendProgressSnapshot(jobID, store, eng)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.sendProgressSnapshot(jobID, store, eng)
		}
	}
}

func (a *Agent) sendProgressSnapshot(jobID string, store checkpoint.Store, eng *engine.Engine) {
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
			},
		},
	})
}

func (a *Agent) sendJobFailed(jobID, errMsg string, rowsCompleted, durationMs int64) {
	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobFailed{
			JobFailed: &agentv1.JobFailed{
				JobId:         jobID,
				ErrorMessage:  errMsg,
				RowsCompleted: rowsCompleted,
				DurationMs:    durationMs,
				FailedAt:      timestamppb.Now(),
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

func newJobLogger(base *logger.Logger, jobID string) *logger.Logger {
	_ = jobID
	return base
}
