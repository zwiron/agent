package main

import (
	"context"
	"encoding/json"
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
	if workers <= 0 {
		workers = 4
	}
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
	}

	// Start progress reporter in background.
	progressCtx, progressCancel := context.WithCancel(jobCtx)
	var progressWg sync.WaitGroup
	progressWg.Add(1)
	go func() {
		defer progressWg.Done()
		a.reportJobProgress(progressCtx, jobID, store, workers)
	}()

	// Run the engine.
	eng := engine.New(store, a.log)
	startTime := time.Now()
	runErr := eng.Run(jobCtx, engineCfg)
	progressCancel()
	progressWg.Wait()
	durationMs := time.Since(startTime).Milliseconds()

	// Send final progress snapshot so Atlas has accurate task/worker counts.
	a.sendProgressSnapshot(jobID, store, workers)

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
func (a *Agent) reportJobProgress(ctx context.Context, jobID string, store checkpoint.Store, workerCount int) {
	// Send first snapshot immediately so Atlas marks the job as running.
	a.sendProgressSnapshot(jobID, store, workerCount)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.sendProgressSnapshot(jobID, store, workerCount)
		}
	}
}

func (a *Agent) sendProgressSnapshot(jobID string, store checkpoint.Store, workerCount int) {
	// Get progress for the specific job, not all checkpoint jobs.
	p, err := store.JobProgress(context.Background(), jobID)
	if err != nil {
		return
	}

	// Get row count from the job record.
	var rowsCompleted int64
	if j, err := store.GetJob(context.Background(), jobID); err == nil {
		rowsCompleted = j.TotalRows
	}

	pct := float64(0)
	if p.Total > 0 {
		pct = float64(p.Completed) / float64(p.Total) * 100
	}

	// Collect per-worker snapshots.
	var workerStatuses []*agentv1.WorkerStatus
	snapshots, err := store.WorkerSnapshots(context.Background(), jobID, workerCount)
	if err == nil {
		for _, ws := range snapshots {
			workerStatuses = append(workerStatuses, &agentv1.WorkerStatus{
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

	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobProgress{
			JobProgress: &agentv1.JobProgress{
				JobId:          jobID,
				TotalRows:      rowsCompleted,
				RowsCompleted:  rowsCompleted,
				RowsPerSec:     p.RowsPerSec,
				Pct:            pct,
				TasksTotal:     int32(p.Total),
				TasksCompleted: int32(p.Completed),
				TasksRunning:   int32(p.Running),
				TasksPending:   int32(p.Pending),
				TasksFailed:    int32(p.Failed),
				Timestamp:      timestamppb.Now(),
				Workers:        workerStatuses,
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

func (a *Agent) sendJobEvent(jobID, eventType, worker, taskID, table, rangeStr string) {
	data, _ := json.Marshal(map[string]string{
		"job_id":     jobID,
		"event_type": eventType,
		"worker":     worker,
	})
	_ = data

	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_JobEvent{
			JobEvent: &agentv1.JobEvent{
				JobId:     jobID,
				EventType: eventType,
				Worker:    worker,
				TaskId:    taskID,
				Table:     table,
				Range:     rangeStr,
				Timestamp: timestamppb.Now(),
			},
		},
	})
}

func newJobLogger(base *logger.Logger, jobID string) *logger.Logger {
	_ = jobID
	return base
}
