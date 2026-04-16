package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/kardianos/service"
	"github.com/zwiron/pkg/logger"
	"github.com/zwiron/pkg/tracing"
)

const (
	serviceName = "zwiron-agent"
	serviceDesc = "Zwiron data movement agent"
)

// svcLogDir returns the platform-appropriate log directory.
func svcLogDir() string {
	switch runtime.GOOS {
	case "windows":
		return filepath.Join(os.Getenv("ProgramData"), "zwiron", "log")
	case "darwin":
		return "/usr/local/var/log"
	default: // linux
		return "/var/log/zwiron"
	}
}

// svcConfig returns the kardianos service configuration.
func svcConfig() *service.Config {
	logDir := svcLogDir()
	if err := os.MkdirAll(logDir, 0755); err != nil {
		logDir = ""
	}

	cfg := &service.Config{
		Name:        serviceName,
		DisplayName: "Zwiron Agent",
		Description: serviceDesc,
	}

	if logDir != "" {
		cfg.Option = service.KeyValue{
			"LogOutput":    true,
			"LogDirectory": logDir,
		}
	}

	return cfg
}

// program implements service.Interface for kardianos/service.
type program struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (p *program) Start(s service.Service) error {
	p.done = make(chan struct{})
	go p.run()
	return nil
}

func (p *program) Stop(s service.Service) error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.done != nil {
		<-p.done
	}
	return nil
}

func (p *program) run() {
	defer close(p.done)

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	if err := runAgent(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "agent fatal: %v\n", err)
	}
}

// runAgent is the shared core between foreground (cmd_run) and service mode.
// It reads config, sets up tracing/logging, creates the Agent, and enters
// the reconnect loop. Blocks until ctx is cancelled.
func runAgent(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	return runAgentWithConfig(ctx, cfg)
}

// runAgentWithConfig runs the agent with an already-parsed config.
func runAgentWithConfig(ctx context.Context, cfg AgentConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	// Tracing.
	otelHost := os.Getenv("ZWIRON_OTEL_HOST")
	if otelHost == "" {
		otelHost = "disabled"
	}
	_, tracingShutdown, err := tracing.Init(tracing.Config{
		ServiceName:    "agent",
		ServiceVersion: version,
		Host:           otelHost,
		Probability:    1.0,
	})
	if err != nil {
		return fmt.Errorf("tracing init: %w", err)
	}
	defer tracingShutdown(context.Background())

	// Logger.
	log := logger.NewWithTraceFunc(logger.Config{
		Level:       "info",
		ServiceName: "agent",
		Format:      "pretty",
	}, tracing.GetTraceID)

	log.Info(ctx, "agent.config",
		"version", version,
		"atlas_addr", cfg.Addr,
		"data_dir", cfg.DataDir,
	)

	// Keys.
	keyPath := filepath.Join(cfg.DataDir, "agent.key")
	keys, err := LoadOrGenerateKeys(keyPath, log)
	if err != nil {
		return fmt.Errorf("key management: %w", err)
	}
	log.Info(ctx, "agent.keys.ready", "path", keyPath)

	// Metrics.
	agentMetrics := NewAgentMetrics()
	if cfg.MetricsPort != "0" {
		StartMetricsServer(ctx, log, agentMetrics, cfg.MetricsPort)
	}

	agent := &Agent{
		log:       log,
		token:     cfg.Token,
		atlasAddr: cfg.Addr,
		dataDir:   cfg.DataDir,
		keys:      keys,
		maxJobs:   cfg.MaxJobs,
		metrics:   agentMetrics,
	}

	if agent.maxJobs > 0 {
		agent.jobSem = make(chan struct{}, agent.maxJobs)
		log.Info(ctx, "agent.concurrency_limit", "max_concurrent_jobs", agent.maxJobs)
	}

	// Reconnect loop with exponential backoff + jitter.
	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 60 * time.Second
		backoffFactor  = 2
	)
	backoff := initialBackoff

	for {
		connStart := time.Now()
		err := agent.Run(ctx)

		if ctx.Err() != nil {
			log.Info(ctx, "agent.stopped")
			return nil
		}

		if time.Since(connStart) > 30*time.Second {
			backoff = initialBackoff
		}

		jitter := time.Duration(rand.Int64N(int64(backoff / 2)))
		wait := backoff + jitter - backoff/4

		log.Warn(ctx, "agent.disconnected", "error", err, "retry_in", wait)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}

		backoff *= backoffFactor
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}
