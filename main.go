package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/zwiron/pkg/config"
	"github.com/zwiron/pkg/logger"
	"github.com/zwiron/pkg/tracing"

	_ "github.com/zwiron/connector/sql/mysql"
	_ "github.com/zwiron/connector/sql/postgres"
)

func main() {
	ctx := context.Background()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "agent fatal: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg := struct {
		Token   string `conf:"required,mask"`
		Addr    string `conf:"default:localhost:9090"`
		DataDir string `conf:"default:auto"`
		MaxJobs int    `conf:"default:0"`
		Metrics struct {
			Port string `conf:"default:9091"`
		}
		OTel struct {
			Host string `conf:"default:disabled"`
		}
	}{}
	config.MustParse("ZWIRON", &cfg)

	if cfg.DataDir == "auto" {
		cfg.DataDir = defaultDataDir()
	}

	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	// -------------------------------------------------------------------------
	// Tracing
	// -------------------------------------------------------------------------

	_, tracingShutdown, err := tracing.Init(tracing.Config{
		ServiceName:    "agent",
		ServiceVersion: "develop",
		Host:           cfg.OTel.Host,
		Probability:    1.0,
	})
	if err != nil {
		return fmt.Errorf("tracing init: %w", err)
	}
	defer tracingShutdown(context.Background())

	// -------------------------------------------------------------------------
	// Logger (with trace ID injection)
	// -------------------------------------------------------------------------

	log := logger.NewWithTraceFunc(logger.Config{
		Level:       "info",
		ServiceName: "agent",
		Format:      "pretty",
	}, tracing.GetTraceID)

	log.Info(ctx, "agent.config",
		"atlas_addr", cfg.Addr,
		"data_dir", cfg.DataDir,
	)

	keyPath := filepath.Join(cfg.DataDir, "agent.key")
	keys, err := LoadOrGenerateKeys(keyPath, log)
	if err != nil {
		return fmt.Errorf("key management: %w", err)
	}

	log.Info(ctx, "agent.keys.ready", "path", keyPath)

	// Initialize Prometheus metrics.
	agentMetrics := NewAgentMetrics()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info(ctx, "agent.shutdown", "signal", sig.String())
		cancel()
	}()

	agent := &Agent{
		log:       log,
		token:     cfg.Token,
		atlasAddr: cfg.Addr,
		dataDir:   cfg.DataDir,
		keys:      keys,
		maxJobs:   cfg.MaxJobs,
		metrics:   agentMetrics,
	}

	// Initialize job concurrency semaphore.
	if agent.maxJobs > 0 {
		agent.jobSem = make(chan struct{}, agent.maxJobs)
		log.Info(ctx, "agent.concurrency_limit", "max_concurrent_jobs", agent.maxJobs)
	}

	// Start Prometheus metrics server.
	if cfg.Metrics.Port != "0" {
		StartMetricsServer(ctx, log, agentMetrics, cfg.Metrics.Port)
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

		// Clean exit (context cancelled = shutdown signal).
		if ctx.Err() != nil {
			log.Info(ctx, "agent.stopped")
			return nil
		}

		// If the connection was stable for >30s, reset backoff on next failure.
		if time.Since(connStart) > 30*time.Second {
			backoff = initialBackoff
		}

		// Add jitter: backoff ± 25% to prevent thundering herd.
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

func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".zwiron"
	}
	return filepath.Join(home, ".zwiron", "agent")
}
