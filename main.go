package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

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
	token := flag.String("token", "", "agent registration token from Atlas dashboard")
	atlasAddr := flag.String("atlas-addr", "localhost:9090", "Atlas gRPC server address")
	dataDir := flag.String("data-dir", defaultDataDir(), "directory for keys and checkpoints")
	insecureFlag := flag.Bool("insecure", false, "use insecure gRPC connection (no TLS)")
	flag.Parse()

	if *token == "" {
		*token = os.Getenv("ZWIRON_AGENT_TOKEN")
	}
	if *token == "" {
		return fmt.Errorf("--token or ZWIRON_AGENT_TOKEN is required")
	}

	if envAddr := os.Getenv("ZWIRON_ATLAS_ADDR"); envAddr != "" && *atlasAddr == "localhost:9090" {
		*atlasAddr = envAddr
	}

	if err := os.MkdirAll(*dataDir, 0700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	// -------------------------------------------------------------------------
	// Tracing
	// -------------------------------------------------------------------------

	otelHost := os.Getenv("ZWIRON_OTEL_HOST")
	_, tracingShutdown, err := tracing.Init(tracing.Config{
		ServiceName:    "agent",
		ServiceVersion: "develop",
		Host:           otelHost,
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
		"atlas_addr", *atlasAddr,
		"data_dir", *dataDir,
		"insecure", *insecureFlag,
	)

	keyPath := filepath.Join(*dataDir, "agent.key")
	keys, err := LoadOrGenerateKeys(keyPath, log)
	if err != nil {
		return fmt.Errorf("key management: %w", err)
	}

	log.Info(ctx, "agent.keys.ready", "path", keyPath)

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
		token:     *token,
		atlasAddr: *atlasAddr,
		insecure:  *insecureFlag,
		dataDir:   *dataDir,
		keys:      keys,
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
