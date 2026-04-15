package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/kardianos/service"
)

const defaultAtlasAddr = "grpc.zwiron.com:443"

func cmdInstall(args []string) error {
	fs := flag.NewFlagSet("install", flag.ExitOnError)
	token := fs.String("token", "", "agent registration token (required)")
	addr := fs.String("addr", defaultAtlasAddr, "Atlas gRPC address")
	maxJobs := fs.Int("max-jobs", 0, "maximum concurrent jobs (0 = unlimited)")
	metricsPort := fs.String("metrics-port", "9091", "Prometheus metrics port (0 = disabled)")
	fs.Parse(args)

	if *token == "" {
		fmt.Fprintln(os.Stderr, "Usage: sudo zwiron-agent install --token <token>")
		os.Exit(1)
	}

	// Write config file.
	cfg := AgentConfig{
		Token:       *token,
		Addr:        *addr,
		DataDir:     serviceDataDir(),
		MaxJobs:     *maxJobs,
		MetricsPort: *metricsPort,
	}
	if err := saveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	fmt.Printf("✓ Config written to %s\n", configPath())

	// Create data directory.
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	// Register and start system service.
	prg := &program{}
	s, err := service.New(prg, svcConfig())
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	// Stop existing service if running (ignore errors).
	_ = s.Stop()
	_ = s.Uninstall()

	if err := s.Install(); err != nil {
		return fmt.Errorf("install service: %w", err)
	}
	fmt.Printf("✓ Service registered (%s)\n", service.Platform())

	if err := s.Start(); err != nil {
		return fmt.Errorf("start service: %w", err)
	}
	fmt.Println("✓ Service started")

	fmt.Printf("\nAgent is running. Check status with: zwiron-agent status\n")
	return nil
}
