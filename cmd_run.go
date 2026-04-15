package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/zwiron/pkg/config"
)

// cmdRun is the foreground run mode — for Docker and local development.
// Reads configuration from ZWIRON_ env vars via pkg/config.
func cmdRun() error {
	cfg := struct {
		Token   string `conf:"required,mask"`
		Addr    string `conf:"default:grpc.zwiron.com:443"`
		DataDir string `conf:"default:auto"`
		MaxJobs int    `conf:"default:0"`
		Metrics struct {
			Port string `conf:"default:9091"`
		}
	}{}
	config.MustParse("ZWIRON", &cfg)

	if cfg.DataDir == "auto" {
		cfg.DataDir = defaultDataDir()
	}

	acfg := AgentConfig{
		Token:       cfg.Token,
		Addr:        cfg.Addr,
		DataDir:     cfg.DataDir,
		MaxJobs:     cfg.MaxJobs,
		MetricsPort: cfg.Metrics.Port,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	return runAgentWithConfig(ctx, acfg)
}

func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".zwiron"
	}
	return home + "/.zwiron/agent"
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Zwiron Agent %s

Usage:
  zwiron-agent <command> [options]

Commands:
  install     Install as a system service
  uninstall   Remove the system service
  start       Start the service
  stop        Stop the service
  restart     Restart the service
  status      Show agent status
  logs        Tail service logs
  run         Run in foreground (for Docker / development)
  version     Print version

Install:
  sudo zwiron-agent install --token <token>

Run (foreground):
  ZWIRON_TOKEN=<token> zwiron-agent run

`, version)
}
