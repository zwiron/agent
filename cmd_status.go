package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/kardianos/service"
)

func cmdStatus() error {
	// Check service status.
	prg := &program{}
	s, err := service.New(prg, svcConfig())
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	status, err := s.Status()
	var stateStr string
	switch {
	case err != nil:
		stateStr = "not installed"
	case status == service.StatusRunning:
		stateStr = "running"
	case status == service.StatusStopped:
		stateStr = "stopped"
	default:
		stateStr = "unknown"
	}

	// Try to load config for details.
	cfg, cfgErr := loadConfig()

	fmt.Printf("Zwiron Agent %s\n\n", version)
	fmt.Printf("  Service:   %s\n", stateStr)

	if cfgErr == nil {
		fmt.Printf("  Atlas:     %s\n", cfg.Addr)
		fmt.Printf("  Data Dir:  %s\n", cfg.DataDir)
		fmt.Printf("  Config:    %s\n", configPath())
	} else if stateStr == "not installed" {
		fmt.Printf("  Config:    not found\n")
	}

	// If running, try to get metrics for extra info.
	if status == service.StatusRunning && cfgErr == nil {
		port := cfg.MetricsPort
		if port == "" {
			port = "9091"
		}
		if info := fetchAgentInfo(port); info != nil {
			if v, ok := info["agent_id"]; ok && v != "" {
				fmt.Printf("  Agent ID:  %s\n", v)
			}
			if v, ok := info["uptime"]; ok && v != "" {
				fmt.Printf("  Uptime:    %s\n", v)
			}
		}
	}

	fmt.Println()
	return nil
}

// fetchAgentInfo tries to get info from the metrics endpoint.
func fetchAgentInfo(port string) map[string]string {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://localhost:%s/healthz", port))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil
	}
	var info map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil
	}
	return info
}

func init() {
	// Ensure TERM is set for pretty output.
	if os.Getenv("TERM") == "" {
		os.Setenv("TERM", "xterm-256color")
	}
}
