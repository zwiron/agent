package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v3"
)

// AgentConfig is the on-disk configuration written by "install" and read by
// the service on startup. Kept minimal — only what's needed to connect.
type AgentConfig struct {
	Token       string `yaml:"token"`
	Addr        string `yaml:"addr"`
	DataDir     string `yaml:"data_dir"`
	MaxJobs     int    `yaml:"max_jobs,omitempty"`
	MetricsPort string `yaml:"metrics_port,omitempty"`
}

// configDir returns the system-level config directory.
func configDir() string {
	if runtime.GOOS == "windows" {
		return filepath.Join(os.Getenv("ProgramData"), "Zwiron")
	}
	return "/etc/zwiron"
}

// configPath returns the full path to agent.yaml.
func configPath() string {
	return filepath.Join(configDir(), "agent.yaml")
}

// serviceDataDir returns the data directory for service mode.
func serviceDataDir() string {
	if runtime.GOOS == "windows" {
		return filepath.Join(os.Getenv("ProgramData"), "Zwiron", "agent")
	}
	return "/var/lib/zwiron-agent"
}

// loadConfig reads agent.yaml from the system config path.
func loadConfig() (AgentConfig, error) {
	data, err := os.ReadFile(configPath())
	if err != nil {
		return AgentConfig{}, fmt.Errorf("read config %s: %w", configPath(), err)
	}
	var cfg AgentConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return AgentConfig{}, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Token == "" {
		return AgentConfig{}, fmt.Errorf("config: token is required")
	}
	if cfg.Addr == "" {
		cfg.Addr = "localhost:9090"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = serviceDataDir()
	}
	if cfg.MetricsPort == "" {
		cfg.MetricsPort = "9091"
	}
	return cfg, nil
}

// saveConfig writes agent.yaml to the system config path with restricted perms.
func saveConfig(cfg AgentConfig) error {
	if err := os.MkdirAll(configDir(), 0755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := os.WriteFile(configPath(), data, 0600); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

// removeConfig deletes the config file and directory if empty.
func removeConfig() error {
	os.Remove(configPath())
	// Remove dir only if empty.
	entries, _ := os.ReadDir(configDir())
	if len(entries) == 0 {
		os.Remove(configDir())
	}
	return nil
}
