package main

import (
	"context"
	"net/http"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zwiron/pkg/logger"
)

// AgentMetrics tracks runtime metrics for the agent.
type AgentMetrics struct {
	JobsRunning  prometheus.Gauge
	JobsTotal    prometheus.Counter
	JobsFailed   prometheus.Counter
	RowsTotal    prometheus.Counter
	MemoryBytes  prometheus.Gauge
	GoroutineGauge prometheus.Gauge
	JobDuration  prometheus.Histogram

	registry *prometheus.Registry
}

// NewAgentMetrics creates and registers all agent metrics.
func NewAgentMetrics() *AgentMetrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &AgentMetrics{
		JobsRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "zwiron_agent_jobs_running",
			Help: "Number of currently running jobs on this agent.",
		}),
		JobsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "zwiron_agent_jobs_total",
			Help: "Total number of jobs executed by this agent.",
		}),
		JobsFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "zwiron_agent_jobs_failed_total",
			Help: "Total number of failed jobs on this agent.",
		}),
		RowsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "zwiron_agent_rows_total",
			Help: "Total number of rows synced by this agent.",
		}),
		MemoryBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "zwiron_agent_memory_bytes",
			Help: "Current memory usage in bytes (Go runtime alloc).",
		}),
		GoroutineGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "zwiron_agent_goroutines",
			Help: "Number of active goroutines.",
		}),
		JobDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "zwiron_agent_job_duration_seconds",
			Help:    "Job execution duration in seconds.",
			Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		}),
		registry: reg,
	}

	reg.MustRegister(m.JobsRunning, m.JobsTotal, m.JobsFailed, m.RowsTotal,
		m.MemoryBytes, m.GoroutineGauge, m.JobDuration)

	return m
}

// Handler returns an HTTP handler for the /metrics endpoint.
func (m *AgentMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// UpdateRuntimeMetrics refreshes memory and goroutine gauges.
func (m *AgentMetrics) UpdateRuntimeMetrics() {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	m.MemoryBytes.Set(float64(mem.Alloc))
	m.GoroutineGauge.Set(float64(runtime.NumGoroutine()))
}

// StartMetricsServer starts a lightweight HTTP server for Prometheus scraping.
func StartMetricsServer(ctx context.Context, log *logger.Logger, m *AgentMetrics, port string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		log.Info(ctx, "agent.metrics_server", "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error(ctx, "agent.metrics_server.error", "error", err)
		}
	}()

	// Update runtime metrics periodically.
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				srv.Close()
				return
			case <-ticker.C:
				m.UpdateRuntimeMetrics()
			}
		}
	}()
}
