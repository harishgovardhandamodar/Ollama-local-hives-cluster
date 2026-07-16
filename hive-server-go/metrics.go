package main

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

type MetricsCollector struct {
	mu sync.RWMutex

	// Counters
	jobsSubmitted   int64
	jobsCompleted   int64
	jobsFailed      int64
	messagesCached  int64
	peersForwarded  int64

	// Gauges
	queueDepth      int
	runningJobs     int
	connectedPeers  int
	activeClients   int

	// Histograms (simplified)
	jobDurations    []float64
	tokenCounts     []float64
	tpsValues       []float64

	startTime time.Time
}

var globalMetrics = &MetricsCollector{
	startTime: time.Now(),
}

func (m *MetricsCollector) IncrJobsSubmitted() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobsSubmitted++
}

func (m *MetricsCollector) IncrJobsCompleted() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobsCompleted++
}

func (m *MetricsCollector) IncrJobsFailed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobsFailed++
}

func (m *MetricsCollector) IncrMessagesCached() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messagesCached++
}

func (m *MetricsCollector) IncrPeersForwarded() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peersForwarded++
}

func (m *MetricsCollector) SetQueueDepth(depth int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queueDepth = depth
}

func (m *MetricsCollector) SetRunningJobs(running int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runningJobs = running
}

func (m *MetricsCollector) SetConnectedPeers(peers int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connectedPeers = peers
}

func (m *MetricsCollector) SetActiveClients(clients int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeClients = clients
}

func (m *MetricsCollector) RecordJobDuration(seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobDurations = append(m.jobDurations, seconds)
	if len(m.jobDurations) > 1000 {
		m.jobDurations = m.jobDurations[1:]
	}
}

func (m *MetricsCollector) RecordTokenCount(tokens int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tokenCounts = append(m.tokenCounts, float64(tokens))
	if len(m.tokenCounts) > 1000 {
		m.tokenCounts = m.tokenCounts[1:]
	}
}

func (m *MetricsCollector) RecordTPS(tps float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tpsValues = append(m.tpsValues, tps)
	if len(m.tpsValues) > 1000 {
		m.tpsValues = m.tpsValues[1:]
	}
}

func (m *MetricsCollector) Render() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var sb strings.Builder

	// Metadata
	sb.WriteString("# HELP hive_uptime_seconds Server uptime in seconds\n")
	sb.WriteString("# TYPE hive_uptime_seconds gauge\n")
	sb.WriteString(fmt.Sprintf("hive_uptime_seconds %f\n", time.Since(m.startTime).Seconds()))

	// Counters
	sb.WriteString("# HELP hive_jobs_total Total jobs submitted\n")
	sb.WriteString("# TYPE hive_jobs_total counter\n")
	sb.WriteString(fmt.Sprintf("hive_jobs_total %d\n", m.jobsSubmitted))

	sb.WriteString("# HELP hive_jobs_completed_total Total jobs completed\n")
	sb.WriteString("# TYPE hive_jobs_completed_total counter\n")
	sb.WriteString(fmt.Sprintf("hive_jobs_completed_total %d\n", m.jobsCompleted))

	sb.WriteString("# HELP hive_jobs_failed_total Total jobs failed\n")
	sb.WriteString("# TYPE hive_jobs_failed_total counter\n")
	sb.WriteString(fmt.Sprintf("hive_jobs_failed_total %d\n", m.jobsFailed))

	sb.WriteString("# HELP hive_messages_cached_total Total messages served from cache\n")
	sb.WriteString("# TYPE hive_messages_cached_total counter\n")
	sb.WriteString(fmt.Sprintf("hive_messages_cached_total %d\n", m.messagesCached))

	sb.WriteString("# HELP hive_peers_forwarded_total Total jobs forwarded to peers\n")
	sb.WriteString("# TYPE hive_peers_forwarded_total counter\n")
	sb.WriteString(fmt.Sprintf("hive_peers_forwarded_total %d\n", m.peersForwarded))

	// Gauges
	sb.WriteString("# HELP hive_queue_depth Current queue depth\n")
	sb.WriteString("# TYPE hive_queue_depth gauge\n")
	sb.WriteString(fmt.Sprintf("hive_queue_depth %d\n", m.queueDepth))

	sb.WriteString("# HELP hive_running_jobs Currently running jobs\n")
	sb.WriteString("# TYPE hive_running_jobs gauge\n")
	sb.WriteString(fmt.Sprintf("hive_running_jobs %d\n", m.runningJobs))

	sb.WriteString("# HELP hive_connected_peers Number of connected peers\n")
	sb.WriteString("# TYPE hive_connected_peers gauge\n")
	sb.WriteString(fmt.Sprintf("hive_connected_peers %d\n", m.connectedPeers))

	sb.WriteString("# HELP hive_active_clients Number of active clients\n")
	sb.WriteString("# TYPE hive_active_clients gauge\n")
	sb.WriteString(fmt.Sprintf("hive_active_clients %d\n", m.activeClients))

	// Histograms (simplified as summaries)
	if len(m.jobDurations) > 0 {
		sb.WriteString("# HELP hive_job_duration_seconds Job execution duration\n")
		sb.WriteString("# TYPE hive_job_duration_seconds summary\n")
		sb.WriteString(fmt.Sprintf("hive_job_duration_seconds_sum %f\n", sumFloat64(m.jobDurations)))
		sb.WriteString(fmt.Sprintf("hive_job_duration_seconds_count %d\n", len(m.jobDurations)))
	}

	if len(m.tpsValues) > 0 {
		sb.WriteString("# HELP hive_tokens_per_second Tokens per second\n")
		sb.WriteString("# TYPE hive_tokens_per_second summary\n")
		sb.WriteString(fmt.Sprintf("hive_tokens_per_second_sum %f\n", sumFloat64(m.tpsValues)))
		sb.WriteString(fmt.Sprintf("hive_tokens_per_second_count %d\n", len(m.tpsValues)))
	}

	return sb.String()
}

func sumFloat64(vals []float64) float64 {
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Write([]byte(globalMetrics.Render()))
}
