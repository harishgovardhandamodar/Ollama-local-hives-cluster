package main

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsManager handles all Prometheus metrics for the Hive Server
type MetricsManager struct {
	mu sync.RWMutex
	
	// Request metrics
	requestsTotal      *prometheus.CounterVec
	requestDuration    *prometheus.HistogramVec
	requestSize        *prometheus.HistogramVec
	responseSize       *prometheus.HistogramVec
	
	// Model metrics
	modelLoadsTotal    *prometheus.CounterVec
	modelsLoaded       *prometheus.GaugeVec
	modelLatency       *prometheus.HistogramVec
	modelSwitchesTotal prometheus.Counter
	
	// Queue metrics
	queueDepth         *prometheus.GaugeVec
	queueEnqueueTotal  *prometheus.CounterVec
	queueDequeueTotal  *prometheus.CounterVec
	queueWaitTime      *prometheus.HistogramVec
	
	// Peer metrics
	peerLatency        *prometheus.HistogramVec
	peerRequestsTotal  *prometheus.CounterVec
	peerFailuresTotal  *prometheus.CounterVec
	
	// Cache metrics
	cacheHitsTotal     prometheus.Counter
	cacheMissesTotal   prometheus.Counter
	cacheSize          prometheus.Gauge
	cacheEvictionsTotal prometheus.Counter
	
	// Circuit breaker metrics
	circuitBreakerState *prometheus.GaugeVec
	circuitBreakerTrips *prometheus.CounterVec
	
	// Token metrics
	tokensGenerated    *prometheus.CounterVec
	tokensPerSecond    *prometheus.GaugeVec
	
	// Resource metrics
	vramUsageBytes     *prometheus.GaugeVec
	gpuUtilization     *prometheus.GaugeVec
}

// NewMetricsManager creates and registers all Prometheus metrics
func NewMetricsManager() *MetricsManager {
	mm := &MetricsManager{}
	
	// Request metrics
	mm.requestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hive_requests_total",
			Help: "Total number of requests processed",
		},
		[]string{"model", "provider", "status", "priority", "routing_decision"},
	)
	
	mm.requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "hive_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
		},
		[]string{"model", "provider", "job_type", "priority"},
	)
	
	mm.requestSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "hive_request_size_bytes",
			Help:    "Size of incoming requests in bytes",
			Buckets: prometheus.ExponentialBuckets(100, 2, 10),
		},
		[]string{"job_type"},
	)
	
	mm.responseSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "hive_response_size_bytes",
			Help:    "Size of outgoing responses in bytes",
			Buckets: prometheus.ExponentialBuckets(100, 2, 12),
		},
		[]string{"model", "job_type"},
	)
	
	// Model metrics
	mm.modelLoadsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hive_model_loads_total",
			Help: "Total number of model load operations",
		},
		[]string{"model", "provider", "node_id", "status"},
	)
	
	mm.modelsLoaded = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hive_models_loaded",
			Help: "Number of models currently loaded",
		},
		[]string{"node_id", "provider"},
	)
	
	mm.modelLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "hive_model_inference_latency_seconds",
			Help:    "Model inference latency in seconds",
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 10),
		},
		[]string{"model", "provider", "context_length"},
	)
	
	mm.modelSwitchesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "hive_model_switches_total",
			Help: "Total number of times a different model was used than requested",
		},
	)
	
	// Queue metrics
	mm.queueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hive_queue_depth",
			Help: "Current depth of the job queue by priority",
		},
		[]string{"priority"},
	)
	
	mm.queueEnqueueTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hive_queue_enqueue_total",
			Help: "Total number of jobs enqueued",
		},
		[]string{"priority", "job_type"},
	)
	
	mm.queueDequeueTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hive_queue_dequeue_total",
			Help: "Total number of jobs dequeued",
		},
		[]string{"priority", "job_type"},
	)
	
	mm.queueWaitTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "hive_queue_wait_time_seconds",
			Help:    "Time jobs spend waiting in queue",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		},
		[]string{"priority"},
	)
	
	// Peer metrics
	mm.peerLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "hive_peer_latency_seconds",
			Help:    "Latency to peer nodes in seconds",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
		},
		[]string{"peer_id", "operation"},
	)
	
	mm.peerRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hive_peer_requests_total",
			Help: "Total requests forwarded to peers",
		},
		[]string{"peer_id", "status"},
	)
	
	mm.peerFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hive_peer_failures_total",
			Help: "Total failed requests to peers",
		},
		[]string{"peer_id", "error_type"},
	)
	
	// Cache metrics
	mm.cacheHitsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "hive_cache_hits_total",
			Help: "Total cache hits",
		},
	)
	
	mm.cacheMissesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "hive_cache_misses_total",
			Help: "Total cache misses",
		},
	)
	
	mm.cacheSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "hive_cache_size",
			Help: "Current number of entries in cache",
		},
	)
	
	mm.cacheEvictionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "hive_cache_evictions_total",
			Help: "Total cache evictions",
		},
	)
	
	// Circuit breaker metrics
	mm.circuitBreakerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hive_circuit_breaker_state",
			Help: "Current state of circuit breakers (0=closed, 1=open, 2=half-open)",
		},
		[]string{"provider", "endpoint"},
	)
	
	mm.circuitBreakerTrips = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hive_circuit_breaker_trips_total",
			Help: "Total number of times circuit breakers have tripped",
		},
		[]string{"provider", "endpoint"},
	)
	
	// Token metrics
	mm.tokensGenerated = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hive_tokens_generated_total",
			Help: "Total tokens generated by model",
		},
		[]string{"model", "provider"},
	)
	
	mm.tokensPerSecond = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hive_tokens_per_second",
			Help: "Current tokens per second rate",
		},
		[]string{"model", "provider"},
	)
	
	// Resource metrics
	mm.vramUsageBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hive_vram_usage_bytes",
			Help: "VRAM usage in bytes",
		},
		[]string{"provider", "device"},
	)
	
	mm.gpuUtilization = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hive_gpu_utilization",
			Help: "GPU utilization percentage",
		},
		[]string{"provider", "device"},
	)
	
	return mm
}

// RecordRequest records a completed request
func (mm *MetricsManager) RecordRequest(model, provider, status, priority, routing string, duration time.Duration) {
	mm.requestsTotal.WithLabelValues(model, provider, status, priority, routing).Inc()
	mm.requestDuration.WithLabelValues(model, provider, "chat", priority).Observe(duration.Seconds())
}

// RecordModelLoad records a model load operation
func (mm *MetricsManager) RecordModelLoad(model, provider, nodeID, status string) {
	mm.modelLoadsTotal.WithLabelValues(model, provider, nodeID, status).Inc()
}

// UpdateModelsLoaded updates the gauge for loaded models
func (mm *MetricsManager) UpdateModelsLoaded(nodeID, provider string, count int) {
	mm.modelsLoaded.WithLabelValues(nodeID, provider).Set(float64(count))
}

// RecordQueueEnqueue records a job being enqueued
func (mm *MetricsManager) RecordQueueEnqueue(priority JobPriority, jobType string, waitTime time.Duration) {
	mm.queueEnqueueTotal.WithLabelValues(priority.String(), jobType).Inc()
	mm.queueWaitTime.WithLabelValues(priority.String()).Observe(waitTime.Seconds())
}

// RecordQueueDequeue records a job being dequeued
func (mm *MetricsManager) RecordQueueDequeue(priority JobPriority, jobType string) {
	mm.queueDequeueTotal.WithLabelValues(priority.String(), jobType).Inc()
}

// UpdateQueueDepth updates the queue depth gauge
func (mm *MetricsManager) UpdateQueueDepth(priority JobPriority, depth int) {
	mm.queueDepth.WithLabelValues(priority.String()).Set(float64(depth))
}

// RecordPeerRequest records a request forwarded to a peer
func (mm *MetricsManager) RecordPeerRequest(peerID, status string, latency time.Duration) {
	mm.peerRequestsTotal.WithLabelValues(peerID, status).Inc()
	mm.peerLatency.WithLabelValues(peerID, "forward").Observe(latency.Seconds())
}

// RecordPeerFailure records a failed peer request
func (mm *MetricsManager) RecordPeerFailure(peerID, errorType string) {
	mm.peerFailuresTotal.WithLabelValues(peerID, errorType).Inc()
}

// RecordCacheHit records a cache hit
func (mm *MetricsManager) RecordCacheHit() {
	mm.cacheHitsTotal.Inc()
}

// RecordCacheMiss records a cache miss
func (mm *MetricsManager) RecordCacheMiss() {
	mm.cacheMissesTotal.Inc()
}

// UpdateCacheSize updates the cache size gauge
func (mm *MetricsManager) UpdateCacheSize(size int64) {
	mm.cacheSize.Set(float64(size))
}

// RecordCacheEviction records a cache eviction
func (mm *MetricsManager) RecordCacheEviction() {
	mm.cacheEvictionsTotal.Inc()
}

// UpdateCircuitBreakerState updates the circuit breaker state gauge
func (mm *MetricsManager) UpdateCircuitBreakerState(provider, endpoint string, state int) {
	mm.circuitBreakerState.WithLabelValues(provider, endpoint).Set(float64(state))
}

// RecordCircuitBreakerTrip records a circuit breaker trip
func (mm *MetricsManager) RecordCircuitBreakerTrip(provider, endpoint string) {
	mm.circuitBreakerTrips.WithLabelValues(provider, endpoint).Inc()
}

// RecordTokensGenerated records tokens generated
func (mm *MetricsManager) RecordTokensGenerated(model, provider string, count int) {
	mm.tokensGenerated.WithLabelValues(model, provider).Add(float64(count))
}

// UpdateTokensPerSecond updates the tokens per second gauge
func (mm *MetricsManager) UpdateTokensPerSecond(model, provider string, tps float64) {
	mm.tokensPerSecond.WithLabelValues(model, provider).Set(tps)
}

// UpdateVRAMUsage updates VRAM usage gauge
func (mm *MetricsManager) UpdateVRAMUsage(provider, device string, bytes int64) {
	mm.vramUsageBytes.WithLabelValues(provider, device).Set(float64(bytes))
}

// UpdateGPUUtilization updates GPU utilization gauge
func (mm *MetricsManager) UpdateGPUUtilization(provider, device string, percent float64) {
	mm.gpuUtilization.WithLabelValues(provider, device).Set(percent)
}

// RecordModelSwitch records when a different model is used than requested
func (mm *MetricsManager) RecordModelSwitch() {
	mm.modelSwitchesTotal.Inc()
}

// MetricsHandler returns an HTTP handler for Prometheus metrics
func (mm *MetricsManager) MetricsHandler() http.Handler {
	return promhttp.Handler()
}

// StartBackgroundMetrics starts background goroutines to collect periodic metrics
func (mm *MetricsManager) StartBackgroundMetrics(hs *HiveServer, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		
		for range ticker.C {
			mm.collectPeriodicMetrics(hs)
		}
	}()
}

// collectPeriodicMetrics collects metrics that need periodic sampling
func (mm *MetricsManager) collectPeriodicMetrics(hs *HiveServer) {
	if hs == nil {
		return
	}
	
	// Collect queue depths from priority queue if available
	// Note: This would need proper integration with the OllamaQueue
	
	// Collect model counts
	if hs.modelRegistry != nil {
		stats := hs.modelRegistry.GetStats()
		if localCount, ok := stats["local_models"].(int); ok {
			mm.UpdateModelsLoaded(getServerID(), "local", localCount)
		}
	}
	
	// Collect cache metrics if available
	// This would be connected to the cache implementation
}

// GetMetricsSummary returns a summary of key metrics as JSON
func (mm *MetricsManager) GetMetricsSummary() map[string]interface{} {
	// This could be used for the dashboard API
	return map[string]interface{}{
		"requests_total":     mm.requestsTotal,
		"cache_hit_ratio":    "calculated_from_hits_misses",
		"queue_depths":       "by_priority",
		"models_loaded":      "by_node",
	}
}

// RegisterCustomCollector allows registering custom collectors
func (mm *MetricsManager) RegisterCustomCollector(collector prometheus.Collector) error {
	return prometheus.Register(collector)
}
