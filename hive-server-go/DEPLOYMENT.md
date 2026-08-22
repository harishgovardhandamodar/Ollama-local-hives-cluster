# Hive Server Go - Deployment Guide

This guide covers deployment of the production-grade Hive Server with all Phase 1-4 features enabled.

## Quick Start with All Features

```bash
# Build
go build -o hive-server .

# Run with all advanced features enabled
export OLLAMA_BASE_URL=http://localhost:11434
export OLLAMA_MODEL=llama3.1:8b
export SERVER_PORT=8081
export MAX_CONCURRENT=4

# Phase 1: Model Registry & Smart Selection
export MODEL_REGISTRY_ENABLED=true
export PREFER_LOADED_MODELS=true
export MODEL_COMPATIBILITY=true
export ROUTING_STRATEGY=loaded-first

# Phase 2: Priority Queue
export PRIORITY_QUEUE_ENABLED=true

# Phase 3: Circuit Breaker
export CIRCUIT_BREAKER_ENABLED=true

# Phase 4: Caching & Metrics
export RESPONSE_CACHE_ENABLED=true
export CACHE_MAX_SIZE=1000
export CACHE_TTL_SECONDS=3600
export PROMETHEUS_ENABLED=true
export PROMETHEUS_PORT=9090

./hive-server
```

## Docker Deployment

### Single Node with All Features

```yaml
# docker-compose.yml
version: '3.8'

services:
  hive-server:
    image: hive-server-go:latest
    build: .
    ports:
      - "8081:8081"   # HTTP API
      - "8082:8082/udp" # Mesh discovery
      - "9090:9090"   # Prometheus metrics
    volumes:
      - hive-data:/data
    environment:
      - OLLAMA_BASE_URL=http://host.docker.internal:11434
      - OLLAMA_MODEL=llama3.1:8b
      - MESH_ENABLED=true
      - MESH_ANNOUNCE_ADDRESS=192.168.1.50:8081
      
      # Advanced features
      - MODEL_REGISTRY_ENABLED=true
      - PREFER_LOADED_MODELS=true
      - MODEL_COMPATIBILITY=true
      - ROUTING_STRATEGY=loaded-first
      - PRIORITY_QUEUE_ENABLED=true
      - CIRCUIT_BREAKER_ENABLED=true
      - RESPONSE_CACHE_ENABLED=true
      - CACHE_MAX_SIZE=1000
      - PROMETHEUS_ENABLED=true
      - PROMETHEUS_PORT=9090
    
    # GPU support (Linux)
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: all
              capabilities: [gpu]

volumes:
  hive-data:
```

### Multi-Node Cluster

```yaml
# docker-compose.cluster.yml
version: '3.8'

services:
  hive-node-1:
    image: hive-server-go:latest
    ports:
      - "8081:8081"
      - "8082:8082/udp"
      - "9090:9090"
    environment:
      - SERVER_ID=hive-node-1
      - OLLAMA_BASE_URL=http://ollama-1:11434
      - MESH_ENABLED=true
      - MESH_SEED_PEERS=hive-node-2:8081,hive-node-3:8081
      - MESH_ANNOUNCE_ADDRESS=192.168.1.51:8081
      # Pre-load specific models on this node
      - OLLAMA_MODEL=llama3.1:8b

  hive-node-2:
    image: hive-server-go:latest
    ports:
      - "8082:8081"
      - "8083:8082/udp"
      - "9091:9090"
    environment:
      - SERVER_ID=hive-node-2
      - OLLAMA_BASE_URL=http://ollama-2:11434
      - MESH_ENABLED=true
      - MESH_SEED_PEERS=hive-node-1:8081,hive-node-3:8081
      - MESH_ANNOUNCE_ADDRESS=192.168.1.52:8081
      - OLLAMA_MODEL=qwen2.5-coder:32b

  hive-node-3:
    image: hive-server-go:latest
    ports:
      - "8083:8081"
      - "8084:8082/udp"
      - "9092:9090"
    environment:
      - SERVER_ID=hive-node-3
      - OLLAMA_BASE_URL=http://ollama-3:11434
      - MESH_ENABLED=true
      - MESH_SEED_PEERS=hive-node-1:8081,hive-node-2:8081
      - MESH_ANNOUNCE_ADDRESS=192.168.1.53:8081
      - OLLAMA_MODEL=llama3.1:70b
```

## Kubernetes Deployment

```yaml
# kubernetes/hive-server-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hive-server
spec:
  replicas: 3
  selector:
    matchLabels:
      app: hive-server
  template:
    metadata:
      labels:
        app: hive-server
    spec:
      containers:
      - name: hive-server
        image: hive-server-go:latest
        ports:
        - containerPort: 8081
          name: http
        - containerPort: 8082
          name: mesh
          protocol: UDP
        - containerPort: 9090
          name: metrics
        env:
        - name: SERVER_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: OLLAMA_BASE_URL
          value: "http://ollama.default.svc.cluster.local:11434"
        - name: MESH_ENABLED
          value: "true"
        - name: MESH_SEED_PEERS
          value: "hive-server-0.hive-server.default.svc.cluster.local:8081,hive-server-1.hive-server.default.svc.cluster.local:8081"
        - name: MODEL_REGISTRY_ENABLED
          value: "true"
        - name: PREFER_LOADED_MODELS
          value: "true"
        - name: ROUTING_STRATEGY
          value: "loaded-first"
        - name: PRIORITY_QUEUE_ENABLED
          value: "true"
        - name: CIRCUIT_BREAKER_ENABLED
          value: "true"
        - name: RESPONSE_CACHE_ENABLED
          value: "true"
        - name: PROMETHEUS_ENABLED
          value: "true"
        volumeMounts:
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "8Gi"
            cpu: "4000m"
            nvidia.com/gpu: "1"
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: hive-data-pvc
      nodeSelector:
        gpu: "true"
---
apiVersion: v1
kind: Service
metadata:
  name: hive-server
spec:
  selector:
    app: hive-server
  ports:
  - name: http
    port: 8081
    targetPort: 8081
  - name: mesh
    port: 8082
    targetPort: 8082
    protocol: UDP
  - name: metrics
    port: 9090
    targetPort: 9090
  type: LoadBalancer
```

## Performance Tuning

### Model Loading Optimization

For best model loading optimization:

1. **Pre-load frequently used models** on different nodes
2. **Set `ROUTING_STRATEGY=loaded-first`** to prefer already-loaded models
3. **Enable `MODEL_COMPATIBILITY=true`** for automatic substitution
4. **Monitor model registry** via `/api/models` endpoint

Example pre-loading strategy:
```bash
# Node 1: Small models for quick responses
OLLAMA_MODEL=llama3.1:8b

# Node 2: Medium coding models  
OLLAMA_MODEL=qwen2.5-coder:32b

# Node 3: Large reasoning models
OLLAMA_MODEL=llama3.1:70b
```

### Priority Queue Tuning

Adjust queue priorities based on workload:

```bash
# For interactive applications
export MAX_CONCURRENT=8
# Realtime priority gets 50% of workers by default

# For batch processing
export MAX_CONCURRENT=16
# More capacity for normal/low priority jobs
```

### Circuit Breaker Configuration

Tune circuit breaker thresholds:

```bash
# Defaults (configured in code):
# - FailureThreshold: 5 consecutive failures
# - Timeout: 30 seconds before retry
# - HalfOpenMaxRequests: 3 test requests

# For critical production systems, reduce threshold:
# Modify circuit_breaker.go constants
```

### Cache Optimization

Tune cache for your workload:

```bash
# High-churn workloads (many unique prompts)
export CACHE_MAX_SIZE=500
export CACHE_TTL_SECONDS=1800

# Stable workloads (repeated prompts)
export CACHE_MAX_SIZE=2000
export CACHE_TTL_SECONDS=7200

# For coding assistance (high repetition)
export CACHE_MAX_SIZE=5000
export CACHE_TTL_SECONDS=86400
```

## Monitoring & Observability

### Prometheus Metrics

Access metrics at `http://localhost:9090/metrics`:

```promql
# Request rate by model
rate(hive_requests_total[5m])

# Model load state
hive_models_loaded{node_id="hive-node-1"}

# Queue depth by priority
hive_queue_depth{priority="realtime"}

# Circuit breaker state
hive_circuit_breaker_state{provider="ollama"}

# Cache hit rate
hive_cache_hits_total / hive_cache_requests_total

# P99 latency by routing decision
histogram_quantile(0.99, rate(hive_request_latency_seconds_bucket[5m]))
```

### Grafana Dashboard

Import the provided dashboard JSON or create panels for:
- Model loading distribution across nodes
- Request routing decisions (loaded vs new load)
- Queue depth by priority over time
- Circuit breaker state transitions
- Cache hit/miss ratio
- Peer health and latency

### Alerting Rules

```yaml
# prometheus-alerts.yml
groups:
- name: hive-server
  rules:
  - alert: HighCircuitBreakerFailures
    expr: hive_circuit_breaker_state{state="open"} == 1
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "Circuit breaker open for {{ $labels.provider }}"
  
  - alert: ModelLoadFailure
    expr: rate(hive_model_load_failures_total[5m]) > 0.1
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High model load failure rate"
  
  - alert: QueueBacklog
    expr: hive_queue_depth{priority="realtime"} > 10
    for: 1m
    labels:
      severity: warning
    annotations:
      summary: "Realtime queue backlog building up"
```

## Health Checks

```bash
# Basic health
curl http://localhost:8081/v1/health

# Detailed status
curl http://localhost:8081/api/status

# Model registry status
curl http://localhost:8081/api/models

# Queue status by priority
curl http://localhost:8081/api/queue

# Circuit breaker state
curl http://localhost:8081/api/providers

# Prometheus metrics
curl http://localhost:9090/metrics
```

## Troubleshooting

### Models Not Being Shared

1. Check mesh connectivity: `curl http://localhost:8081/api/peers`
2. Verify model registry: `curl http://localhost:8081/api/models`
3. Ensure `MESH_ENABLED=true` and `MODEL_REGISTRY_ENABLED=true`
4. Check firewall rules for UDP port 8082

### Priority Queue Not Working

1. Verify `PRIORITY_QUEUE_ENABLED=true`
2. Check job submission includes priority field
3. Monitor queue depths: `curl http://localhost:8081/api/queue`

### Circuit Breaker Tripping Frequently

1. Check provider health: `curl http://localhost:8081/api/providers`
2. Review logs for error patterns
3. Adjust circuit breaker thresholds in code
4. Consider increasing provider resources

### Cache Not Hitting

1. Verify `RESPONSE_CACHE_ENABLED=true`
2. Check cache stats in logs
3. Ensure prompts are identical (semantic matching requires exact matches currently)
4. Increase `CACHE_MAX_SIZE` if evictions are high

## Security Considerations

### Production Hardening

```bash
# Enable authentication (requires code changes)
# export API_KEY_REQUIRED=true
# export API_KEYS=key1,key2,key3

# Rate limiting
# export RATE_LIMIT_REQUESTS_PER_MINUTE=60

# TLS/HTTPS
# Generate certificates and use:
# --tls-cert=/path/to/cert.pem --tls-key=/path/to/key.pem
```

### Network Isolation

- Use private networks for mesh communication
- Restrict Prometheus metrics access to monitoring subnet
- Use firewall rules to limit API access

## Backup & Recovery

### Database Backup

```bash
# SQLite database location
DB_PATH=${HIVE_DB_PATH:-/data/hive-server.db}

# Backup command
sqlite3 $DB_PATH ".backup '/backups/hive-backup-$(date +%Y%m%d).db'"

# Restore
cp /backups/hive-backup-20250101.db $DB_PATH
```

### State Recovery

On restart, the server will:
1. Reload model registry from mesh peers
2. Restore pending jobs from SQLite (if persistence enabled)
3. Re-establish mesh connections
4. Warm cache from recent queries (if enabled)

## Next Steps

After deployment:
1. Monitor metrics for 24-48 hours
2. Tune configuration based on observed patterns
3. Set up alerting for critical metrics
4. Document your model distribution strategy
5. Plan capacity for growth

For advanced configuration and customization, see the source code comments in each component file.
