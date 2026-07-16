# Hive Server Go

A standalone Go inference orchestration server for Ollama, LM Studio, vLLM, and OpenAI-compatible providers. Features concurrent job queuing, peer mesh discovery, GPU-aware hardware metrics, SQLite-backed token usage reporting, a live dashboard, **OpenAI-compatible API**, **Coding Agent API**, and **enterprise-grade hardening**.

## Features

- **Job Queue** — Goroutine worker pool with configurable concurrency, priority queues, Ollama proxy, token tracking
- **Mesh Discovery** — UDP broadcast peer discovery, seed peers, cross-platform model mapping (MLX→NVIDIA), active health polling
- **OpenAI-Compatible API** — `/v1/chat/completions` (streaming + non-streaming), `/v1/models` (local + peer), `/v1/health`
- **Coding Agent API** — Session-based context management, auto-compression, audit logging for Hermes/OpenCode/Codex
- **Auto-Forwarding** — When a model isn't found locally, requests are automatically forwarded to the peer that has it
- **Enterprise Security** — API key authentication, rate limiting, structured JSON logging
- **Audit Trail** — Every request captured with full lifecycle, prompt, model, timing. Viewable as dashboard timeline overlay with expandable event graph
- **Response Cache** — Prompt-hash based caching with configurable TTL for repeated queries
- **Metrics** — Prometheus-compatible `/metrics` endpoint with counters, gauges, and histograms
- **Live Dashboard** — Real-time stats, queue, mesh topology, token usage charts, live logs

## Quick Start

### Docker Compose (Recommended)

```sh
# Clone and start the full stack
git clone https://github.com/harishgovardhandamodar/hive-serving-local-Cluster.git
cd hive-serving-local-Cluster

# Start with Docker Compose
docker compose up -d

# Check status
docker compose ps
curl http://localhost:8081/api/status
```

### Docker (Manual)

```sh
# Basic server
docker run -d --name hive-server \
  -p 8081:8081 \
  -p 8082:8082/udp \
  -v hive-data:/data \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  -e OLLAMA_MODEL=llama3.1:8b \
  hive-server-go:latest

# With authentication
docker run -d --name hive-server \
  -p 8081:8081 \
  -v hive-data:/data \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  -e HIVE_API_KEY=your-secret-key \
  -e HIVE_RATE_LIMIT=100 \
  hive-server-go:latest

# With mesh networking
docker run -d --name hive-server \
  -p 8081:8081 \
  -p 8082:8082/udp \
  -v hive-data:/data \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  -e MESH_ENABLED=true \
  -e MESH_SEED_PEERS="192.168.1.100:8081" \
  -e MESH_ANNOUNCE_ADDRESS="192.168.1.50:8081" \
  -e MESH_MODEL_MAP="gemma4:31b-mlx->gemma4:31b" \
  hive-server-go:latest
```

### Local Build

```sh
go build -o hive-server-go ./hive-server-go/
OLLAMA_BASE_URL=http://localhost:11434 ./hive-server-go
```

Open http://localhost:8081 for the dashboard.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server URL |
| `OLLAMA_MODEL` | `llama3.1:8b` | Default model for inference |
| `SERVER_PORT` | `8081` | HTTP server port |
| `MAX_CONCURRENT` | `2` | Max concurrent jobs |
| `MAX_CLIENTS` | `5` | Max registered clients |
| `MESH_ENABLED` | `true` | Enable UDP peer discovery |
| `MESH_DISCOVERY_PORT` | `8082` | UDP discovery port |
| `MESH_SEED_PEERS` | — | Comma-separated seed peer addresses |
| `MESH_ANNOUNCE_ADDRESS` | auto | LAN IP:port for beacons (required in Docker) |
| `MESH_MODEL_MAP` | — | Cross-platform model mapping (e.g., `mlx->nvidia`) |
| `HIVE_API_KEY` | — | API key for authentication |
| `HIVE_RATE_LIMIT` | `100` | Requests per minute per IP |
| `HIVE_CACHE_ENABLED` | `false` | Enable response caching |
| `HIVE_CACHE_MAX_ENTRIES` | `1000` | Max cache entries |
| `HIVE_CACHE_TTL_SECONDS` | `300` | Cache TTL in seconds |
| `HIVE_LOG_JSON` | `false` | Enable structured JSON logging |
| `HIVE_DB_PATH` | `/data/hive-server.db` | SQLite database path |
| `HIVE_CONFIG` | `hive.yaml` | YAML config file path |

### YAML Config File

Create `hive.yaml` in the working directory:

```yaml
server:
  port: 8081
  ollama_url: http://localhost:11434
  ollama_model: llama3.1:8b
  max_concurrent: 4
  max_clients: 10
  api_key: your-secret-key

mesh:
  enabled: true
  discovery_port: 8082
  announce_address: 192.168.1.50:8081
  seed_peers: "192.168.1.100:8081"
  model_map: "gemma4:31b-mlx->gemma4:31b"

database:
  path: /data/hive-server.db

cache:
  enabled: true
  max_entries: 2000
  ttl_seconds: 600

logging:
  json: true

custom_providers:
  - http://localhost:1234/v1
```

Environment variables override YAML config values.

### Cross-Platform Model Mapping

When your mesh has mixed hardware (MLX on Mac, NVIDIA on Linux):

```sh
MESH_MODEL_MAP="gemma4:31b-mlx->gemma4:31b,qwen3.6:35b-mlx->qwen3.6:35b"
```

Requests for `gemma4:31b-mlx` will be automatically remapped to `gemma4:31b` when forwarding to NVIDIA peers.

## API Reference

### Health & Status

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/status` | GET | Server status, queue, mesh, hardware |
| `/v1/health` | GET | OpenAI-compatible health check |
| `/metrics` | GET | Prometheus metrics |

### Authentication

All endpoints (except dashboard, health, peer intros) require API key:

```sh
# Bearer token
curl -H "Authorization: Bearer your-api-key" http://localhost:8081/api/status

# Query parameter
curl "http://localhost:8081/api/status?api_key=your-api-key"
```

Dashboard (`/`), health (`/v1/health`), and peer endpoints are exempt from auth.

### Rate Limiting

Default: 100 requests per minute per IP. Returns `429 Too Many Requests` when exceeded.

```json
{"error": "rate limit exceeded"}
```

### Job Queue

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/jobs` | POST | Submit job |
| `/api/jobs` | GET | List recent jobs |
| `/api/jobs/{job_id}` | GET | Get job status |
| `/api/jobs/stream` | GET | SSE job streaming |
| `/api/queue` | GET | Queue status |

#### Submit Job

```json
{
  "client_id": "my-app",
  "job_type": "generate",
  "payload": {
    "prompt": "Hello, world!",
    "model": "llama3.1:8b",
    "stream": false
  }
}
```

#### Job Types

| Type | Priority | Payload |
|------|----------|---------|
| `coding_agent_chat` | High | `messages`, `model`, `session_id` |
| `chat` | High | `messages`, `model`, `stream` |
| `generate` | Normal | `prompt`, `model`, `system`, `temperature` |
| `embed` / `get_embedding` | Normal | `prompt`, `text`, `model` |
| `list_models` | Low | — |
| `pull_model` | Low | `name` |
| Custom types | Low | Any Ollama API fields |

#### SSE Job Streaming

```sh
curl -N "http://localhost:8081/api/jobs/stream?job_id=xxx"

# Events:
data: {"job_id":"xxx","status":"running","progress":0.5}
data: {"job_id":"xxx","status":"completed","result":{...}}
data: {"job_id":"xxx","status":"failed","error":"timeout"}
```

### Client Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/clients/register` | POST | Register client |
| `/api/clients/{id}/heartbeat` | POST | Keep-alive |
| `/api/clients/unregister` | POST | Deregister |
| `/api/clients` | GET | List clients |

### OpenAI-Compatible API

Point any OpenAI client (Hermes, OpenCode, Codex, LangChain) at `http://localhost:8081/v1`.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/chat/completions` | POST | Chat completion (streaming + non-streaming) |
| `/v1/models` | GET | List models (local + peers) |

#### Chat Completion

```sh
# Streaming
curl -N -X POST http://localhost:8081/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen3.6:35b",
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": true
  }'

# Non-streaming
curl -X POST http://localhost:8081/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama3.1:8b",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

**Auto-forwarding**: If the model isn't found locally, the request is automatically forwarded to the peer that has it.

#### List Models

Returns models from local Ollama + all mesh peers:

```json
{
  "object": "list",
  "data": [
    {"id": "llama3.1:8b", "object": "model", "owned_by": "local"},
    {"id": "qwen3.6:35b", "object": "model", "owned_by": "hive-axiom"}
  ]
}
```

### Coding Agent API

Session-based interface for coding agents with context management and audit logging.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/agent/sessions` | POST | Create session |
| `/api/agent/sessions` | GET | List sessions |
| `/api/agent/sessions/{id}` | GET | Get session |
| `/api/agent/sessions/{id}` | DELETE | Delete session |
| `/api/agent/sessions/{id}/messages` | POST | Send message |
| `/api/agent/sessions/{id}/messages` | GET | List messages |
| `/api/agent/sessions/{id}/context` | GET | Context stats |
| `/api/agent/audit` | GET | Search audit logs |
| `/api/agent/models` | GET | Models with context windows |

#### Create Session

```json
{
  "agent_type": "hermes",
  "model": "glm-4.7-flash:bf16",
  "system_prompt": "You are a coding assistant.",
  "token_budget": 160000
}
```

#### Send Message

```json
{
  "role": "user",
  "content": "Write a hello world in Go"
}
```

### Model Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/models/pull-proxy` | POST | Pull model from peer |
| `/api/ollama/health` | GET | Ollama health + models |

#### Pull from Peer

```json
{
  "model": "qwen3.6:35b",
  "peer_id": "hive-axiom"
}
```

### Mesh Networking

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/peers` | GET | List peers |
| `/api/peers/register` | POST | Register peer |
| `/api/peers/introduce` | POST | Peer self-registration |
| `/api/peers/scan` | GET | Re-probe seed peers |
| `/api/peers/diagnostics` | GET | Mesh config + model map |

### Observability

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/logs` | GET | Server logs (poll by timestamp) |
| `/api/logs/clear` | POST | Clear log buffer |
| `/api/reports/usage` | GET | Aggregated token usage |
| `/api/reports/usage/recent` | GET | Recent 100 records |
| `/api/reports/usage/timeseries` | GET | TPS time-series |
| `/api/reports/usage/histogram` | GET | TPS distribution |

### Dashboard

**`GET /`** — Live dashboard with stats, queue, mesh topology, token usage charts, live logs, and **Audit Trail** tab.

### Audit Trail

Every request to the server is captured with full request/response lifecycle, prompt, model, and timing data. The audit trail is stored in SQLite and viewable via API or the dashboard's **Audit Trail** tab.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/audit/recent` | GET | Recent audit events (with category filter) |
| `/api/audit/search` | GET | Full-text search across prompts, models, paths |
| `/api/audit/timeline/{request_id}` | GET | Event timeline for a specific request |
| `/api/audit/summary/{request_id}` | GET | Summary with duration & event counts |
| `/api/audit/detail/{request_id}` | GET | Full detail: request body, response, timeline |

#### Recent Events

```sh
curl "http://localhost:8081/api/audit/recent?limit=20&category=coding_agent"
```

```json
{
  "events": [
    {
      "id": 2886,
      "request_id": "req_1784220418094_00808808",
      "event_type": "request",
      "category": "openai_compat",
      "method": "POST",
      "path": "/v1/chat/completions",
      "model": "gemma4:31b-mlx",
      "prompt": "[System: The active model...]",
      "overrides": {"model": "gemma4:31b-mlx", "messages": [...]},
      "created_at": "2026-07-16T18:46:58+02:00"
    }
  ],
  "count": 1
}
```

#### Request Detail (Overlay)

```sh
curl "http://localhost:8081/api/audit/detail/req_xxx"
```

```json
{
  "request_id": "req_xxx",
  "method": "POST",
  "path": "/v1/chat/completions",
  "model": "llama3.1:8b",
  "prompt": "Say hi in 3 words",
  "status_code": 200,
  "token_count": 25,
  "total_duration_ms": 5000,
  "total_events": 2,
  "response_content": "Hello!",
  "events": [
    {
      "event_type": "request",
      "model": "llama3.1:8b",
      "prompt": "Say hi in 3 words",
      "method": "POST",
      "path": "/v1/chat/completions",
      "created_at": "..."
    },
    {
      "event_type": "response",
      "content": "Hello!",
      "status_code": 200,
      "duration_ms": 5000
    }
  ]
}
```

#### Search

```sh
curl "http://localhost:8081/api/audit/search?q=gemma4&limit=10"
```

Searches across: `prompt`, `model`, `path`, `method`, `content`, `query`, `job_type`, `error_message`.

#### Dashboard Timeline View

The **Audit Trail** tab on the dashboard:
- Groups events by `request_id`, sorted with important requests first (chat completions, errors)
- Shows method badge, path, model, prompt preview, duration
- Click the request body area → opens **detail overlay** with full request JSON, response content, and expandable event timeline
- Active streaming requests highlighted with pulsing purple border
- Inline toggle (▶/▼) shows brief event summary without opening the overlay
- Category filter: Coding Agent, OpenAI Compat, Job Queue, Mesh, System
- Free-text search filters in real-time

#### Captured Data

| Field | Description |
|-------|-------------|
| `request_id` | Unique per-request identifier (`req_{timestamp}_{hex}`) |
| `prompt` | Extracted from body `prompt`, `messages[last].content`, or nested `payload` |
| `model` | Model name from the request body |
| `overrides` | Full original request body (all parameters) |
| `event_type` | `request`, `response`, `error`, `forward`, `cache_hit`, `job_complete`, `job_error` |
| `category` | `coding_agent`, `openai_compat`, `job_queue`, `mesh`, `system` |
| `duration_ms` | Request-response duration in milliseconds |
| `token_count` | Total tokens used (from completion response) |
| `status_code` | HTTP status code of the response |

#### Data Persistence

Audit trail data is stored in the SQLite database at `./hive-server.db` (configurable via `HIVE_DB_PATH`). Data persists across server restarts. The database can be backed up, copied, or inspected with any SQLite tool:

```sh
sqlite3 hive-server.db "SELECT COUNT(*) FROM request_audit_trail"
sqlite3 hive-server.db "SELECT event_type, path, model, datetime(created_at, 'unixepoch')
  FROM request_audit_trail ORDER BY created_at DESC LIMIT 10"
```

High-frequency dashboard polling endpoints (`/api/logs`, `/api/clients`, `/api/status`, `/api/reports/*`, `/api/ollama/health`) are excluded from audit logging to avoid noise.

## Mesh Discovery

### How It Works

1. **UDP Beacon Broadcast** — Every 10s on `MESH_DISCOVERY_PORT` (default 8082)
2. **Beacon Contents** — `server_id`, `announce_addr`, `port`, `models`, `max_concurrent`, `available_capacity`, `pending_jobs`, `running_jobs`
3. **Peer Staleness** — Peers removed after 30s without beacon
4. **Active Health Polling** — 10s interval HTTP health checks (faster than beacon timeout)
5. **Seed Peers** — Probed via HTTP `/api/status` every 30s
6. **Bi-directional Introduction** — Peers exchange state via `POST /api/peers/introduce`

### Job Forwarding

When local capacity is exhausted:
1. Server selects least-loaded peer
2. Forwards job via `POST /api/jobs/forward`
3. Peer executes and returns result

### Model Auto-Forwarding

When a model isn't found locally:
1. Server queries peer model cache (30s TTL)
2. Finds peer with the model
3. Streams response from peer (transparent to client)

### Docker Networking

In Docker, set `MESH_ANNOUNCE_ADDRESS` to your host's LAN IP:

```sh
MESH_ANNOUNCE_ADDRESS="192.168.1.50:8081"
```

Without this, beacons announce the Docker internal IP which is unreachable from other machines.

## Response Cache

When enabled, identical prompts return cached responses:

```sh
# Enable cache
HIVE_CACHE_ENABLED=true HIVE_CACHE_TTL_SECONDS=600 ./hive-server-go

# Or via YAML
cache:
  enabled: true
  max_entries: 2000
  ttl_seconds: 600
```

Cache uses SHA256 prompt-hash keys with configurable TTL (default 5 minutes).

## Metrics

Prometheus-compatible metrics at `GET /metrics`:

```
# HELP hive_uptime_seconds Server uptime
# TYPE hive_uptime_seconds gauge
hive_uptime_seconds 12345.67

# HELP hive_jobs_total Total jobs submitted
# TYPE hive_jobs_total counter
hive_jobs_total 1234

# HELP hive_queue_depth Current queue depth
# TYPE hive_queue_depth gauge
hive_queue_depth 2

# HELP hive_tokens_per_second Tokens per second
# TYPE hive_tokens_per_second summary
hive_tokens_per_second_sum 123456.78
hive_tokens_per_second_count 567
```

### Metrics Available

- `hive_uptime_seconds` — Server uptime
- `hive_jobs_total` — Jobs submitted
- `hive_jobs_completed_total` — Jobs completed
- `hive_jobs_failed_total` — Jobs failed
- `hive_messages_cached_total` — Cache hits
- `hive_peers_forwarded_total` — Jobs forwarded to peers
- `hive_queue_depth` — Current queue depth
- `hive_running_jobs` — Currently running jobs
- `hive_connected_peers` — Connected peers
- `hive_active_clients` — Active clients
- `hive_job_duration_seconds` — Job execution duration
- `hive_tokens_per_second` — Tokens per second

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Hive Server Go                            │
│                                                               │
│  ┌───────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐  │
│  │ Job Queue  │  │   Mesh   │  │ Provider │  │  Coding    │  │
│  │ (workers)  │  │ Discovery│  │ Manager  │  │  Agent API │  │
│  │ Priority   │  │ UDP bcast│  │ auto-    │  │  Sessions  │  │
│  │ Queues     │  │ health   │  │ detect   │  │  Context   │  │
│  └─────┬─────┘  └──────────┘  └──────────┘  └──────┬─────┘  │
│        │                                             │        │
│  ┌─────▼─────────────────────────────────────────────▼─────┐  │
│  │              OpenAI-Compatible API (:8081)              │  │
│  │  /v1/chat/completions  /v1/models  /v1/health          │  │
│  │  /api/jobs  /api/clients  /api/peers  /api/agent/*     │  │
│  │  /metrics  /api/jobs/stream                            │  │
│  └────────────────────────────┬────────────────────────────┘  │
│                               │                               │
│  ┌────────────────────────────▼────────────────────────────┐  │
│  │  Auth Middleware  →  Rate Limiter  →  Request Logger    │  │
│  └────────────────────────────┬────────────────────────────┘  │
│                               │                               │
│  ┌────────────────────────────▼────────────────────────────┐  │
│  │  Local Ollama (:11434)  ←→  Mesh Peers (:8081)        │  │
│  │  Auto-forward on 404     Model mapping (MLX↔NVIDIA)    │  │
│  │  Response Cache          Priority Job Dispatch          │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  SQLite WAL — token_usage, coding_agent_sessions,      │  │
│  │              coding_agent_messages, coding_agent_audit  │  │
│  │  /data/hive-server.db                                  │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  Observability — Prometheus /metrics, JSON logging,    │  │
│  │                  SSE job streaming, Live dashboard      │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

## Dashboard

- **Stats bar** — workers, queued, running, completed, clients, peers
- **Tabs** — Jobs, Resources, Live Logs (INFO/WARN/ERROR filter)
- **Token Usage** — stats + TPS chart (Line/Hist modes, time range buttons, per-model reports)
- **Mesh Topology** — peer status, load, model map

## Development

### Building

```sh
go build ./hive-server-go/
go vet ./hive-server-go/
```

### Testing

```sh
# Run all tests
go test ./hive-server-go/...

# Build Docker image
docker build -t hive-server-go -f hive-server-go/Dockerfile .
```

### Adding Features

The embedded dashboard at `static/index.html` is compiled into the binary via `//go:embed`.

## License

MIT
