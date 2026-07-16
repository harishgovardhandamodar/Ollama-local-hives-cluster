# Hive Server Go

A standalone Go inference orchestration server for Ollama, LM Studio, vLLM, and OpenAI-compatible providers. Features concurrent job queuing, peer mesh discovery, GPU-aware hardware metrics, SQLite-backed token usage reporting, a live dashboard, **OpenAI-compatible API**, **Coding Agent API**, and **enterprise-grade hardening**.

## Features

- **Job Queue** — Goroutine worker pool with configurable concurrency, priority queues, Ollama proxy, token tracking
- **Mesh Discovery** — UDP broadcast peer discovery, seed peers, cross-platform model mapping (MLX→NVIDIA), active health polling
- **OpenAI-Compatible API** — `/v1/chat/completions` (streaming + non-streaming), `/v1/models` (local + peer), `/v1/health`
- **Coding Agent API** — Session-based context management, auto-compression, audit logging for Hermes/OpenCode/Codex
- **Auto-Forwarding** — When a model isn't found locally, requests are automatically forwarded to the peer that has it
- **Enterprise Security** — API key authentication, rate limiting, structured JSON logging
- **Response Cache** — Prompt-hash based caching with configurable TTL for repeated queries
- **Metrics** — Prometheus-compatible `/metrics` endpoint with counters, gauges, and histograms
- **Live Dashboard** — Real-time stats, queue, mesh topology, token usage charts, live logs

## Quick Start

```sh
# Local
go build -o hive-server-go ./hive-server-go/
OLLAMA_BASE_URL=http://localhost:11434 ./hive-server-go

# Docker with persistent database
docker run -d --name hive-server \
  -p 8081:8081 \
  -p 8082:8082/udp \
  -v hive-data:/data \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  -e OLLAMA_MODEL=llama3.1:8b \
  -e MESH_ENABLED=true \
  -e MESH_SEED_PEERS="192.168.1.100:8081" \
  -e MESH_ANNOUNCE_ADDRESS="192.168.1.50:8081" \
  -e MESH_MODEL_MAP="gemma4:31b-mlx->gemma4:31b" \
  hive-server-go:latest

# Docker with Nvidia GPU (Linux)
docker run -d --name hive-server --gpus all --network host \
  -p 8081:8081 -p 8082:8082/udp \
  -v hive-data:/data \
  -e OLLAMA_BASE_URL=http://localhost:11434 \
  -e MESH_ENABLED=true \
  -e MESH_SEED_PEERS="192.168.1.100:8081" \
  -e MESH_ANNOUNCE_ADDRESS="192.168.1.50:8081" \
  hive-server-go:latest
```

Open http://localhost:8081 for the dashboard.

## Configuration

| Env Variable | Default | Description |
|---|---|---|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server URL |
| `OLLAMA_MODEL` | `llama3.1:8b` | Default model for inference jobs |
| `SERVER_PORT` | `8081` | HTTP server port |
| `MAX_CONCURRENT` | `2` | Max concurrent inference jobs |
| `MAX_CLIENTS` | `5` | Max registered clients |
| `MESH_ENABLED` | `true` | Enable UDP peer discovery |
| `MESH_DISCOVERY_PORT` | `8082` | UDP discovery port |
| `MESH_SEED_PEERS` | — | Comma-separated seed peer addresses |
| `MESH_ANNOUNCE_ADDRESS` | auto-detected | LAN IP:port for beacon announcements (required in Docker) |
| `MESH_MODEL_MAP` | — | Cross-platform model remapping (e.g. `gemma4:31b-mlx->gemma4:31b`) |
| `SERVER_ID` | `hostname` | Unique server identifier |
| `CUSTOM_PROVIDER_URLS` | — | Comma-separated OpenAI-compatible API URLs |
| `HIVE_DB_PATH` | `/data/hive-server.db` | SQLite database path |

### Cross-Platform Model Mapping

When your mesh has mixed hardware (MLX on Mac, NVIDIA on Linux), use `MESH_MODEL_MAP` to remap model names before forwarding:

```sh
MESH_MODEL_MAP="gemma4:31b-mlx->gemma4:31b,qwen3.6:35b-mlx->qwen3.6:35b"
```

## OpenAI-Compatible API

Point any OpenAI-compatible client (Hermes, OpenCode, Codex, LangChain, etc.) at `http://localhost:8081/v1`.

### `POST /v1/chat/completions`

Supports streaming (SSE) and non-streaming. Automatically forwards to mesh peers when the requested model isn't available locally.

```sh
# Streaming
curl -N -X POST http://localhost:8081/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"qwen3.6:35b","messages":[{"role":"user","content":"Hello"}],"stream":true}'

# Non-streaming
curl -X POST http://localhost:8081/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"llama3.1:8b","messages":[{"role":"user","content":"Hello"}]}'
```

**Auto-forwarding**: If `model` is not found on local Ollama, the server finds the mesh peer that has it and streams the response from there. No client-side logic needed.

### `GET /v1/models`

Returns models from local Ollama + all mesh peers. Hermes `model --refresh` uses this.

```json
{
  "object": "list",
  "data": [
    {"id": "llama3.1:8b", "object": "model", "owned_by": "local"},
    {"id": "qwen3.6:35b", "object": "model", "owned_by": "hive-axiom"},
    {"id": "gemma4:31b", "object": "model", "owned_by": "hive-axiom"}
  ]
}
```

### `GET /v1/health`

```json
{"status": "ok", "server": "hive-server-go", "version": "1.7.0"}
```

## Coding Agent API

Session-based interface for coding agents with context management, automatic compression, and audit logging.

### Quick Start

```sh
# Create session
curl -s -X POST http://localhost:8081/api/agent/sessions \
  -H "Content-Type: application/json" \
  -d '{"agent_type":"hermes","model":"glm-4.7-flash:bf16","system_prompt":"You are a coding assistant.","token_budget":160000}'

# Send message
curl -s -X POST http://localhost:8081/api/agent/sessions/{session_id}/messages \
  -H "Content-Type: application/json" \
  -d '{"role":"user","content":"Write a hello world in Go"}'
```

### Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/agent/sessions` | Create session (auto-detects token budget from model) |
| `GET` | `/api/agent/sessions` | List sessions |
| `GET` | `/api/agent/sessions/{id}` | Get session details |
| `DELETE` | `/api/agent/sessions/{id}` | Delete session |
| `POST` | `/api/agent/sessions/{id}/messages` | Send message (triggers inference) |
| `GET` | `/api/agent/sessions/{id}/messages` | List messages |
| `GET` | `/api/agent/sessions/{id}/context` | Context stats (tokens, compression, model context window) |
| `GET` | `/api/agent/audit` | Search audit logs |
| `GET` | `/api/agent/models` | Available models with detected context windows |

### Context Compression

When token usage exceeds 85% of the budget, older messages are automatically compressed into a summary. The system uses a 3-layer model context detection:

1. **Ollama API** — Queries `/api/show` for actual context window
2. **Hardcoded Registry** — 150+ known models (GLM-4: 200K, Llama 3: 128K, etc.)
3. **Pattern Matching** — Heuristic detection by model name patterns

### Hermes Configuration

```yaml
# ~/.hermes/config.yaml
model:
  name: glm-4.7-flash:bf16
  base_url: http://localhost:8081/v1
providers:
  ollama-launch:
    type: ollama
    name: Hive Server
    api: localhost:8081/v1
```

## API Reference

### Status & Health

**`GET /api/status`** — Server status, queue depth, hardware info, mesh peer count.

**`GET /api/ollama/health`** — Ollama connectivity and available models.

### Client Management

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/clients/register` | Register a client |
| `POST` | `/api/clients/{client_id}/heartbeat` | Keep-alive |
| `POST` | `/api/clients/unregister` | Deregister |
| `GET` | `/api/clients` | List clients |

### Job Queue

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/jobs` | Submit inference job |
| `GET` | `/api/jobs` | List recent jobs |
| `GET` | `/api/jobs/{job_id}` | Poll job result |
| `POST` | `/api/jobs/forward` | Receive forwarded job from peer |
| `GET` | `/api/queue` | Queue status |

#### Job Types

| Type | Payload |
|---|---|
| `generate` | `prompt`, `model`, `system`, `temperature`, `stream` |
| `chat` | `messages`, `model`, `stream` |
| `embed` / `get_embedding` | `prompt`, `text`, `model` |
| `list_models` | — |
| `pull_model` | `name` |
| `coding_agent_chat` | `messages`, `model`, `session_id` |

### Resources

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/nodes` | All nodes with providers and models |
| `GET` | `/api/providers` | Available provider types |
| `GET` | `/api/models` | Aggregated models (deduplicated) |

### Usage Reports

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/reports/usage` | Aggregated token usage |
| `GET` | `/api/reports/usage/recent` | Recent 100 records |
| `GET` | `/api/reports/usage/timeseries` | TPS time-series |
| `GET` | `/api/reports/usage/histogram` | TPS distribution |

### Mesh

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/peers` | Discovered peers with load |
| `POST` | `/api/peers/register` | Manually register peer |
| `POST` | `/api/peers/introduce` | Remote peer self-registration |
| `POST` | `/api/peers/scan` | Re-probe seed peers |
| `GET` | `/api/peers/diagnostics` | Mesh config + model map |

### Observability

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/logs` | Server logs (poll by timestamp) |
| `POST` | `/api/logs/clear` | Clear log buffer |

### Dashboard

**`GET /`** — Live dashboard with stats, queue, mesh, token usage charts, live logs.

## Mesh Discovery

- UDP beacon broadcast every 10s on `MESH_DISCOVERY_PORT` (default 8082)
- Beacons include: `server_id`, `announce_addr`, `port`, `max_concurrent`, `available_capacity`, `pending_jobs`, `running_jobs`
- Peers stale after 30s of no beacon
- Seed peers probed via HTTP `/api/status` every 30s
- Bi-directional introduction via `POST /api/peers/introduce`
- Job forwarding: when local capacity exhausted, forwards to least-loaded peer
- Cross-platform model mapping: `MESH_MODEL_MAP` remaps model names (e.g. MLX→NVIDIA) before forwarding

### Docker Networking

In Docker, set `MESH_ANNOUNCE_ADDRESS` to your host's LAN IP so peers can reach this container:

```sh
MESH_ANNOUNCE_ADDRESS="192.168.1.50:8081"
```

Without this, beacons announce the Docker internal IP which is unreachable from other machines.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Hive Server Go                            │
│                                                               │
│  ┌───────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐  │
│  │ Job Queue  │  │   Mesh   │  │ Provider │  │  Coding    │  │
│  │ (workers)  │  │ Discovery│  │ Manager  │  │  Agent API │  │
│  │ Ollama     │  │ UDP bcast│  │ auto-    │  │  Sessions  │  │
│  │ calls      │  │ seed p/r │  │ detect   │  │  Context   │  │
│  └─────┬─────┘  └──────────┘  └──────────┘  └──────┬─────┘  │
│        │                                             │        │
│  ┌─────▼─────────────────────────────────────────────▼─────┐  │
│  │              OpenAI-Compatible API (:8081)              │  │
│  │  /v1/chat/completions  /v1/models  /v1/health          │  │
│  │  /api/jobs  /api/clients  /api/peers  /api/agent/*     │  │
│  └────────────────────────────┬────────────────────────────┘  │
│                               │                               │
│  ┌────────────────────────────▼────────────────────────────┐  │
│  │  Local Ollama (:11434)  ←→  Mesh Peers (:8081)        │  │
│  │  Auto-forward on 404     Model mapping (MLX↔NVIDIA)    │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  SQLite — token_usage, coding_agent_sessions,          │  │
│  │          coding_agent_messages, coding_agent_audit     │  │
│  │  /data/hive-server.db                                  │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

## Dashboard

- **Stats bar** — workers, queued, running, completed, clients, peers
- **Tabs** — Jobs, Resources, Live Logs (INFO/WARN/ERROR filter)
- **Token Usage** — stats + TPS chart (Line/Hist modes, time range buttons, per-model reports)
- **Mesh Topology** — peer status, load, model map

## Building

```sh
go build ./hive-server-go/
go vet ./hive-server-go/
```

The embedded dashboard at `static/index.html` is compiled into the binary via `//go:embed`.

## License

MIT
