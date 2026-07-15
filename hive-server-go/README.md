# Hive Server Go

A standalone Go inference orchestration server for Ollama, LM Studio, vLLM, and OpenAI-compatible providers. Features concurrent job queuing, peer mesh discovery, GPU-aware hardware metrics, SQLite-backed token usage reporting, and a live dashboard.

## Quick Start

```sh
# Local
go build -o hive-server-go ./hive-server-go/
OLLAMA_BASE_URL=http://localhost:11434 ./hive-server-go

# Docker (database lives in container — data lost on recreate)
docker run -d --name hive-server -p 8081:8081 \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  hive-server-go:latest

# Docker with persistent SQLite database
docker run -d --name hive-server \
  -p 8081:8081 \
  -v hive-data:/data \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  hive-server-go:latest

# Docker with Nvidia GPU (Linux)
docker run -d --name hive-server --gpus all --network host \
  -e OLLAMA_BASE_URL=http://localhost:11434 \
  -v hive-data:/data \
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
| `SERVER_ID` | `hostname` | Unique server identifier |
| `CUSTOM_PROVIDER_URLS` | — | Comma-separated OpenAI-compatible API URLs |
| `HIVE_DB_PATH` | `/data/hive-server.db` | SQLite database path (persist with `-v volume:/data`) |

## API Reference

### Status & Health

**`GET /api/status`** — Server status, queue depth, hardware info, mesh peer count.

```json
{
  "server_id": "hive-hostname",
  "version": "1.0.0",
  "uptime": "5m32s",
  "ollama_url": "http://localhost:11434",
  "ollama_model": "llama3.1:8b",
  "queue": {
    "pending": 0,
    "running": 2,
    "completed_recent": 15,
    "max_concurrent": 2,
    "ollama_url": "http://localhost:11434",
    "ollama_model": "llama3.1:8b"
  },
  "clients": 3,
  "max_clients": 5,
  "mesh_enabled": true,
  "peers": 2,
  "hardware": {
    "platform": "linux",
    "architecture": "arm64",
    "gpus": [{"model":"NVIDIA A100","memory_gb":80,"vram_used_gb":32.5,"vram_total_gb":80.0,"driver_version":"550.54"}]
  }
}
```

**`GET /api/ollama/health`** — Ollama connectivity and available models.

### Client Management

**`POST /api/clients/register`** — Register a client.
```json
// Request:  {"client_id": "my-app", "name": "My Application"}
// Response: {"client_id": "my-app", "name": "My Application", "connected_at": ..., "last_heartbeat": ...}
```

**`POST /api/clients/{client_id}/heartbeat`** — Keep-alive (clients stale after 120s are removed).

**`POST /api/clients/unregister`** — Deregister a client.

**`GET /api/clients`** — List all connected clients.

### Job Queue

**`POST /api/jobs`** — Submit an inference job.
```json
// Request:
{
  "client_id": "my-app",
  "job_type": "generate",
  "payload": {"prompt": "Why is the sky blue?", "model": "llama3.1:8b", "stream": false}
}
// Response:
{
  "job_id": "my-app:generate:1234567890123",
  "status": "running",
  "client_id": "my-app",
  "job_type": "generate",
  "created_at": 1234567890.123
}
```

**`GET /api/jobs`** — List recent jobs (last 1 hour).

**`GET /api/jobs/{job_id}`** — Poll a specific job for result.

**`POST /api/jobs/forward`** — Receive a forwarded job from a mesh peer.

**`GET /api/queue`** — Queue status (pending, running, completed counts).

#### Supported Job Types

| Type | Description | Payload Fields |
|---|---|---|
| `generate` | Ollama text generation | `prompt`, `model`, `system`, `temperature`, `stream` |
| `chat` | Chat completion | `messages`, `model`, `stream` |
| `embed` / `get_embedding` | Text embeddings | `prompt`, `text`, `model` |
| `list_models` | List available Ollama models | — |
| `pull_model` | Pull a model to Ollama | `name` |
| `custom_prompt` | Arbitrary Ollama prompt | `path`, plus any Ollama API fields |

Unknown job types are forwarded as generic prompts to Ollama, with the job type name and payload text included in the prompt.

### Resources (Nodes, Providers, Models)

**`GET /api/nodes`** — All nodes (self + mesh peers) with their providers and models.

```json
{
  "nodes": [{
    "id": "hive-server-1",
    "endpoint": "http://192.168.1.10:8081",
    "is_self": true,
    "alive": true,
    "providers": [{
      "type": "ollama",
      "base_url": "http://localhost:11434",
      "healthy": true,
      "models": [
        {"name": "llama3.1:8b", "provider": "ollama", "node_id": "hive-server-1", "size_gb": "4.9"}
      ]
    }]
  }]
}
```

**`GET /api/providers`** — Available provider types across the mesh.
```json
{"providers": ["ollama", "lm_studio", "vllm"]}
```

**`GET /api/models`** — Aggregated model list across all nodes and providers (deduplicated).

### Usage Reports

**`GET /api/reports/usage`** — Aggregated token usage by provider/model/node.

**`GET /api/reports/usage/recent`** — Recent 100 token usage records.

**`GET /api/reports/usage/timeseries?model=&since=`** — TPS time-series points for line chart.

**`GET /api/reports/usage/histogram?model=&since=&bins=25`** — Binned TPS distribution from SQLite.

### Mesh

**`GET /api/peers`** — Discovered mesh peers with load and capacity info.
**`POST /api/peers/register`** — Manually register a mesh peer.
**`POST /api/peers/introduce`** — Allow a remote peer to register itself.
**`POST /api/peers/scan`** — Re-probe seed peers.
**`GET /api/peers/diagnostics`** — Mesh configuration and status.

### Observability

**`GET /api/logs?since=<epoch>`** — Server logs with timestamp-based polling.
**`POST /api/logs/clear`** — Clear the in-memory log buffer.

### Dashboard

**`GET /`** — Live dashboard HTML with stats, mesh topology, clients, queue, token usage, and live logs.

## Data Persistence

Token usage metrics are stored in a SQLite database. By default the database lives at `/data/hive-server.db` inside the container. To persist data across container restarts, mount a volume:

```sh
docker run -d --name hive-server \
  -v hive-data:/data \
  ... hive-server-go:latest
```

To use a custom path, set `HIVE_DB_PATH`:

```sh
docker run -d --name hive-server \
  -v ./db:/app/db \
  -e HIVE_DB_PATH=/app/db/metrics.db \
  ... hive-server-go:latest
```

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  Hive Server Go                   │
│                                                   │
│  ┌──────────┐  ┌──────────┐  ┌────────────────┐  │
│  │ Job Queue │  │  Mesh    │  │   Provider     │  │
│  │ (workers) │  │ Discovery│  │   Manager      │  │
│  │ Ollama    │  │ UDP bcast│  │ Ollama/LM/vLLM │  │
│  │ calls     │  │ seed p/r │  │ auto-detect    │  │
│  └────┬─────┘  └──────────┘  └────────────────┘  │
│       │                                            │
│  ┌────▼───────────────────────────────────────┐    │
│  │           HTTP API (port 8081)              │    │
│  │  /api/jobs  /api/clients  /api/nodes       │    │
│  │  /api/peers /api/logs    /api/reports/usage│    │
│  └────────────────────────────────────────────┘    │
│       │                                            │
│  ┌────▼───────────────────────────────────────┐    │
│  │  SQLite (token_usage table)                │    │
│  │  /data/hive-server.db                      │    │
│  └────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────┘
```

## Dashboard

The dashboard (served at `/`) has:
- **Stats bar** — workers, queued, running, completed, clients, peers
- **Main grid** — Connected Clients, Job Queue, Mesh Peers, Ollama Health, **Token Usage** (stats + TPS chart with Line/Hist views + time range buttons + per-model reports)
- **Sidebar** — Mesh Topology, **Live Logs** (with INFO/WARN/ERROR filter)

## Developer Notes

### Building

```sh
go build ./hive-server-go/
go test ./internal/...
```

The `hive-server-go/` package is part of the `github.com/hive-cluster/hive-serving` module. The embedded dashboard at `static/index.html` is compiled into the binary via `//go:embed`.

### Adding a New Provider Type

1. Add the provider type constant in `provider.go`:
   ```go
   const ProviderMyEngine ProviderType = "my_engine"
   ```
2. Add a probe method:
   ```go
   func (pm *ProviderManager) probeMyEngine(nodeID string) *ProviderInfo { ... }
   ```
3. Call it from `detectLocalProviders()` and `detectPeerProviders()`.

### Mesh Discovery Protocol

- Peers broadcast a UDP beacon every 10s on `MESH_DISCOVERY_PORT` (default 8082)
- Beacons contain: `server_id`, `port`, `max_concurrent`, `pending_jobs`, `running_jobs`, `clients`
- Peers are considered stale after 30s of no beacon
- Seed peers are probed via HTTP `/api/status` every 30s
- On seed probe, the server also introduces itself to the peer via `POST /api/peers/introduce`
- Job forwarding: when local capacity is exhausted, the server selects the best peer (lowest load) and forwards via `POST /api/jobs/forward`

### Job Queue

- Goroutine-based worker pool with `sync.Cond` for dispatch
- Workers are bounded by `MAX_CONCURRENT`
- Completed jobs are retained for 1 hour in memory
- Token metrics are extracted from Ollama responses and stored in SQLite
- Unknown job types fall back to a generic Ollama prompt with the payload as context

### Extending the Dashboard

Edit `static/index.html` to add views, tabs, or visualizations. Rebuild the binary — the HTML is embedded at compile time.
