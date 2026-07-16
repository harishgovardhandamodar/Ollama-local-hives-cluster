# Coding Agent API

Hive Server's Coding Agent API provides a session-based interface for coding agents (OpenCode, Hermes, Codex) to interact with local LLMs via Ollama — with automatic context management, compression, and full audit logging.

## Table of Contents

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Running with MLX / Ollama](#running-with-mlx--ollama)
- [API Reference](#api-reference)
- [Agent-Specific Setup](#agent-specific-setup)
  - [OpenCode](#opencode)
  - [Hermes](#hermes)
  - [Codex (OpenAI CLI)](#codex-openai-cli)
- [Context Compression](#context-compression)
- [Audit & Traceability](#audit--traceability)
- [Examples](#examples)
- [Data Persistence](#data-persistence)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Coding Agent (OpenCode / Hermes / Codex)                    │
│  connects to Hive Server on port 8081                        │
└──────────────────────────┬──────────────────────────────────┘
                           │  HTTP REST API
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Hive Server Go                            │
│                                                              │
│  ┌─────────────────────┐  ┌─────────────────────────────┐   │
│  │  Coding Agent API   │  │  Existing Job Queue          │   │
│  │  POST /api/agent/*  │  │  POST /api/jobs              │   │
│  │                     │  │                              │   │
│  │  Session Manager    │  │  Goroutine workers           │   │
│  │  Context Compressor │  │  Ollama proxy                │   │
│  │  Audit Logger       │  │  Token usage tracking        │   │
│  └─────────┬───────────┘  └─────────────────────────────┘   │
│            │                                                 │
│  ┌─────────▼─────────────────────────────────────────────┐   │
│  │              Ollama / LM Studio / vLLM                │   │
│  │              (MLX models via ollama-mlx)               │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │  SQLite (coding_agent_sessions, messages, audit)      │   │
│  │  /data/hive-server.db                                 │   │
│  └───────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

The coding agent API runs **alongside** the existing job queue — it's a separate subsystem optimized for conversational coding workflows. It bypasses the job queue for lower latency (direct Ollama `/api/chat` call) and maintains its own persistent session state.

---

## Quick Start

### 1. Start Hive Server

```sh
# Local
OLLAMA_BASE_URL=http://localhost:11434 OLLAMA_MODEL=qwen2.5-coder:7b ./hive-server-go

# Docker with persistent DB
docker run -d --name hive-server \
  -p 8081:8081 \
  -v hive-data:/data \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  -e OLLAMA_MODEL=qwen2.5-coder:7b \
  hive-server-go:latest
```

### 2. Create a Session

```sh
curl -s -X POST http://localhost:8081/api/agent/sessions \
  -H 'Content-Type: application/json' \
  -d '{
    "agent_type": "opencode",
    "model": "qwen2.5-coder:7b",
    "system_prompt": "You are an expert Go developer. Write clean, idiomatic Go code.",
    "token_budget": 64000
  }'
```

Response:
```json
{
  "session": {
    "session_id": "ca-opencode-1718500000000",
    "agent_type": "opencode",
    "model": "qwen2.5-coder:7b",
    "token_budget": 26214,
    "status": "active",
    "total_messages": 1,
    "total_tokens": 42,
    "compressions": 0
  },
  "model_context": {
    "model": "qwen2.5-coder:7b",
    "context_length": 32768,
    "source": "registry"
  }
}
```

> Note: `token_budget` is auto-detected as 80% of the model's 32K context window.
```

### 3. Send Messages

```sh
curl -s -X POST http://localhost:8081/api/agent/sessions/ca-opencode-1718500000000/messages \
  -H 'Content-Type: application/json' \
  -d '{
    "role": "user",
    "content": "Write a REST API handler in Go for user management with CRUD operations"
  }'
```

### 4. Check Context Usage

```sh
curl -s http://localhost:8081/api/agent/sessions/ca-opencode-1718500000000/context
```

```json
{
  "session_id": "ca-opencode-1718500000000",
  "token_budget": 64000,
  "total_tokens": 1847,
  "budget_used_pct": 2.88,
  "total_messages": 3,
  "summary_messages": 0,
  "compressions": 0,
  "needs_compression": false
}
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server URL |
| `OLLAMA_MODEL` | `llama3.1:8b` | Default model (overridden per-session) |
| `SERVER_PORT` | `8081` | API port |
| `HIVE_DB_PATH` | `/data/hive-server.db` | SQLite path for sessions + audit |

### Session Parameters

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `agent_type` | string | `"custom"` | One of: `opencode`, `hermes`, `codex`, `custom` |
| `model` | string | server default | Ollama model name (e.g. `qwen2.5-coder:7b`) |
| `system_prompt` | string | `""` | System instruction for the coding session |
| `token_budget` | int | **auto-detected** | Max context window in tokens (4,096 – 200,000). If omitted, auto-detected from model. |
| `metadata` | object | `null` | Arbitrary key-value metadata |

### Dynamic Token Budget

When `token_budget` is omitted or `0`, the server **automatically detects** the model's context window using this priority:

1. **Ollama `/api/show`** — queries the live model metadata for `context_length` or `num_ctx`
2. **Model registry** — 150+ known models with their context windows (Qwen, DeepSeek, Llama, Mistral, Gemma, CodeLlama, StarCoder, Yi, Hermes, etc.)
3. **Pattern matching** — fuzzy match on model name substrings
4. **Default** — 8,192 tokens if undetected

The actual token budget is set to **80% of the detected context window**, leaving room for response generation.

```
Model: qwen2.5-coder:14b
  Detected context: 131,072 tokens (via registry)
  Auto budget:     104,857 tokens (80%)
```

```
Model: qwen2.5-coder:7b
  Detected context: 32,768 tokens (via registry)
  Auto budget:      26,214 tokens (80%)
```

You can still override with an explicit `token_budget`:

```sh
# Auto-detect (recommended)
curl -X POST http://localhost:8081/api/agent/sessions \
  -d '{"agent_type":"opencode","model":"qwen2.5-coder:14b"}'

# Explicit override (constrained budget)
curl -X POST http://localhost:8081/api/agent/sessions \
  -d '{"agent_type":"opencode","model":"qwen2.5-coder:14b","token_budget":32000}'
```

---

## Running with MLX / Ollama

### Option A: MLX via Ollama (Recommended)

Ollama supports Apple Silicon MLX models natively. No extra config needed — just pull the model:

```sh
# Pull a coding model
ollama pull qwen2.5-coder:7b
ollama pull qwen2.5-coder:14b     # larger, better quality
ollama pull deepseek-coder-v2:16b  # strong on code

# Pull with specific quantization
ollama pull qwen2.5-coder:7b-q4_K_M

# For general tasks
ollama pull llama3.1:8b
ollama pull mistral:7b
```

Then point Hive Server at it:

```sh
OLLAMA_BASE_URL=http://localhost:11434 \
OLLAMA_MODEL=qwen2.5-coder:7b \
./hive-server-go
```

### Option B: Ollama-mlx (Separate Bridge)

If you use `ollama-mlx` for custom MLX model paths:

```sh
# Start ollama-mlx on a different port
ollama-mlx serve --port 11435

# Point Hive Server at it
OLLAMA_BASE_URL=http://localhost:11435 \
OLLAMA_MODEL=my-mlx-model \
./hive-server-go
```

### Option C: LM Studio with MLX Models

LM Studio serves MLX models on its own port:

```sh
# In LM Studio, load an MLX model and start the server (default: port 1234)
# Then:
OLLAMA_BASE_URL=http://localhost:1234 \
OLLAMA_MODEL=mlx-community/Qwen2.5-Coder-7B-Instruct-4bit \
./hive-server-go
```

### Option D: vLLM with MLX-backed Models

```sh
# Start vLLM serving an MLX-compatible model
python -m vllm.entrypoints.openai.api_server \
  --model mlx-community/Qwen2.5-Coder-7B-Instruct-4bit \
  --port 8000

# Point Hive Server at it (OpenAI-compatible endpoint)
CUSTOM_PROVIDER_URLS=http://localhost:8000 \
./hive-server-go
```

### Docker with MLX (macOS)

```sh
# Docker Desktop on macOS has access to the host network
docker run -d --name hive-server \
  -p 8081:8081 \
  -v hive-data:/data \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  -e OLLAMA_MODEL=qwen2.5-coder:7b \
  hive-server-go:latest
```

### Recommended Models for Coding

| Model | Size | Context Window | Auto Budget (80%) | Best For |
|-------|------|---------------|-------------------|----------|
| `qwen2.5-coder:7b` | 4.4 GB | 32K | ~26K | Day-to-day Go/Python/JS coding |
| `qwen2.5-coder:14b` | 9 GB | 128K | ~102K | Complex refactoring, architecture |
| `deepseek-coder-v2:16b` | 9 GB | 128K | ~102K | Multi-language, large codebases |
| `codellama:13b` | 7.4 GB | 16K | ~13K | Code completion, autocomplete |
| `starcoder2:15b` | 9 GB | 16K | ~13K | Fill-in-the-middle, autocomplete |
| `llama3.1:8b` | 4.7 GB | 128K | ~102K | General tasks, docs, explanations |
| `gemma3:12b` | 8.1 GB | 128K | ~102K | Multi-language coding |

---

## API Reference

### Sessions

#### `POST /api/agent/sessions` — Create Session

```sh
curl -s -X POST http://localhost:8081/api/agent/sessions \
  -H 'Content-Type: application/json' \
  -d '{
    "agent_type": "opencode",
    "model": "qwen2.5-coder:7b",
    "system_prompt": "You are an expert Go developer. Write clean, idiomatic Go code. Always include error handling.",
    "token_budget": 64000,
    "metadata": {
      "project": "hive-serving",
      "branch": "feat/coding-agent-api"
    }
  }'
```

Response:
```json
{
  "session_id": "ca-opencode-1718500000000",
  "agent_type": "opencode",
  "model": "qwen2.5-coder:7b",
  "token_budget": 64000,
  "status": "active",
  "system_prompt": "You are an expert Go developer...",
  "metadata": {"project": "hive-serving", "branch": "feat/coding-agent-api"},
  "created_at": 1718500000.0,
  "updated_at": 1718500000.0,
  "total_messages": 1,
  "total_tokens": 42,
  "compressions": 0
}
```

#### `GET /api/agent/sessions` — List Sessions

```sh
# All sessions
curl -s http://localhost:8081/api/agent/sessions

# Filter by agent type
curl -s "http://localhost:8081/api/agent/sessions?agent_type=opencode"
```

Response:
```json
{
  "sessions": [...],
  "count": 3
}
```

#### `GET /api/agent/sessions/{session_id}` — Get Session

```sh
curl -s http://localhost:8081/api/agent/sessions/ca-opencode-1718500000000
```

#### `DELETE /api/agent/sessions/{session_id}` — Delete Session

```sh
curl -s -X DELETE http://localhost:8081/api/agent/sessions/ca-opencode-1718500000000
```

Removes the session and all associated messages and audit logs from SQLite.

#### `POST /api/agent/sessions/{session_id}/archive` — Archive Session

```sh
curl -s -X POST http://localhost:8081/api/agent/sessions/ca-opencode-1718500000000/archive
```

### Messages

#### `POST /api/agent/sessions/{session_id}/messages` — Send Message

This is the core endpoint. It:
1. Persists the message to SQLite
2. Checks if context compression is needed
3. If so, compresses older messages into a summary
4. Sends the full context to Ollama via `/api/chat`
5. Persists the response
6. Logs everything to the audit table

```sh
curl -s -X POST http://localhost:8081/api/agent/sessions/ca-opencode-1718500000000/messages \
  -H 'Content-Type: application/json' \
  -d '{
    "role": "user",
    "content": "Write a REST API handler in Go for user CRUD operations using the standard library"
  }'
```

Response:
```json
{
  "session_id": "ca-opencode-1718500000000",
  "response": "Here is a Go REST API handler for user CRUD operations...\n\n```go\npackage main\n...",
  "agent_type": "opencode",
  "model": "qwen2.5-coder:7b",
  "total_tokens": 1847,
  "token_budget": 64000,
  "compressions": 0
}
```

**Roles:**
- `"user"` — Human/agent message (default)
- `"assistant"` — LLM response (stored but not triggered by this endpoint)
- `"system"` — System instruction (stored, used as context)

#### `GET /api/agent/sessions/{session_id}/messages` — Get Messages

```sh
# All messages
curl -s http://localhost:8081/api/agent/sessions/ca-opencode-1718500000000/messages

# Last N messages
curl -s "http://localhost:8081/api/agent/sessions/ca-opencode-1718500000000/messages?limit=10"
```

Response:
```json
{
  "session_id": "ca-opencode-1718500000000",
  "messages": [
    {
      "id": "msg-ca-opencode-...-sys-...",
      "session_id": "ca-opencode-...",
      "role": "system",
      "content": "You are an expert Go developer...",
      "token_count": 42,
      "compress_group": 0,
      "is_summary": false,
      "created_at": 1718500000.0
    },
    {
      "id": "msg-ca-opencode-...-1718500001000",
      "role": "user",
      "content": "Write a REST API handler...",
      "token_count": 28,
      "compress_group": 0,
      "is_summary": false,
      "created_at": 1718500001.0
    },
    {
      "role": "assistant",
      "content": "Here is a Go REST API handler...",
      "token_count": 1777,
      "compress_group": 0,
      "is_summary": false,
      "created_at": 1718500005.0
    }
  ],
  "count": 3
}
```

### Model Catalog

#### `GET /api/agent/models` — List Models with Context Windows

Returns both live models from Ollama (with auto-detected context windows) and the full registry of known models.

```sh
curl -s http://localhost:8081/api/agent/models
```

Response:
```json
{
  "live_models": [
    {
      "name": "qwen2.5-coder:7b",
      "context_length": 32768,
      "recommended_budget": 26214,
      "detection_source": "registry",
      "size_gb": "4.4"
    },
    {
      "name": "qwen2.5-coder:14b",
      "context_length": 131072,
      "recommended_budget": 104857,
      "detection_source": "registry",
      "size_gb": "9.0"
    }
  ],
  "registry": [
    {"model": "qwen2.5-coder:7b", "context_length": 32768, "source": "registry"},
    {"model": "qwen2.5-coder:14b", "context_length": 131072, "source": "registry"},
    {"model": "deepseek-coder-v2:16b", "context_length": 128000, "source": "registry"},
    {"model": "llama3.1:8b", "context_length": 131072, "source": "registry"},
    ...
  ]
}
```

### Context Management

#### `GET /api/agent/sessions/{session_id}/context` — Context Stats

```sh
curl -s http://localhost:8081/api/agent/sessions/ca-opencode-1718500000000/context
```

Response:
```json
{
  "session_id": "ca-opencode-1718500000000",
  "token_budget": 26214,
  "total_tokens": 1847,
  "budget_used_pct": 7.04,
  "total_messages": 3,
  "summary_messages": 0,
  "compressions": 0,
  "role_breakdown": {
    "system": 42,
    "user": 845,
    "assistant": 960
  },
  "needs_compression": false,
  "model_context": {
    "model": "qwen2.5-coder:7b",
    "context_length": 32768,
    "detection_source": "registry",
    "budget_ratio": "80%"
  }
}
```

When `needs_compression` is `true`, the next `POST /messages` call will trigger automatic context compression.

### Audit & Search

#### `GET /api/agent/sessions/{session_id}/audit` — Audit Logs

```sh
curl -s http://localhost:8081/api/agent/sessions/ca-opencode-1718500000000/audit
```

Response:
```json
{
  "session_id": "ca-opencode-1718500000000",
  "audit_logs": [
    {
      "id": 1,
      "session_id": "ca-opencode-...",
      "agent_type": "opencode",
      "model": "qwen2.5-coder:7b",
      "role": "system",
      "content": "You are an expert Go developer...",
      "token_count": 42,
      "compress_group": 0,
      "is_summary": false,
      "created_at": 1718500000.0
    },
    {
      "id": 2,
      "role": "user",
      "content": "Write a REST API handler...",
      "token_count": 28,
      "created_at": 1718500001.0
    },
    {
      "id": 3,
      "role": "assistant",
      "content": "Here is a Go REST API handler...",
      "token_count": 1777,
      "created_at": 1718500005.0
    }
  ],
  "count": 3
}
```

The audit log is **immutable** — entries are never updated or deleted (except when the entire session is deleted). This provides a full, tamper-resistant record of every interaction.

#### `GET /api/agent/search` — Search Messages

```sh
curl -s "http://localhost:8081/api/agent/search?q=handler&limit=10"
```

Searches all messages across all sessions for content containing the query string.

---

## Agent-Specific Setup

### OpenCode

OpenCode is a Go-based coding agent that connects to OpenAI-compatible APIs. Since Hive Server's coding agent API is REST-based, you can use it as a proxy.

**Configuration (`opencode.json`):**

```json
{
  "provider": "openai-compatible",
  "api_base": "http://localhost:8081",
  "model": "qwen2.5-coder:7b",
  "temperature": 0.1
}
```

**Direct integration pattern:**

```python
# opencode_client.py — example OpenCode integration with Hive Agent API
import httpx
import json

HIVE_URL = "http://localhost:8081"

class HiveAgentClient:
    def __init__(self, agent_type="opencode", model="qwen2.5-coder:7b", token_budget=0):
        self.client = httpx.Client(base_url=HIVE_URL, timeout=600.0)
        self.session_id = self._create_session(agent_type, model, token_budget)

    def _create_session(self, agent_type, model, token_budget):
        payload = {
            "agent_type": agent_type,
            "model": model,
            "system_prompt": "You are an expert software engineer. Write clean, production-ready code.",
        }
        if token_budget > 0:
            payload["token_budget"] = token_budget
        resp = self.client.post("/api/agent/sessions", json=payload)
        data = resp.json()
        self.context_info = data.get("model_context", {})
        return data["session"]["session_id"]

    def chat(self, message: str) -> str:
        resp = self.client.post(f"/api/agent/sessions/{self.session_id}/messages", json={
            "role": "user",
            "content": message,
        })
        return resp.json()["response"]

    def context_stats(self) -> dict:
        resp = self.client.get(f"/api/agent/sessions/{self.session_id}/context")
        return resp.json()

    def audit_log(self) -> list:
        resp = self.client.get(f"/api/agent/sessions/{self.session_id}/audit")
        return resp.json()["audit_logs"]


# Usage
agent = HiveAgentClient(model="qwen2.5-coder:14b")
response = agent.chat("Write a Fibonacci function in Go with memoization")
print(response)
print(f"Context: {agent.context_stats()['budget_used_pct']:.1f}% used")
```

### Hermes

Hermes uses function calling and tool-use patterns. The Hive Agent API supports this by allowing custom metadata and system prompts.

**Hermes session with tool-calling context:**

```sh
curl -s -X POST http://localhost:8081/api/agent/sessions \
  -H 'Content-Type: application/json' \
  -d '{
    "agent_type": "hermes",
    "model": "qwen2.5-coder:14b",
    "system_prompt": "You are Hermes, a coding assistant with tool-use capabilities. When asked to run code, provide the exact command. When asked to edit files, provide a unified diff. Always explain your reasoning.",
    "metadata": {
      "tools_enabled": true,
      "allowed_tools": ["bash", "read_file", "write_file", "grep"],
      "max_tool_calls": 10
    }
  }'
```

**Hermes Python client:**

```python
import httpx
import json

HIVE_URL = "http://localhost:8081"

def hermes_session(model="qwen2.5-coder:14b"):
    """Create a Hermes coding session with tool-use metadata.
    Token budget is auto-detected from the model."""
    resp = httpx.post(f"{HIVE_URL}/api/agent/sessions", json={
        "agent_type": "hermes",
        "model": model,
        "system_prompt": (
            "You are Hermes, a coding assistant. "
            "When you need to run a command, output: ```bash\n<command>\n```\n"
            "When you need to edit a file, output a unified diff. "
            "Always explain what you're doing."
        ),
        "metadata": {"tools_enabled": True},
    })
    return resp.json()["session_id"]

def hermes_chat(session_id, message):
    """Send a message and get a response."""
    resp = httpx.post(
        f"{HIVE_URL}/api/agent/sessions/{session_id}/messages",
        json={"role": "user", "content": message},
        timeout=600.0,
    )
    data = resp.json()
    return data["response"]

# Usage
sid = hermes_session()
print(hermes_chat(sid, "Write a Python script to monitor CPU usage and alert when > 90%"))
print(hermes_chat(sid, "Now add logging to a file"))
# Context is managed automatically — old messages get compressed when nearing the token budget
```

### Codex (OpenAI CLI)

The OpenAI Codex CLI can be configured to use Hive Server as its backend. Since Hive Server exposes an OpenAI-compatible proxy path, Codex can connect directly.

**Codex configuration (`~/.codex/config.json`):**

```json
{
  "api_base_url": "http://localhost:8081",
  "model": "qwen2.5-coder:7b",
  "temperature": 0.1
}
```

**Using the Agent API from Codex scripts:**

```python
#!/usr/bin/env python3
"""codex_hive.py — Codex CLI wrapper using Hive Agent API."""
import httpx
import sys

HIVE_URL = "http://localhost:8081"

def create_codex_session(model="qwen2.5-coder:7b"):
    """Create a Codex-style session with code-first system prompt.
    Token budget is auto-detected from the model."""
    resp = httpx.post(f"{HIVE_URL}/api/agent/sessions", json={
        "agent_type": "codex",
        "model": model,
        "system_prompt": (
            "You are Codex, a code generation assistant. "
            "Generate production-ready code. "
            "Always include: error handling, types, and brief comments. "
            "Output code blocks with language tags."
        ),
    })
    return resp.json()["session_id"]

def codex_generate(session_id, prompt):
    """Generate code from a prompt."""
    resp = httpx.post(
        f"{HIVE_URL}/api/agent/sessions/{session_id}/messages",
        json={"role": "user", "content": prompt},
        timeout=600.0,
    )
    data = resp.json()
    print(f"[tokens: {data['total_tokens']}/{data['token_budget']}]")
    return data["response"]

if __name__ == "__main__":
    prompt = sys.argv[1] if len(sys.argv) > 1 else "Write a Go HTTP middleware for request logging"
    sid = create_codex_session()
    print(codex_generate(sid, prompt))
```

---

## Context Compression

### How It Works

When a session's token count exceeds 85% of its auto-detected `token_budget`, the next message triggers automatic context compression:

```
Model: qwen2.5-coder:14b (128K context, 80% budget = 102K)
Token usage: ████████████████░░░░ 85.2%  ← triggers compression
                              ↓
┌──────────────────────────────────────────────────────┐
│ Compression:                                         │
│                                                      │
│ [System prompt]  ← always kept                        │
│ [Summary msg]    ← compressed from old messages      │
│ [Recent msg 1]   ← kept                              │
│ [Recent msg 2]   ← kept                              │
│ [Recent msg 3]   ← kept                              │
│ [Recent msg 4]   ← kept                              │
│ [Recent msg 5]   ← kept                              │
│ [Recent msg 6]   ← kept                              │
│                                                      │
│ Token usage: ████████░░░░░░░░░░░░ 42.1%  ← after    │
└──────────────────────────────────────────────────────┘
```

**Compression strategy:**
1. Always preserve the original system prompt (message 0)
2. Keep the most recent 6 messages intact
3. Summarize all older messages into a single `context_summary` message
4. The summary preserves: key decisions, file/function names, error solutions, code patterns, task state
5. Compression uses `temperature: 0.1` for deterministic summaries

**Compression is transparent to your agent.** After compression:
- The session's `total_tokens` is recalculated
- The `compressions` count increments
- The audit log records the compression event
- Your agent sees a shorter, more relevant context

### Controlling Context Size

```sh
# Auto-detect from model (recommended)
curl -s -X POST http://localhost:8081/api/agent/sessions \
  -d '{"agent_type":"opencode","model":"qwen2.5-coder:7b"}'

# Override with explicit budget (smaller = faster, cheaper)
curl -s -X POST http://localhost:8081/api/agent/sessions \
  -d '{"agent_type":"opencode","model":"qwen2.5-coder:7b","token_budget":16000}'

# Override with full context (for complex tasks)
curl -s -X POST http://localhost:8081/api/agent/sessions \
  -d '{"agent_type":"opencode","model":"qwen2.5-coder:14b","token_budget":131072}'
```

### Token Budget Guidelines

When `token_budget` is omitted, the server auto-detects from the model:

| Model | Context Window | Auto Budget (80%) | Best For |
|-------|---------------|-------------------|----------|
| `qwen2.5-coder:7b` | 32K | ~26K | Most coding tasks |
| `qwen2.5-coder:14b` | 128K | ~102K | Multi-file refactoring |
| `deepseek-coder-v2:16b` | 128K | ~102K | Large codebase tasks |
| `llama3.1:8b` | 128K | ~102K | General + code |
| `codellama:13b` | 16K | ~13K | Code completion |
| `gemma3:12b` | 128K | ~102K | Multi-language |

To override, pass an explicit `token_budget`:

```sh
# Use the full context window (no safety margin)
curl -X POST http://localhost:8081/api/agent/sessions \
  -d '{"agent_type":"opencode","model":"qwen2.5-coder:14b","token_budget":131072}'

# Constrain to a smaller budget (for cost/speed savings)
curl -X POST http://localhost:8081/api/agent/sessions \
  -d '{"agent_type":"opencode","model":"qwen2.5-coder:14b","token_budget":32000}'
```

---

## Audit & Traceability

Every interaction is logged to three SQLite tables:

### `coding_agent_sessions`
Session metadata — agent type, model, token budget, compression count, timestamps.

### `coding_agent_messages`
The full conversation history — all messages in chronological order, including compressed summaries.

### `coding_agent_audit`
An **immutable** append-only log of every prompt and response. Never modified, only deleted when the session is deleted. Contains:
- Session ID, agent type, model
- Role (`user`, `assistant`, `system`)
- Full content
- Token count
- Compression group (which compression cycle it belongs to)
- Timestamp

### Querying Audit Logs Directly

The SQLite database at `HIVE_DB_PATH` can be queried directly:

```sql
-- All interactions for a session
SELECT id, role, token_count, created_at, substr(content, 1, 100) as preview
FROM coding_agent_audit
WHERE session_id = 'ca-opencode-1718500000000'
ORDER BY id;

-- Token usage over time
SELECT date(created_at, 'unixepoch') as day, sum(token_count) as tokens
FROM coding_agent_audit
GROUP BY day ORDER BY day;

-- Find all compression events
SELECT * FROM coding_agent_audit
WHERE is_summary = 1
ORDER BY created_at DESC;

-- Most expensive prompts (by token count)
SELECT role, token_count, substr(content, 1, 200) as preview
FROM coding_agent_audit
ORDER BY token_count DESC LIMIT 20;

-- Sessions by agent type
SELECT agent_type, count(*) as sessions, sum(token_count) as total_tokens
FROM coding_agent_audit
GROUP BY agent_type;
```

---

## Examples

### Full Coding Workflow

```bash
# 1. Create session with project-specific context
#    (token_budget auto-detected from qwen2.5-coder:7b → ~26K tokens)
SESSION=$(curl -s -X POST http://localhost:8081/api/agent/sessions \
  -H 'Content-Type: application/json' \
  -d '{
    "agent_type": "opencode",
    "model": "qwen2.5-coder:7b",
    "system_prompt": "You are working on hive-server-go, a Go inference server. Current version: 1.7.0. Use net/http, modernc.org/sqlite, and sync packages. Follow existing code conventions.",
    "metadata": {"project": "hive-server-go", "language": "go"}
  }' | jq -r '.session.session_id')

echo "Session: $SESSION"

# 2. Add context about the current task
curl -s -X POST "http://localhost:8081/api/agent/sessions/$SESSION/messages" \
  -H 'Content-Type: application/json' \
  -d '{"role": "user", "content": "I need to add a new API endpoint for user authentication. The current auth is handled in handlers.go with middleware."}'

# 3. Ask for implementation
curl -s -X POST "http://localhost:8081/api/agent/sessions/$SESSION/messages" \
  -H 'Content-Type: application/json' \
  -d '{"role": "user", "content": "Implement a JWT-based auth middleware that validates tokens from the Authorization header and adds user info to the request context."}'

# 4. Follow up
curl -s -X POST "http://localhost:8081/api/agent/sessions/$SESSION/messages" \
  -H 'Content-Type: application/json' \
  -d '{"role": "user", "content": "Now add rate limiting per user: 100 requests per minute, stored in an in-memory sync.Map."}'

# 5. Check context usage
curl -s "http://localhost:8081/api/agent/sessions/$SESSION/context" | jq .

# 6. Review audit trail
curl -s "http://localhost:8081/api/agent/sessions/$SESSION/audit" | jq '.audit_logs | length'
```

### Multi-Session Workflow

```bash
# Session for backend work (14B model → auto ~102K budget)
BACKEND=$(curl -s -X POST http://localhost:8081/api/agent/sessions \
  -d '{"agent_type":"opencode","model":"qwen2.5-coder:14b"}' \
  | jq -r '.session.session_id')

# Session for frontend work (smaller model → auto ~26K budget)
FRONTEND=$(curl -s -X POST http://localhost:8081/api/agent/sessions \
  -d '{"agent_type":"opencode","model":"llama3.1:8b"}' \
  | jq -r '.session.session_id')

# Work on both in parallel
curl -s -X POST "http://localhost:8081/api/agent/sessions/$BACKEND/messages" \
  -d '{"role":"user","content":"Design the database schema for user sessions"}' &

curl -s -X POST "http://localhost:8081/api/agent/sessions/$FRONTEND/messages" \
  -d '{"role":"user","content":"Build a login form with React and Tailwind"}' &

wait
```

### Search Across Sessions

```sh
# Find all messages mentioning "middleware"
curl -s "http://localhost:8081/api/agent/search?q=middleware" | jq .

# Find all messages about a specific function
curl -s "http://localhost:8081/api/agent/search?q=handleAuth" | jq .
```

---

## Data Persistence

All session data is stored in SQLite at `HIVE_DB_PATH` (default: `/data/hive-server.db`).

### Docker with Persistent Storage

```sh
docker run -d --name hive-server \
  -p 8081:8081 \
  -v hive-data:/data \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  -e OLLAMA_MODEL=qwen2.5-coder:7b \
  hive-server-go:latest
```

### Custom DB Path

```sh
HIVE_DB_PATH=/var/lib/hive/agent-sessions.db ./hive-server-go
```

### Backup the Database

```sh
# Copy the SQLite file
docker cp hive-server:/data/hive-server.db ./backup/hive-server-$(date +%Y%m%d).db

# Or use sqlite3 dump
sqlite3 ./hive-server.db ".dump" > backup.sql
```

### Database Schema

```sql
-- Sessions
CREATE TABLE coding_agent_sessions (
    id TEXT PRIMARY KEY,
    agent_type TEXT NOT NULL,
    model TEXT NOT NULL,
    token_budget INTEGER NOT NULL DEFAULT 32000,
    status TEXT NOT NULL DEFAULT 'active',
    system_prompt TEXT NOT NULL DEFAULT '',
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    total_messages INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    compressions INTEGER NOT NULL DEFAULT 0
);

-- Messages (conversation history)
CREATE TABLE coding_agent_messages (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    token_count INTEGER NOT NULL DEFAULT 0,
    compress_group INTEGER NOT NULL DEFAULT 0,
    is_summary INTEGER NOT NULL DEFAULT 0,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL
);

-- Audit log (immutable)
CREATE TABLE coding_agent_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    agent_type TEXT NOT NULL,
    model TEXT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    token_count INTEGER NOT NULL DEFAULT 0,
    compress_group INTEGER NOT NULL DEFAULT 0,
    is_summary INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL
);
```
