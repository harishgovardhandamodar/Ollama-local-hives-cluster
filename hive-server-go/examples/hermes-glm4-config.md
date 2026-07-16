# Hermes + GLM-4.7-Flash via Hive Server

Configuration guide for connecting Hermes Agent to a local Ollama instance
running `glm-4.7-flash:bf16` through Hive Server's coding agent API.

## Architecture

```
┌──────────────────┐     ┌──────────────────────┐     ┌──────────────┐
│  Hermes Agent     │────▶│  Hive Server Go      │────▶│  Ollama      │
│                   │     │  :8081               │     │  :11434      │
│  ~/.hermes/       │     │                      │     │              │
│  config.yaml      │     │  - Session mgmt      │     │  glm-4.7-    │
│                   │     │  - Context compression│     │  flash:bf16  │
│                   │     │  - Audit logging      │     │              │
│                   │     │  - Token tracking      │     │              │
└──────────────────┘     └──────────────────────┘     └──────────────┘
```

Hermes talks to Hive Server's Agent API (REST), which manages context and
proxies to Ollama. Hermes gets automatic context compression, full audit
logging, and token tracking — things Ollama doesn't provide natively.

---

## Step 1: Pull the Model

```sh
# GLM-4.7-Flash is a 30B-A3B MoE model from ZhipuAI
# bf16 = bfloat16 full precision (~60 GB VRAM or RAM)
ollama pull glm-4.7-flash:bf16

# For quantized variants (less VRAM):
# ollama pull glm-4.7-flash:q4_0    # ~18 GB
# ollama pull glm-4.7-flash:q8_0    # ~34 GB

# Verify
ollama list | grep glm
```

### Context Window

GLM-4.7-Flash has a **200K token context window** — Hive Server auto-detects
this and sets the token budget to ~160K (80% of 200K):

```
Model:    glm-4.7-flash:bf16
Context:  200,000 tokens (detected via registry)
Budget:   160,000 tokens (auto, 80% safety margin)
```

---

## Step 2: Start Hive Server

```sh
# Point Hive Server at your Ollama instance
OLLAMA_BASE_URL=http://localhost:11434 \
OLLAMA_MODEL=glm-4.7-flash:bf16 \
SERVER_PORT=8081 \
MAX_CONCURRENT=2 \
./hive-server-go

# Or with Docker (persistent DB)
docker run -d --name hive-server \
  -p 8081:8081 \
  -v hive-data:/data \
  -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
  -e OLLAMA_MODEL=glm-4.7-flash:bf16 \
  -e MAX_CONCURRENT=2 \
  hive-server-go:latest
```

Verify it's running:

```sh
# Check server status
curl -s http://localhost:8081/api/status | jq '.ollama_model'

# Check Ollama health
curl -s http://localhost:8081/api/ollama/health | jq .

# Check model context detection
curl -s http://localhost:8081/api/agent/models | jq '.live_models[] | select(.name | contains("glm"))'
```

Expected output from `/api/agent/models`:

```json
{
  "name": "glm-4.7-flash:bf16",
  "context_length": 200000,
  "recommended_budget": 160000,
  "detection_source": "registry",
  "size_gb": "60.0"
}
```

---

## Step 3: Configure Hermes

### Option A: Point at Hive Server (Recommended)

Hive Server now exposes an OpenAI-compatible `/v1/chat/completions` endpoint.
Hermes routes through it — getting session management, context compression,
and audit logging for free.

```sh
hermes setup
# Select: "Custom Endpoint"
# Base URL: http://localhost:8081/v1
# API Key: (leave blank)
# Model: glm-4.7-flash:bf16
```

Or edit `~/.hermes/config.yaml` directly:

```yaml
model:
  default: "glm-4.7-flash:bf16"
  provider: "custom"
  base_url: "http://localhost:8081/v1"
  api_key: ""
  context_length: 160000
```

**Why Hive Server instead of Ollama directly?**

| Feature | Ollama (`:11434/v1`) | Hive Server (`:8081/v1`) |
|---------|----------------------|--------------------------|
| Chat completions | Yes | Yes |
| Context compression | No | Auto at 85% budget |
| Session persistence | No | SQLite-backed |
| Audit logging | No | Every prompt/response |
| Token tracking | No | Per-session + global |
| Queue visibility | No | Jobs appear in dashboard |
| Mesh forwarding | No | Auto-reroute to peers |

### Option B: Direct Ollama (No Session Management)

If you don't need context management:

```yaml
model:
  default: "glm-4.7-flash:bf16"
  provider: "custom"
  base_url: "http://localhost:11434/v1"
  api_key: ""
  context_length: 160000
```

But then you lose session persistence, context compression, and audit logging.

---

## Step 4: Custom LLM Client for Hive Server Integration (Optional)

If you want Hermes to use Hive Server for context management (instead of
calling Ollama directly), create a custom LLM client that routes through
the Agent API:

```python
#!/usr/bin/env python3
"""
hermes_hive_client.py — Custom LLM client for Hermes Agent
that routes through Hive Server's coding agent API.

Usage in Hermes:
  - Set this as the LLM client in ~/.hermes/config.yaml
  - Or import and use directly in scripts
"""
import httpx
import json
from typing import AsyncGenerator, Optional

HIVE_URL = "http://localhost:8081"


class HiveLLMClient:
    """LLM client that uses Hive Server's Agent API for context management."""

    def __init__(
        self,
        agent_type: str = "hermes",
        model: str = "glm-4.7-flash:bf16",
        token_budget: int = 0,  # 0 = auto-detect from model
        system_prompt: str = "",
    ):
        self.agent_type = agent_type
        self.model = model
        self.token_budget = token_budget
        self.system_prompt = system_prompt or (
            "You are Hermes, a coding assistant with tool-use capabilities. "
            "When asked to run code, provide the exact command. "
            "When asked to edit files, provide a unified diff. "
            "Always explain your reasoning."
        )
        self.client = httpx.Client(base_url=HIVE_URL, timeout=600.0)
        self.session_id = None

    def create_session(self) -> dict:
        """Create a coding session via Hive Server."""
        payload = {
            "agent_type": self.agent_type,
            "model": self.model,
            "system_prompt": self.system_prompt,
            "metadata": {
                "source": "hermes-agent",
                "tool_calling": True,
            },
        }
        if self.token_budget > 0:
            payload["token_budget"] = self.token_budget

        resp = self.client.post("/api/agent/sessions", json=payload)
        resp.raise_for_status()
        data = resp.json()
        self.session_id = data["session"]["session_id"]
        return data

    def chat(self, message: str, role: str = "user") -> str:
        """Send a message and get a response through Hive Server."""
        if not self.session_id:
            self.create_session()

        resp = self.client.post(
            f"/api/agent/sessions/{self.session_id}/messages",
            json={"role": role, "content": message},
        )
        resp.raise_for_status()
        data = resp.json()
        return data["response"]

    def context_stats(self) -> dict:
        """Get current context usage stats."""
        if not self.session_id:
            return {"error": "no active session"}
        resp = self.client.get(
            f"/api/agent/sessions/{self.session_id}/context"
        )
        return resp.json()

    def audit_log(self, limit: int = 50) -> list:
        """Get the full audit trail for this session."""
        if not self.session_id:
            return []
        resp = self.client.get(
            f"/api/agent/sessions/{self.session_id}/audit",
            params={"limit": limit},
        )
        return resp.json().get("audit_logs", [])


# ── Usage Examples ──────────────────────────────────────────────────

def main():
    # Create a Hermes coding session with GLM-4.7-Flash
    agent = HiveLLMClient(
        agent_type="hermes",
        model="glm-4.7-flash:bf16",
        # token_budget=0  # auto-detect → 160K from 200K context
    )

    # Create session
    session = agent.create_session()
    print(f"Session: {session['session']['session_id']}")
    print(f"Model context: {session['model_context']}")
    print()

    # Coding workflow
    prompts = [
        "Write a Go function that implements a thread-safe LRU cache with expiration",
        "Now add a method to get cache hit/miss statistics",
        "Write tests for the LRU cache",
    ]

    for prompt in prompts:
        print(f">>> {prompt}")
        response = agent.chat(prompt)
        print(response)
        print()

        stats = agent.context_stats()
        print(f"Context: {stats['budget_used_pct']:.1f}% used "
              f"({stats['total_tokens']}/{stats['token_budget']} tokens)")
        print()

    # Full audit trail
    print("=== Audit Trail ===")
    for entry in agent.audit_log():
        content = entry["content"][:80].replace("\n", " ")
        print(f"[{entry['role']:9s}] {content}... ({entry['token_count']} tokens)")


if __name__ == "__main__":
    main()
```

---

## Step 5: Practical Coding Workflow

### Direct API Usage (No Hermes Agent)

If you just want to use the API directly without Hermes Agent:

```bash
# 1. Create a coding session
SESSION=$(curl -s -X POST http://localhost:8081/api/agent/sessions \
  -H 'Content-Type: application/json' \
  -d '{
    "agent_type": "hermes",
    "model": "glm-4.7-flash:bf16",
    "system_prompt": "You are a Go expert working on hive-server-go. Write idiomatic Go code with proper error handling."
  }' | jq -r '.session.session_id')

echo "Session: $SESSION"

# 2. Start coding
curl -s -X POST "http://localhost:8081/api/agent/sessions/$SESSION/messages" \
  -H 'Content-Type: application/json' \
  -d '{"role":"user","content":"Implement a connection pool for HTTP clients with health checks and automatic reconnection"}' \
  | jq -r '.response'

# 3. Follow up (context is managed automatically)
curl -s -X POST "http://localhost:8081/api/agent/sessions/$SESSION/messages" \
  -H 'Content-Type: application/json' \
  -d '{"role":"user","content":"Add a /health endpoint that returns pool stats"}' \
  | jq -r '.response'

# 4. Check context usage (auto-detected 160K budget)
curl -s "http://localhost:8081/api/agent/sessions/$SESSION/context" | jq .
```

### Python Client

```python
import httpx

HIVE = "http://localhost:8081"

# Create session (auto-detects 160K budget from glm-4.7-flash:bf16)
session = httpx.post(f"{HIVE}/api/agent/sessions", json={
    "agent_type": "hermes",
    "model": "glm-4.7-flash:bf16",
    "system_prompt": "You are a Go expert. Write clean, production-ready code.",
}).json()

sid = session["session"]["session_id"]
ctx = session["model_context"]
print(f"Session: {sid}")
print(f"Model context: {ctx['context_length']} tokens ({ctx['source']})")
print(f"Auto budget: {session['session']['token_budget']} tokens")
print()

# Chat loop
while True:
    msg = input(">>> ")
    if not msg:
        break
    resp = httpx.post(
        f"{HIVE}/api/agent/sessions/{sid}/messages",
        json={"role": "user", "content": msg},
        timeout=600.0,
    ).json()
    print(resp["response"])
    print(f"[{resp['total_tokens']}/{resp['token_budget']} tokens]")
```

---

## GLM-4.7-Flash Model Specs

| Property | Value |
|----------|-------|
| **Full name** | GLM-4.7-Flash |
| **Publisher** | ZhipuAI / Z AI |
| **Architecture** | 30B-A3B MoE (Mixture of Experts) |
| **Context window** | 200,000 tokens |
| **Max output** | 131,072 tokens |
| **Function calling** | Yes |
| **Multilingual** | 100+ languages |
| **Ollama tag** | `glm-4.7-flash:bf16` (full precision) |
| **Ollama tag** | `glm-4.7-flash:q4_0` (quantized, ~18 GB) |
| **VRAM (bf16)** | ~60 GB |
| **VRAM (q4_0)** | ~18 GB |
| **Hive detection** | Auto-detected via registry (200K context) |
| **Auto budget** | 160,000 tokens (80% of 200K) |

### Why GLM-4.7-Flash for Coding?

1. **200K context** — fits entire codebases in a single session
2. **Function calling** — reliable tool use for Hermes agent workflows
3. **MoE efficiency** — only 3B active params per token, fast inference
4. **Multilingual** — strong on Go, Python, JS, and 100+ languages
5. **Free tier** — $0/M tokens on many providers

---

## Troubleshooting

### "Iteration budget reached (60/60)" Error

This means Hermes exhausted its iteration limit before completing the task.
Each tool call (file read, shell command, etc.) consumes one iteration.

**Fix 1: Increase the budget in Hermes config**

```yaml
# ~/.hermes/config.yaml
agent:
  max_iterations: 200  # default is 20-60
```

Or set via env:
```sh
export HERMES_MAX_ITERATIONS=200
```

**Fix 2: Use a model with better tool-calling**

GLM-4.7-Flash supports function calling, but smaller/quantized variants may
struggle with clean tool-call formatting, causing retries that eat iterations.

```sh
# Check if the model supports tools
curl -s http://localhost:11434/api/show -d '{"name":"glm-4.7-flash:bf16"}' | jq '.capabilities'
```

**Fix 3: Reduce task complexity per iteration**

Break complex tasks into smaller steps:
```
# Instead of:
"Read all files, analyze the architecture, refactor the auth module, add tests, update docs"

# Do:
"Read main.go and handlers.go, summarize the current auth flow"
"Add a JWT validation middleware to handlers.go"
"Write tests for the new middleware"
```

**Fix 4: Use Hive Server's context management**

When routing through Hive Server (`:8081/v1`), the 200K context window means
Hermes won't lose track of what it's doing mid-task. This reduces wasted
iterations from "re-reading" files the model already saw.

### Coding agent jobs not showing in queue

Make sure Hermes is pointed at **Hive Server** (`localhost:8081/v1`), not
Ollama directly (`localhost:11434/v1`):

```sh
# Verify Hermes is hitting Hive Server
curl -s http://localhost:8081/api/queue | jq .

# Check for coding agent jobs
curl -s http://localhost:8081/api/jobs | jq '.jobs[] | select(.job_type == "coding_agent_chat") | {job_id, status, client_id}'
```

If Hermes is pointed at Ollama directly, jobs never reach Hive Server's queue.

### Model not detected

```sh
# Check if model is in the registry
curl -s http://localhost:8081/api/agent/models | jq '.live_models[] | select(.name | contains("glm"))'

# If empty, check Ollama has the model
ollama list | grep glm

# Force detection via /api/show
curl -s http://localhost:8081/api/agent/models | jq '.registry[] | select(.model | contains("glm"))'
```

### Hermes can't connect

```sh
# Verify Hive Server is running (NOT Ollama — Hermes should hit :8081)
curl http://localhost:8081/api/status

# Verify the OpenAI-compatible endpoint works
curl http://localhost:8081/v1/models | jq .

# Verify Ollama is running (Hive Server connects to this)
curl http://localhost:11434/api/tags

# Check Hermes config
cat ~/.hermes/config.yaml | grep -A5 model
```

### Context compression not triggering

The default threshold is 85% of the token budget. With a 160K budget,
you need ~136K tokens of conversation before compression kicks in. For
most coding sessions this means 50-100+ message exchanges.

```sh
# Check current context usage
curl -s "http://localhost:8081/api/agent/sessions/$SESSION/context" | jq .
```

### Slow responses on bf16

The bf16 variant is full precision (~60 GB). For faster inference:

```sh
# Use quantized version
ollama pull glm-4.7-flash:q4_0

# Update session to use it
curl -X POST http://localhost:8081/api/agent/sessions \
  -d '{"agent_type":"hermes","model":"glm-4.7-flash:q4_0"}'
```

### Verify the full pipeline

```sh
# 1. Hive Server is running
curl -s http://localhost:8081/api/status | jq '.version'

# 2. OpenAI-compatible endpoint works
curl -s http://localhost:8081/v1/models | jq '.data | length'

# 3. Send a test chat completion (same format Hermes uses)
curl -s -X POST http://localhost:8081/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "glm-4.7-flash:bf16",
    "messages": [{"role":"user","content":"Write a Go hello world"}],
    "stream": false
  }' | jq '.choices[0].message.content'

# 4. Check the job appeared in the queue
curl -s http://localhost:8081/api/queue | jq .

# 5. Check audit log
curl -s http://localhost:8081/api/agent/sessions | jq '.sessions[0].session_id' -r | \
  xargs -I{} curl -s http://localhost:8081/api/agent/sessions/{}/audit | jq '.count'
```
