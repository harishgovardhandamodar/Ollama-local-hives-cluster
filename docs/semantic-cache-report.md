# Semantic Cache Feature Report

## Overview

Semantic caching for LLM request/response pairs with similarity-based matching, persistent storage, and a live dashboard. The cache intercepts requests before they reach the LLM backend, returning cached responses when a semantically similar prompt has been seen before.

## Architecture

```
Client Request
    │
    ▼
┌──────────────────┐
│  HTTP Handler     │
│  (handlers.go)    │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐      ┌───────────────────┐      ┌─────────────────┐
│  Proxy            │─────▶│  Semantic Cache    │─────▶│  Vector Index   │
│  (proxy.go)       │◀─────│  (semanticcache.go)│      │  (LSH / Flat)   │
└────────┬─────────┘      └─────────┬─────────┘      └─────────────────┘
         │                          │
         ▼                          ▼
┌──────────────────┐      ┌───────────────────┐
│  LLM Backend     │      │  SQLite (WAL)      │
│  (Ollama/vLLM)   │      │  Persistence       │
└──────────────────┘      └───────────────────┘
```

## Components

### 1. Core Cache Engine — `internal/semanticcache/semanticcache.go`

| Component | Description |
|-----------|-------------|
| `SemanticCache` struct | Thread-safe cache with `sync.RWMutex`, vector index, embedding map |
| `Get()` | Exact SHA256 key match first, then index-assisted cosine similarity search |
| `Set()` | Stores entry with embedding (if Ollama available), adds to index, triggers async DB save |
| `evictOldest()` | Removes oldest entry by `CreatedAt` when at capacity |
| `evictionLoop()` | Background goroutine evicts expired entries every 60s |
| `migrate()` / `loadFromDB()` | SQLite schema creation and startup hydration |
| `WarmFromFile()` | Preloads cache from JSONL export file at startup |
| `resolveEmbeddingModel()` | Returns per-model embedding model or falls back to default |
| `cosineSimilarity()` | Pure Go implementation, no external dependencies |
| `serializeEmbedding()` / `deserializeEmbedding()` | JSON-based float32 array encoding for SQLite BLOB storage |

### 2. Vector Index — `internal/semanticcache/vectorindex.go`

| Component | Description |
|-----------|-------------|
| `VectorIndex` interface | Pluggable interface: `Insert`, `Remove`, `Search`, `Len` |
| `FlatIndex` | O(n) linear scan — simple, no preprocessing, best for <1k entries |
| `LSHIndex` | Locality-Sensitive Hashing with 16 random hyperplanes, 256 buckets — sub-linear candidate retrieval |
| `LSHIndex.hash()` | Projects vector onto 16 random hyperplanes → 16-bit signature → bucket index |
| `LSHIndex.Search()` | Queries candidate bucket + neighbors (-1, 0, +1), filters by model/TTL, ranks by cosine similarity |

**Performance comparison:**

| Index | Build | Search | Memory | Best for |
|-------|-------|--------|--------|----------|
| `FlatIndex` | O(1) | O(n × d) | O(n × d) | <1k entries, exact results |
| `LSHIndex` | O(n × d) | O(c × d) where c ≪ n | O(n × d + 256 × n) | >1k entries, approximate OK |

### 3. Embedding Cache — `internal/semanticcache/semanticcache.go`

| Component | Description |
|-----------|-------------|
| `EmbeddingCache` struct | LRU in-memory cache mapping `model:text` → embedding vector |
| `Get()` / `Set()` | O(1) lookup, bounded capacity with LRU eviction |
| `Clear()` | Resets cache (called on full `Clear()`) |

**Impact:** Avoids redundant Ollama API calls for the same prompt. At 50% cache hit rate on embeddings, reduces embedding latency by ~50% on repeat queries.

### 4. Streaming Response Buffering — `internal/proxy/proxy.go`

| Component | Description |
|-----------|-------------|
| `handleStreamResponse()` | Buffers streaming chunks up to `StreamBufferMaxSize` (default 1MB) |
| `extractStreamResponse()` | Reassembles NDJSON chunks into a single response string |
| `writeCachedStreamResponse()` | Converts cached response back to streaming NDJSON format |
| Cache-on-complete | If full stream fits in buffer, caches the assembled response |

**Flow for streaming requests:**
1. Client requests streaming response
2. Proxy checks cache — if hit, streams cached response as NDJSON chunks
3. If miss, forwards to LLM backend, buffers chunks while streaming to client
4. If complete stream fits in buffer (< `StreamBufferMaxSize`), caches the full response
5. Next identical/similar request gets a streaming cache hit

### 5. Per-Model Embedding Configuration — `config/config.go`

```yaml
semantic_cache:
  model_embedding_map:
    llama3: nomic-embed-text
    mistral: bge-small
    codellama: code-embeddings
    qwen2: nomic-embed-text
```

Or via environment variable:
```bash
SEMANTIC_CACHE_MODEL_EMBEDDING_MAP="llama3:nomic-embed-text,mistral:bge-small"
```

**Behavior:** Each LLM model can use a different embedding model. If a model is not in the map, falls back to the default `embedding_model`.

### 6. Cache Warming — `semanticcache.go:WarmFromFile()`

```yaml
semantic_cache:
  warmup_file: /data/cache-warmup.jsonl
```

**JSONL format** (one JSON object per line):
```jsonl
{"model":"llama3","prompt":"What is Go?","response":"Go is a programming language."}
{"model":"mistral","prompt":"What is Python?","response":"Python is a high-level language."}
```

**Behavior:**
- Reads JSONL file at startup
- Deduplicates by `model:prompt` key
- Generates embeddings via Ollama for each entry
- Persists to SQLite for subsequent restarts
- Logs count of warmed entries

### 7. Cache Integration — `internal/proxy/proxy.go`

- **Pre-forward lookup**: Before sending to LLM backend, checks cache for exact key match (no Ollama call needed)
- **Post-response store**: After successful LLM response, asynchronously stores with embedding
- **Streaming support**: Buffers and caches streaming responses
- **Response headers**: `X-Cache: HIT` or `X-Cache: MISS`, `X-Cache-Entry-ID: <id>`

### 8. API Endpoints — `internal/api/handlers.go`

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/cache/stats` | GET | Returns hit/miss counts, hit rate, entry count, index stats, embedding cache stats |
| `/api/cache/entries` | GET | Lists all cached entries sorted by hit count |
| `/api/cache/clear` | POST | Clears all entries, resets stats, clears index, truncates SQLite table |
| `/api/cache/invalidate` | POST | Removes a specific entry by ID |

### 9. Dashboard — `web/index.html`

- Cache stats cards: entries, hit rate, hits, misses
- Index type and size display
- Embedding cache hit/miss stats
- Live entry list with prompt, model, hit count, timestamps
- Refresh / Clear buttons
- Auto-refreshes every 10 seconds

## Configuration

```yaml
semantic_cache:
  enabled: false
  max_entries: 1000
  ttl_seconds: 300
  similarity_threshold: 0.92
  embedding_model: nomic-embed-text
  index_type: lsh
  embedding_cache_size: 5000
  warmup_file: ""
  model_embedding_map:
    llama3: nomic-embed-text
    mistral: bge-small

stream_buffer:
  max_size: 1048576
  timeout_seconds: 30
```

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Master toggle |
| `max_entries` | `1000` | LRU eviction capacity |
| `ttl_seconds` | `300` | Entry expiration (5 minutes) |
| `similarity_threshold` | `0.92` | Minimum cosine similarity for a semantic match |
| `embedding_model` | `nomic-embed-text` | Default Ollama model for embeddings |
| `index_type` | `lsh` | Vector index: `"lsh"` or `"flat"` |
| `embedding_cache_size` | `5000` | Max cached embeddings in memory |
| `warmup_file` | `""` | Path to JSONL file for cache warming |
| `model_embedding_map` | `{}` | Per-model embedding model overrides |
| `stream_buffer.max_size` | `1048576` | Max bytes to buffer for streaming cache (1MB) |
| `stream_buffer.timeout_seconds` | `30` | Stream buffer timeout |

## How It Works

1. **Request arrives** at the proxy
2. **Exact key lookup**: SHA256 hash of `model:prompt:messages` — O(1) check, no embedding call
3. **If miss and index has entries**: Check embedding cache first; if miss, query Ollama for embedding. Then search vector index (LSH candidate retrieval → cosine re-ranking)
4. **If similarity >= threshold (0.92)**: Return cached response, increment hit count
5. **If miss**: Forward to LLM backend. For streaming: buffer chunks, cache if complete. For sync: cache full response.
6. **TTL eviction**: Background goroutine removes entries older than `ttl_seconds` every minute
7. **LRU eviction**: When entries exceed `max_entries`, oldest by creation time is removed
8. **Cache warming**: On startup, load entries from JSONL file if configured

## Test Results

### Test Suite Summary

All tests pass: `go test ./...` — 0 failures, ~5.6s total.

### New Feature Tests

| Test | Feature | What It Validates |
|------|---------|-------------------|
| `TestFlatIndexInsertAndSearch` | Vector Index | Insert + search with model filter |
| `TestFlatIndexSearchK` | Vector Index | Top-k retrieval |
| `TestFlatIndexModelFilter` | Vector Index | Cross-model isolation |
| `TestFlatIndexRemove` | Vector Index | Entry removal from index |
| `TestLSHIndexInsertAndSearch` | LSH Index | Insert + search with hash bucketing |
| `TestLSHIndexSearchK` | LSH Index | Top-k retrieval |
| `TestLSHIndexModelFilter` | LSH Index | Cross-model isolation |
| `TestLSHIndexRemove` | LSH Index | Entry removal from hash buckets |
| `TestLSHIndexHighDimensional` | LSH Index | 768-dim vectors, 50 entries |
| `TestLSHIndexDifferentDimensions` | LSH Index | 2D to 768D, 5 subtests |
| `TestVectorIndexInterface` | Interface | Both indexes via interface |
| `TestEmbeddingCacheSetAndGet` | Embedding Cache | Basic cache hit |
| `TestEmbeddingCacheMiss` | Embedding Cache | Cache miss |
| `TestEmbeddingCacheLRU` | Embedding Cache | LRU eviction at capacity |
| `TestEmbeddingCacheDuplicate` | Embedding Cache | Dedup on Set |
| `TestEmbeddingCacheClear` | Embedding Cache | Full clear |
| `TestEmbeddingCacheConcurrency` | Embedding Cache | 20 goroutines × 50 ops |
| `TestCacheWarmingFromFile` | Warmup | Load 3 entries from JSONL |
| `TestCacheWarmingSkipsDuplicates` | Warmup | Dedup during warmup |
| `TestCacheWarmingInvalidFile` | Warmup | Graceful error handling |
| `TestPerModelEmbeddingConfig` | Per-Model | Model → embedding mapping |
| `TestCacheWithLSHIndex` | Integration | Cache with LSH index |
| `TestCacheWithFlatIndex` | Integration | Cache with flat index |
| `TestDefaultConfigNewFields` | Config | New defaults validated |

### Size-Variant Test Matrix

| Category | Sizes Tested | Result |
|----------|-------------|--------|
| Embedding dimensions | 1, 3, 10, 100, 768, 1536 | PASS |
| Serialization roundtrip | 1, 5, 20, 100, 768, 1536 elements | PASS |
| Cache capacity (eviction) | 1, 5, 10, 50, 100 entries | PASS |
| Prompt lengths | 0, 1, 50, 200, 500 chars + unicode/special | PASS |
| Entry counts (lookup) | 10, 50, 100, 200 entries | PASS |
| TTL durations | 1s (expired), 2s, 5s, 30s (alive) | PASS |
| Similarity thresholds | 0.50, 0.70, 0.85, 0.92, 0.99 | PASS |
| List entry limits | 1, 10, 25, 50, 100, 200, 500 | PASS |
| Concurrent loads | 10×10, 20×25, 50×10 goroutines | PASS |
| Similarity search counts | 5, 20, 100 stored vectors | PASS |
| LSH dimensions | 2, 10, 64, 256, 768 | PASS |

### Integration Test — 61 Requests

```
Total requests: 61
Hits: 31, Misses: 30
Hit rate: 50.82%
Entries in cache: 61
```

**Workload breakdown:**
- 2 models: `llama3`, `mistral`
- 30 unique prompts across domains: programming languages, networking, data structures, AI/ML, databases, frontend
- Each prompt repeated 1× (miss) then 1× (hit), with one prompt repeated 3×
- Verified exact key match (no Ollama dependency in tests)

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Exact key lookup | O(n) scan, SHA256 compare | Could be O(1) with map (future) |
| Similarity search (flat) | O(n × d) | n = entries, d = embedding dimension |
| Similarity search (LSH) | O(c × d) | c = candidates from bucket ≈ n/32 |
| Embedding cache lookup | O(1) | LRU map lookup |
| SQLite write | O(1) per entry | Async goroutine, WAL mode |
| Eviction | O(n) per eviction | Background loop every 60s |
| Memory | O(n × d × 4 bytes) | ~3MB for 1000 entries × 768 dims |
| Embedding cache memory | O(k × d × 4 bytes) | ~15MB for 5000 entries × 768 dims |

## File Index

| File | Lines | Purpose |
|------|-------|---------|
| `internal/semanticcache/semanticcache.go` | ~690 | Core cache engine, embedding cache, warmup |
| `internal/semanticcache/vectorindex.go` | ~200 | VectorIndex interface, FlatIndex, LSHIndex |
| `internal/semanticcache/semanticcache_test.go` | ~520 | 24 new tests for all features |
| `internal/proxy/proxy.go` | ~360 | Cache integration with streaming buffer |
| `internal/api/handlers.go` | — | Cache management API |
| `config/config.go` | ~90 | Config with per-model, index, warmup, stream fields |
| `main.go` | ~130 | Initialization and route wiring |
| `hive.yaml` | ~45 | Runtime configuration |
| `web/index.html` | — | Dashboard UI |

## Remaining Future Improvements

- Prometheus metrics export
- HNSW index (higher recall than LSH, higher build cost)
- Exact key lookup via map (O(1) instead of O(n) scan)
- Distributed cache with Redis backing store
