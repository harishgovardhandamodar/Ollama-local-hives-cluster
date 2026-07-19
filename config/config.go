package config

import (
	"flag"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	NodeID           string
	ListenAddr       string
	OllamaAddr       string
	HeartbeatTimeout time.Duration
	HeartbeatInterval time.Duration
	MaxQueueSize     int
	MaxConcurrent    int
	RequestTimeout   time.Duration
	NodeCapacity     int
	WebDir           string
	DatabasePath     string

	SemanticCacheEnabled             bool
	SemanticCacheMaxEntries          int
	SemanticCacheTTLSeconds          int
	SemanticCacheSimilarityThreshold float64
	SemanticCacheEmbeddingModel      string
	SemanticCacheModelEmbeddingMap   map[string]string
	SemanticCacheIndexType           string
	SemanticCacheEmbeddingCacheSize  int
	SemanticCacheWarmupFile          string
	StreamBufferMaxSize              int
	StreamBufferTimeoutSec          int
}

func Load() *Config {
	cfg := &Config{
		NodeID:           envOr("NODE_ID", ""),
		ListenAddr:       envOr("LISTEN_ADDR", ":8080"),
		OllamaAddr:       envOr("OLLAMA_ADDR", "http://127.0.0.1:11434"),
		HeartbeatTimeout: 10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		MaxQueueSize:     1000,
		MaxConcurrent:    10,
		RequestTimeout:   300 * time.Second,
		NodeCapacity:     5,
		WebDir:           envOr("WEB_DIR", "./web"),
		DatabasePath:     envOr("DATABASE_PATH", "./hive-server.db"),

		SemanticCacheEnabled:             envOr("SEMANTIC_CACHE_ENABLED", "false") == "true",
		SemanticCacheMaxEntries:          envIntOr("SEMANTIC_CACHE_MAX_ENTRIES", 1000),
		SemanticCacheTTLSeconds:          envIntOr("SEMANTIC_CACHE_TTL_SECONDS", 300),
		SemanticCacheSimilarityThreshold: 0.92,
		SemanticCacheEmbeddingModel:      envOr("SEMANTIC_CACHE_EMBEDDING_MODEL", "nomic-embed-text"),
		SemanticCacheModelEmbeddingMap:   parseModelEmbeddingMap(envOr("SEMANTIC_CACHE_MODEL_EMBEDDING_MAP", "")),
		SemanticCacheIndexType:           envOr("SEMANTIC_CACHE_INDEX_TYPE", "lsh"),
		SemanticCacheEmbeddingCacheSize:  envIntOr("SEMANTIC_CACHE_EMBEDDING_CACHE_SIZE", 5000),
		SemanticCacheWarmupFile:          envOr("SEMANTIC_CACHE_WARMUP_FILE", ""),
		StreamBufferMaxSize:              envIntOr("STREAM_BUFFER_MAX_SIZE", 1048576),
		StreamBufferTimeoutSec:          envIntOr("STREAM_BUFFER_TIMEOUT_SEC", 30),
	}

	flag.StringVar(&cfg.NodeID, "node-id", cfg.NodeID, "Unique node ID (auto-generated if empty)")
	flag.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "Address to listen on")
	flag.StringVar(&cfg.OllamaAddr, "ollama", cfg.OllamaAddr, "Local Ollama API address")
	flag.IntVar(&cfg.MaxQueueSize, "max-queue", cfg.MaxQueueSize, "Max queued requests")
	flag.IntVar(&cfg.MaxConcurrent, "max-concurrent", cfg.MaxConcurrent, "Max concurrent requests per node")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "Request timeout")
	flag.IntVar(&cfg.NodeCapacity, "capacity", cfg.NodeCapacity, "Node capacity (concurrent slot count)")
	flag.StringVar(&cfg.WebDir, "web-dir", cfg.WebDir, "Path to web static files")
	flag.StringVar(&cfg.DatabasePath, "db-path", cfg.DatabasePath, "Path to SQLite database")
	flag.Parse()

	if cfg.NodeID == "" {
		cfg.NodeID = generateNodeID()
	}

	return cfg
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func parseModelEmbeddingMap(s string) map[string]string {
	if s == "" {
		return make(map[string]string)
	}
	result := make(map[string]string)
	pairs := strings.Split(s, ",")
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) == 2 {
			result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return result
}
