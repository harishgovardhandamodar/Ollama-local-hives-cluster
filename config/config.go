package config

import (
	"flag"
	"os"
	"strconv"
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
	}

	flag.StringVar(&cfg.NodeID, "node-id", cfg.NodeID, "Unique node ID (auto-generated if empty)")
	flag.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "Address to listen on")
	flag.StringVar(&cfg.OllamaAddr, "ollama", cfg.OllamaAddr, "Local Ollama API address")
	flag.IntVar(&cfg.MaxQueueSize, "max-queue", cfg.MaxQueueSize, "Max queued requests")
	flag.IntVar(&cfg.MaxConcurrent, "max-concurrent", cfg.MaxConcurrent, "Max concurrent requests per node")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "Request timeout")
	flag.IntVar(&cfg.NodeCapacity, "capacity", cfg.NodeCapacity, "Node capacity (concurrent slot count)")
	flag.StringVar(&cfg.WebDir, "web-dir", cfg.WebDir, "Path to web static files")
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
