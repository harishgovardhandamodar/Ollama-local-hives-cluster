package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Load config file if exists (env vars override file config)
	cfgFile, _ := LoadConfigFile("")

	// Apply config file defaults, then env overrides
	ollamaURL := envOrDefault("OLLAMA_BASE_URL", firstNonEmpty(cfgFile.Server.OllamaURL, "http://localhost:11434"))
	ollamaModel := envOrDefault("OLLAMA_MODEL", firstNonEmpty(cfgFile.Server.OllamaModel, "llama3.1:8b"))
	serverPort := envOrDefaultInt("SERVER_PORT", firstNonZero(cfgFile.Server.Port, 8081))
	maxConcurrent := envOrDefaultInt("MAX_CONCURRENT", firstNonZero(cfgFile.Server.MaxConcurrent, 2))
	meshEnabled := os.Getenv("MESH_ENABLED") != "false"
	maxClients := envOrDefaultInt("MAX_CLIENTS", firstNonZero(cfgFile.Server.MaxClients, 5))
	readTimeout := envOrDefaultInt("HTTP_READ_TIMEOUT", firstNonZero(cfgFile.Server.ReadTimeout, 30))
	writeTimeout := envOrDefaultInt("HTTP_WRITE_TIMEOUT", firstNonZero(cfgFile.Server.WriteTimeout, 600))
	idleTimeout := envOrDefaultInt("HTTP_IDLE_TIMEOUT", firstNonZero(cfgFile.Server.IdleTimeout, 120))
	apiKey := envOrDefault("HIVE_API_KEY", cfgFile.Server.APIKey)

	var customProviders []string
	if len(cfgFile.CustomProviders) > 0 {
		customProviders = cfgFile.CustomProviders
	}
	if v := os.Getenv("CUSTOM_PROVIDER_URLS"); v != "" {
		customProviders = splitAndTrim(v, ",")
	}

	// Initialize database with WAL mode
	initDB()

	// Initialize cache if enabled
	cacheEnabled := os.Getenv("HIVE_CACHE_ENABLED") == "true" || cfgFile.Cache.Enabled
	var cache *ResponseCache
	if cacheEnabled {
		cacheEntries := envOrDefaultInt("HIVE_CACHE_MAX_ENTRIES", firstNonZero(cfgFile.Cache.MaxEntries, 1000))
		cacheTTL := envOrDefaultInt("HIVE_CACHE_TTL_SECONDS", firstNonZero(cfgFile.Cache.TTLSeconds, 300))
		cache = NewResponseCache(cacheEntries, cacheTTL)
		logInfo("Response cache enabled: %d entries, %ds TTL", cacheEntries, cacheTTL)
	}

	// Initialize rate limiter
	rateLimit := envOrDefaultInt("HIVE_RATE_LIMIT", 100)
	rl := NewRateLimiter(rateLimit, time.Minute)

	cfg := ServerConfig{
		OllamaURL:          ollamaURL,
		OllamaModel:        ollamaModel,
		ServerPort:         serverPort,
		MaxConcurrent:      maxConcurrent,
		MeshEnabled:        meshEnabled,
		MaxClients:         maxClients,
		CustomProviderURLs: customProviders,
		Cache:              cache,
	}

	server := NewHiveServer(cfg)
	server.Start()
	defer server.Stop()

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	// Add metrics endpoint
	mux.HandleFunc("/metrics", handleMetrics)

	// Add job streaming endpoint
	mux.HandleFunc("/api/jobs/stream", handleJobStream)

	// Apply auth middleware
	handler := AuthMiddleware(apiKey, rl, requestLogger(corsMiddleware(mux)))

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", serverPort),
		Handler:      handler,
		ReadTimeout:  time.Duration(readTimeout) * time.Second,
		WriteTimeout: time.Duration(writeTimeout) * time.Second,
		IdleTimeout:  time.Duration(idleTimeout) * time.Second,
	}

	// Graceful shutdown with job queue drain
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logInfo("Received shutdown signal, draining job queue...")

		// Give in-flight jobs up to 30 seconds to complete
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Stop accepting new jobs
		server.Stop()

		// Wait for HTTP server to finish in-flight requests
		if err := httpServer.Shutdown(ctx); err != nil {
			logError("HTTP server shutdown error: %v", err)
		}

		closeDB()
		logInfo("Server stopped gracefully")
		os.Exit(0)
	}()

	logInfo("Server listening on %s", httpServer.Addr)
	logInfo("Ollama URL: %s", ollamaURL)
	if apiKey != "" {
		logInfo("API key authentication enabled")
	}

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logError("Server error: %v", err)
		os.Exit(1)
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func firstNonZero(vals ...int) int {
	for _, v := range vals {
		if v != 0 {
			return v
		}
	}
	return 0
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		lw := &logResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(lw, r)
		dur := time.Since(start)
		if dur > 100*time.Millisecond {
			logInfo("%s %s %d %v", r.Method, r.URL.Path, lw.statusCode, dur)
		}
	})
}

type logResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lw *logResponseWriter) WriteHeader(code int) {
	lw.statusCode = code
	lw.ResponseWriter.WriteHeader(code)
}

func (lw *logResponseWriter) Flush() {
	if f, ok := lw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
