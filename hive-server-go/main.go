package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	// Load config file if exists (env vars override file config)
	cfgFile, _ := LoadConfigFile("")

	// Apply config file defaults, then env overrides
	var ollamaURL, ollamaModel, apiKey string
	var serverPort, maxConcurrent, maxClients, readTimeout, writeTimeout, idleTimeout int
	var customProviders []string

	if cfgFile != nil {
		ollamaURL = firstNonEmpty(cfgFile.Server.OllamaURL, "http://localhost:11434")
		ollamaModel = firstNonEmpty(cfgFile.Server.OllamaModel, "llama3.1:8b")
		serverPort = firstNonZero(cfgFile.Server.Port, 8081)
		maxConcurrent = firstNonZero(cfgFile.Server.MaxConcurrent, 2)
		maxClients = firstNonZero(cfgFile.Server.MaxClients, 5)
		readTimeout = firstNonZero(cfgFile.Server.ReadTimeout, 30)
		writeTimeout = firstNonZero(cfgFile.Server.WriteTimeout, 600)
		idleTimeout = firstNonZero(cfgFile.Server.IdleTimeout, 120)
		apiKey = cfgFile.Server.APIKey
		customProviders = cfgFile.CustomProviders
	} else {
		ollamaURL = "http://localhost:11434"
		ollamaModel = "llama3.1:8b"
		serverPort = 8081
		maxConcurrent = 2
		maxClients = 5
		readTimeout = 30
		writeTimeout = 600
		idleTimeout = 120
	}

	// Environment variables override config file
	ollamaURL = envOrDefault("OLLAMA_BASE_URL", ollamaURL)
	ollamaModel = envOrDefault("OLLAMA_MODEL", ollamaModel)
	serverPort = envOrDefaultInt("SERVER_PORT", serverPort)
	maxConcurrent = envOrDefaultInt("MAX_CONCURRENT", maxConcurrent)
	maxClients = envOrDefaultInt("MAX_CLIENTS", maxClients)
	readTimeout = envOrDefaultInt("HTTP_READ_TIMEOUT", readTimeout)
	writeTimeout = envOrDefaultInt("HTTP_WRITE_TIMEOUT", writeTimeout)
	idleTimeout = envOrDefaultInt("HTTP_IDLE_TIMEOUT", idleTimeout)
	apiKey = envOrDefault("HIVE_API_KEY", apiKey)

	if v := os.Getenv("CUSTOM_PROVIDER_URLS"); v != "" {
		customProviders = splitAndTrim(v, ",")
	}

	meshEnabled := os.Getenv("MESH_ENABLED") != "false"

	// Initialize database with WAL mode
	initDB()

	// Initialize cache if enabled
	cacheEnabled := os.Getenv("HIVE_CACHE_ENABLED") == "true"
	var cache *ResponseCache
	if cacheEnabled {
		cacheEntries := envOrDefaultInt("HIVE_CACHE_MAX_ENTRIES", 1000)
		cacheTTL := envOrDefaultInt("HIVE_CACHE_TTL_SECONDS", 300)
		if cfgFile != nil {
			cacheEntries = firstNonZero(cfgFile.Cache.MaxEntries, cacheEntries)
			cacheTTL = firstNonZero(cfgFile.Cache.TTLSeconds, cacheTTL)
			cacheEnabled = cfgFile.Cache.Enabled || cacheEnabled
		}
		if cacheEnabled {
			cache = NewResponseCache(cacheEntries, cacheTTL)
			logInfo("Response cache enabled: %d entries, %ds TTL", cacheEntries, cacheTTL)
		}
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

	// Initialize audit trail manager
	auditManager := NewAuditTrailManager(defaultDB)
	globalAuditManager = auditManager
	defer auditManager.Stop()

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	// Add metrics endpoint
	mux.HandleFunc("GET /metrics", handleMetrics)

	// Add job streaming endpoint
	mux.HandleFunc("GET /api/jobs/stream", handleJobStream)

	// Add audit trail API endpoints
	mux.HandleFunc("GET /api/audit/recent", handleAuditRecent)
	mux.HandleFunc("GET /api/audit/search", handleAuditSearch)
	mux.HandleFunc("GET /api/audit/timeline/{request_id}", handleAuditTimeline)
	mux.HandleFunc("GET /api/audit/summary/{request_id}", handleAuditSummary)

	// Apply middleware stack: CORS -> Auth -> Request Logger -> Audit -> Handler
	handler := AuthMiddleware(apiKey, rl, requestLogger(corsMiddleware(auditMiddleware(auditManager, mux))))

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

func auditMiddleware(atm *AuditTrailManager, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip health check and static files from audit
		if r.URL.Path == "/api/health" || r.URL.Path == "/api/status" ||
			r.URL.Path == "/metrics" || r.URL.Path == "/api/peers" ||
			r.URL.Path == "/api/peers/health" || strings.HasPrefix(r.URL.Path, "/static/") {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()

		// Read request body for audit
		var bodyBytes []byte
		if r.Body != nil {
			bodyBytes, _ = io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		}

		// Log the request
		requestID := atm.LogRequest(r, bodyBytes, "")

		// Wrap response writer to capture response
		lw := &auditLogWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
			body:           &bytes.Buffer{},
		}

		next.ServeHTTP(lw, r)

		// Log the response
		durationMs := float64(time.Since(start).Milliseconds())
		atm.LogResponse(requestID, lw.statusCode, lw.body.Bytes(), durationMs)

		// Add request ID to response headers
		w.Header().Set("X-Request-ID", requestID)
	})
}

type auditLogWriter struct {
	http.ResponseWriter
	statusCode int
	body       *bytes.Buffer
}

func (lw *auditLogWriter) WriteHeader(code int) {
	lw.statusCode = code
	lw.ResponseWriter.WriteHeader(code)
}

func (lw *auditLogWriter) Write(b []byte) (int, error) {
	lw.body.Write(b)
	return lw.ResponseWriter.Write(b)
}

func (lw *auditLogWriter) Flush() {
	if f, ok := lw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
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
