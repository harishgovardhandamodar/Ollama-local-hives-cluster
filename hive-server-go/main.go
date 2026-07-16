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
	ollamaURL := envOr("OLLAMA_BASE_URL", "http://localhost:11434")
	ollamaModel := envOr("OLLAMA_MODEL", "llama3.1:8b")
	serverPort := getEnvInt("SERVER_PORT", 8081)
	maxConcurrent := getEnvInt("MAX_CONCURRENT", 2)
	meshEnabled := os.Getenv("MESH_ENABLED") != "false"
	maxClients := getEnvInt("MAX_CLIENTS", 5)
	readTimeout := getEnvInt("HTTP_READ_TIMEOUT", 30)
	writeTimeout := getEnvInt("HTTP_WRITE_TIMEOUT", 600)
	idleTimeout := getEnvInt("HTTP_IDLE_TIMEOUT", 120)
	customProviders := splitAndTrim(os.Getenv("CUSTOM_PROVIDER_URLS"), ",")

	initDB()

	cfg := ServerConfig{
		OllamaURL:          ollamaURL,
		OllamaModel:        ollamaModel,
		ServerPort:         serverPort,
		MaxConcurrent:      maxConcurrent,
		MeshEnabled:        meshEnabled,
		MaxClients:         maxClients,
		CustomProviderURLs: customProviders,
	}

	server := NewHiveServer(cfg)
	server.Start()
	defer server.Stop()

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	handler := requestLogger(corsMiddleware(mux))

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", serverPort),
		Handler:      handler,
		ReadTimeout:  time.Duration(readTimeout) * time.Second,
		WriteTimeout: time.Duration(writeTimeout) * time.Second,
		IdleTimeout:  time.Duration(idleTimeout) * time.Second,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		logInfo("Received signal %v, shutting down...", sig)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		server.Stop()
		if err := httpServer.Shutdown(ctx); err != nil {
			logError("HTTP server shutdown error: %v", err)
		}
		closeDB()
		logInfo("Server stopped")
		os.Exit(0)
	}()

	logInfo("Server listening on %s", httpServer.Addr)
	logInfo("Ollama URL: %s", ollamaURL)

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logError("Server error: %v", err)
		os.Exit(1)
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
