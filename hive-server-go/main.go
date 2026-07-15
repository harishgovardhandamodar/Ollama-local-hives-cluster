package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	ollamaURL := envOr("OLLAMA_BASE_URL", "http://localhost:11434")
	ollamaModel := envOr("OLLAMA_MODEL", "llama3.1:8b")
	serverPort := getEnvInt("SERVER_PORT", 8081)
	maxConcurrent := getEnvInt("MAX_CONCURRENT", 2)
	meshEnabled := os.Getenv("MESH_ENABLED") != "false"
	maxClients := getEnvInt("MAX_CLIENTS", 5)

	cfg := ServerConfig{
		OllamaURL:     ollamaURL,
		OllamaModel:   ollamaModel,
		ServerPort:    serverPort,
		MaxConcurrent: maxConcurrent,
		MeshEnabled:   meshEnabled,
		MaxClients:    maxClients,
	}

	server := NewHiveServer(cfg)
	server.Start()
	defer server.Stop()

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	handler := corsMiddleware(mux)

	addr := fmt.Sprintf(":%d", serverPort)
	logInfo("Server listening on %s", addr)
	logInfo("Ollama URL: %s", ollamaURL)

	if err := http.ListenAndServe(addr, handler); err != nil {
		logError("Server error: %v", err)
		os.Exit(1)
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
