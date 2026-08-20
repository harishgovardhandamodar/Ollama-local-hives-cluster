package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/hive-cluster/hive-serving/config"
	"github.com/hive-cluster/hive-serving/internal/api"
	"github.com/hive-cluster/hive-serving/internal/balancer"
	"github.com/hive-cluster/hive-serving/internal/cluster"
	"github.com/hive-cluster/hive-serving/internal/proxy"
	"github.com/hive-cluster/hive-serving/internal/queue"
	"github.com/hive-cluster/hive-serving/memory"
)

func main() {
	cfg := config.Load()

	log.Printf("=== Hive Cluster Node Starting ===")
	log.Printf("Node ID: %s", cfg.NodeID)
	log.Printf("Listen:  %s", cfg.ListenAddr)
	log.Printf("Ollama:  %s", cfg.OllamaAddr)
	log.Printf("Memory:  %s", cfg.LoneWolfURL)

	clusterMgr := cluster.NewManager(cfg.HeartbeatTimeout, nil)
	bal := balancer.New(clusterMgr.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(cfg.MaxQueueSize)
	history := queue.NewHistory(500)
	mem := memory.NewClient(cfg.LoneWolfURL)
	proxyCfg := proxy.ProxyConfig{
		MaxConcurrent:  cfg.MaxConcurrent,
		RequestTimeout: cfg.RequestTimeout,
		NodeID:         cfg.NodeID,
		OllamaAddr:     cfg.OllamaAddr,
		LoneWolfURL:    cfg.LoneWolfURL,
	}
	proxySvc := proxy.New(clusterMgr, bal, q, history, mem, proxyCfg)
	handlers := api.New(clusterMgr, bal, q, history, proxySvc)

	clusterMgr.StartHeartbeatChecker()

	mux := http.NewServeMux()

	mux.HandleFunc("/", handlers.HandleDashboard)
	mux.HandleFunc("/api/sse", handlers.HandleSSE)
	mux.HandleFunc("/api/status", handlers.HandleAPIClusterStatus)
	mux.HandleFunc("/api/nodes", handlers.HandleAPINodes)
	mux.HandleFunc("/api/queue", handlers.HandleAPIQueue)
	mux.HandleFunc("/api/history", handlers.HandleAPIHistory)
	mux.HandleFunc("/api/active", handlers.HandleAPIActive)
	mux.HandleFunc("/api/strategy", handlers.HandleAPIStrategy)
	mux.HandleFunc("/api/cancel", handlers.HandleAPICancel)
	mux.HandleFunc("/api/models", handlers.HandleAPIModels)

	mux.HandleFunc("/register", clusterMgr.HandleRegister)
	mux.HandleFunc("/heartbeat", clusterMgr.HandleHeartbeat)
	mux.HandleFunc("/deregister", clusterMgr.HandleDeregister)

	mux.HandleFunc("/api/generate", proxySvc.HandleOllama)
	mux.HandleFunc("/api/chat", proxySvc.HandleOllama)
	mux.HandleFunc("/api/tags", proxySvc.HandleOllama)
	mux.HandleFunc("/api/pull", proxySvc.HandleOllama)
	mux.HandleFunc("/api/ps", proxySvc.HandleOllama)

	mux.HandleFunc("/api/memory/health", func(w http.ResponseWriter, r *http.Request) {
		health, err := mem.Health()
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "unreachable", "error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(health)
	})
	mux.HandleFunc("/api/memory/recall", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req memory.RecallRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		results, err := mem.Recall(req)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(results)
	})
	mux.HandleFunc("/api/memory/remember", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req memory.RememberRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		result, err := mem.Remember(req)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("/api/memory/forget", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req memory.ForgetRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		removed, err := mem.Forget(req)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]int{"removed": removed})
	})

	corsMiddleware := func(next http.Handler) http.Handler {
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

	server := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: corsMiddleware(mux),
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		server.Close()
	}()

	fmt.Printf("\n  Hive Cluster Dashboard: http://localhost%s\n\n", cfg.ListenAddr)

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
