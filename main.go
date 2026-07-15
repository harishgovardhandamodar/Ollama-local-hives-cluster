package main

import (
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
)

func main() {
	cfg := config.Load()

	log.Printf("=== Hive Cluster Node Starting ===")
	log.Printf("Node ID: %s", cfg.NodeID)
	log.Printf("Listen:  %s", cfg.ListenAddr)
	log.Printf("Ollama:  %s", cfg.OllamaAddr)

	clusterMgr := cluster.NewManager(cfg.HeartbeatTimeout, nil)
	bal := balancer.New(clusterMgr.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(cfg.MaxQueueSize)
	history := queue.NewHistory(500)
	proxyCfg := proxy.ProxyConfig{
		MaxConcurrent:  cfg.MaxConcurrent,
		RequestTimeout: cfg.RequestTimeout,
		NodeID:         cfg.NodeID,
		OllamaAddr:     cfg.OllamaAddr,
	}
	proxySvc := proxy.New(clusterMgr, bal, q, history, proxyCfg)
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
