package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	serverVersion    = "1.9.1"
	startTime        = time.Now()
	globalQueue      *OllamaQueue // global reference for streaming
	globalAuditManager *AuditTrailManager // global audit trail manager
)

type HiveServer struct {
	queue        *OllamaQueue
	mesh         *MeshDiscovery
	clients      *ClientManager
	cfg          ServerConfig
	provider     *ProviderManager
	codingAgent  *CodingAgentManager
}

type ClientManager struct {
	mu          sync.Mutex
	clients     map[string]*ClientInfo
	maxClients  int
}

type ClientInfo struct {
	ClientID      string  `json:"client_id"`
	Name          string  `json:"name"`
	ConnectedAt   float64 `json:"connected_at"`
	LastHeartbeat float64 `json:"last_heartbeat"`
	JobsSubmitted int     `json:"jobs_submitted"`
	JobsCompleted int     `json:"jobs_completed"`
}

type ServerConfig struct {
	OllamaURL          string
	OllamaModel        string
	ServerPort         int
	MaxConcurrent      int
	MeshEnabled        bool
	MaxClients         int
	CustomProviderURLs []string
	Cache              *ResponseCache
}

func NewClientManager(maxClients int) *ClientManager {
	return &ClientManager{
		clients:    make(map[string]*ClientInfo),
		maxClients: maxClients,
	}
}

func (cm *ClientManager) Register(clientID, name string) (*ClientInfo, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if len(cm.clients) >= cm.maxClients {
		if _, exists := cm.clients[clientID]; !exists {
			return nil, fmt.Errorf("max clients (%d) reached", cm.maxClients)
		}
	}
	if existing, ok := cm.clients[clientID]; ok {
		existing.LastHeartbeat = now()
		if name != "" {
			existing.Name = name
		}
		return existing, nil
	}
	client := &ClientInfo{
		ClientID:      clientID,
		Name:          name,
		ConnectedAt:   now(),
		LastHeartbeat: now(),
	}
	cm.clients[clientID] = client
	logInfo("Client registered: %s (%s)", clientID, name)
	return client, nil
}

func (cm *ClientManager) Unregister(clientID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.clients, clientID)
	logInfo("Client unregistered: %s", clientID)
}

func (cm *ClientManager) Heartbeat(clientID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if c, ok := cm.clients[clientID]; ok {
		c.LastHeartbeat = now()
	}
}

func (cm *ClientManager) GetAll() []*ClientInfo {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	result := make([]*ClientInfo, 0, len(cm.clients))
	for _, c := range cm.clients {
		result = append(result, c)
	}
	return result
}

func (cm *ClientManager) Count() int {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return len(cm.clients)
}

func (cm *ClientManager) IncrementSubmitted(clientID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if c, ok := cm.clients[clientID]; ok {
		c.JobsSubmitted++
	}
}

func (cm *ClientManager) IncrementCompleted(clientID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if c, ok := cm.clients[clientID]; ok {
		c.JobsCompleted++
	}
}

func NewHiveServer(cfg ServerConfig) *HiveServer {
	queue := NewOllamaQueue(cfg.MaxConcurrent, cfg.OllamaURL, cfg.OllamaModel)
	globalQueue = queue // Set global reference for streaming
	var mesh *MeshDiscovery
	if cfg.MeshEnabled {
		mesh = NewMeshDiscovery(
			getServerID(),
			cfg.ServerPort,
			getEnvInt("MESH_DISCOVERY_PORT", 8082),
			cfg.MaxConcurrent,
			cfg.OllamaModel,
		)
	}
	provider := NewProviderManager(
		getServerID(),
		cfg.ServerPort,
		cfg.OllamaURL,
		cfg.CustomProviderURLs,
	)

	var cam *CodingAgentManager
	if defaultDB != nil {
		cam = NewCodingAgentManager(defaultDB, queue, mesh, getServerID())
	}

	return &HiveServer{
		queue:       queue,
		mesh:        mesh,
		clients:     NewClientManager(cfg.MaxClients),
		cfg:         cfg,
		provider:    provider,
		codingAgent: cam,
	}
}

func (hs *HiveServer) Start() {
	hs.queue.Start()
	if hs.mesh != nil {
		hs.mesh.Start(
			hs.queue.GetAvailableCapacity,
			hs.queue.GetQueueStatus,
			hs.clients.Count,
		)
	}
	go hs.heartbeatCheckLoop()
	logInfo("Hive Server started on port %d", hs.cfg.ServerPort)
	logInfo("Max clients: %d", hs.cfg.MaxClients)
	logInfo("Ollama URL: %s", hs.cfg.OllamaURL)
	if hs.cfg.MeshEnabled {
		logInfo("Mesh discovery enabled")
	}
}

func (hs *HiveServer) Stop() {
	hs.queue.Stop()
	if hs.mesh != nil {
		hs.mesh.Stop()
	}
}

func (hs *HiveServer) heartbeatCheckLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := now() - 120
		hs.clients.mu.Lock()
		for id, c := range hs.clients.clients {
			if c.LastHeartbeat < cutoff {
				logWarn("Removing stale client: %s", id)
				delete(hs.clients.clients, id)
			}
		}
		hs.clients.mu.Unlock()
	}
}

func (hs *HiveServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", hs.handleRoot)
	mux.HandleFunc("/api/status", hs.handleStatus)
	mux.HandleFunc("/api/clients", hs.handleClients)
	mux.HandleFunc("POST /api/clients/register", hs.handleClientRegister)
	mux.HandleFunc("POST /api/clients/{client_id}/heartbeat", hs.handleClientHeartbeatPath)
	mux.HandleFunc("POST /api/clients/unregister", hs.handleClientUnregister)
	mux.HandleFunc("/api/jobs", hs.handleJobs)
	mux.HandleFunc("GET /api/jobs/{job_id}", hs.handleJobGet)
	mux.HandleFunc("POST /api/jobs/forward", hs.handleJobForward)
	mux.HandleFunc("/api/queue", hs.handleQueue)
	mux.HandleFunc("/api/peers", hs.handlePeers)
	mux.HandleFunc("POST /api/peers/register", hs.handlePeerRegister)
	mux.HandleFunc("POST /api/peers/introduce", hs.handlePeerIntroduce)
	mux.HandleFunc("POST /api/peers/scan", hs.handlePeerScan)
	mux.HandleFunc("/api/peers/diagnostics", hs.handleMeshDiagnostics)
	mux.HandleFunc("/api/ollama/health", hs.handleOllamaHealth)
	mux.HandleFunc("/api/nodes", hs.handleNodes)
	mux.HandleFunc("/api/providers", hs.handleProviders)
	mux.HandleFunc("/api/models", hs.handleModels)
	mux.HandleFunc("/api/logs", hs.handleLogs)
	mux.HandleFunc("POST /api/logs/clear", hs.handleLogsClear)
	mux.HandleFunc("/api/reports/usage", hs.handleUsageReport)
	mux.HandleFunc("/api/reports/usage/recent", hs.handleUsageRecent)
	mux.HandleFunc("/api/reports/usage/timeseries", hs.handleUsageTimeSeries)
	mux.HandleFunc("/api/reports/usage/histogram", hs.handleUsageHistogram)

	// Coding Agent API
	mux.HandleFunc("POST /api/agent/sessions", hs.handleAgentSessionCreate)
	mux.HandleFunc("GET /api/agent/sessions", hs.handleAgentSessionList)
	mux.HandleFunc("GET /api/agent/sessions/{session_id}", hs.handleAgentSessionGet)
	mux.HandleFunc("DELETE /api/agent/sessions/{session_id}", hs.handleAgentSessionDelete)
	mux.HandleFunc("POST /api/agent/sessions/{session_id}/archive", hs.handleAgentSessionArchive)
	mux.HandleFunc("POST /api/agent/sessions/{session_id}/messages", hs.handleAgentMessageSend)
	mux.HandleFunc("GET /api/agent/sessions/{session_id}/messages", hs.handleAgentMessagesGet)
	mux.HandleFunc("GET /api/agent/sessions/{session_id}/context", hs.handleAgentContextStats)
	mux.HandleFunc("GET /api/agent/sessions/{session_id}/audit", hs.handleAgentAuditLogs)
	mux.HandleFunc("GET /api/agent/search", hs.handleAgentSearch)
	mux.HandleFunc("GET /api/agent/models", hs.handleAgentModels)

	// OpenAI-compatible API (for Hermes, Codex, OpenCode, etc.)
	mux.HandleFunc("POST /v1/chat/completions", hs.handleOpenAIChatCompletions)
	mux.HandleFunc("GET /v1/models", hs.handleOpenAIListModels)
	mux.HandleFunc("GET /v1/health", hs.handleOpenAIHealth)

	// Model pull proxy (pull from peers)
	mux.HandleFunc("POST /api/models/pull-proxy", hs.handleModelPullProxy)
}

func (hs *HiveServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(DashboardHTML))
}

func (hs *HiveServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	peers := 0
	if hs.mesh != nil {
		peers = len(hs.mesh.GetAlivePeers())
	}
	writeJSON(w, map[string]interface{}{
		"server_id":    getServerID(),
		"version":      serverVersion,
		"uptime":       time.Since(startTime).String(),
		"ollama_url":   hs.cfg.OllamaURL,
		"ollama_model": hs.cfg.OllamaModel,
		"queue":        hs.queue.GetQueueStatus(),
		"clients":      hs.clients.Count(),
		"max_clients":  hs.cfg.MaxClients,
		"mesh_enabled": hs.cfg.MeshEnabled,
		"peers":        peers,
		"hardware":     getHardwareInfo(),
	})
}

func (hs *HiveServer) handleClients(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]interface{}{
		"clients": hs.clients.GetAll(),
	})
}

func (hs *HiveServer) handleClientRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		ClientID string `json:"client_id"`
		Name     string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if body.ClientID == "" || body.Name == "" {
		http.Error(w, "client_id and name required", http.StatusBadRequest)
		return
	}
	client, err := hs.clients.Register(body.ClientID, body.Name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusTooManyRequests)
		return
	}
	writeJSON(w, client)
}

func (hs *HiveServer) handleClientHeartbeatPath(w http.ResponseWriter, r *http.Request) {
	clientID := r.PathValue("client_id")
	if clientID == "" {
		var body struct {
			ClientID string `json:"client_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ClientID == "" {
			http.Error(w, "client_id required", http.StatusBadRequest)
			return
		}
		clientID = body.ClientID
	}
	hs.clients.Heartbeat(clientID)
	writeJSON(w, map[string]string{"status": "ok"})
}

func (hs *HiveServer) handleClientUnregister(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		ClientID string `json:"client_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	hs.clients.Unregister(body.ClientID)
	writeJSON(w, map[string]string{"status": "ok"})
}

func (hs *HiveServer) handleJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		writeJSON(w, map[string]interface{}{
			"jobs": hs.queue.GetAllJobs(),
		})
	case "POST":
		hs.submitJob(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (hs *HiveServer) submitJob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ClientID string                 `json:"client_id"`
		JobType  string                 `json:"job_type"`
		Payload  map[string]interface{} `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if body.ClientID == "" || body.JobType == "" {
		http.Error(w, "client_id and job_type required", http.StatusBadRequest)
		return
	}

	client, err := hs.clients.Register(body.ClientID, body.ClientID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusTooManyRequests)
		return
	}

	// Check cache for identical requests
	if hs.cfg.Cache != nil && body.JobType == "chat" {
		if msgs, ok := body.Payload["messages"].([]interface{}); ok && len(msgs) > 0 {
			var strMsgs []map[string]string
			for _, m := range msgs {
				if msgMap, ok := m.(map[string]interface{}); ok {
					strMsgs = append(strMsgs, map[string]string{
						"role":    fmt.Sprintf("%v", msgMap["role"]),
						"content": fmt.Sprintf("%v", msgMap["content"]),
					})
				}
			}
			if len(strMsgs) > 0 {
				model := fmt.Sprintf("%v", body.Payload["model"])
				cacheKey := hs.cfg.Cache.GenerateKey(model, strMsgs)
				if cached, found := hs.cfg.Cache.Get(cacheKey); found {
					globalMetrics.IncrMessagesCached()
					writeJSON(w, cached)
					return
				}
			}
		}
	}

	if hs.cfg.MeshEnabled && hs.mesh != nil {
		capacity := hs.queue.GetAvailableCapacity()
		if capacity <= 0 {
			peer := hs.mesh.GetBestPeer()
			if peer != nil {
				jobID := fmt.Sprintf("%s:%s:%d", body.ClientID, body.JobType, time.Now().UnixMilli())
				logInfo("Local queue full, forwarding job %s to peer %s", jobID, peer.ServerID)
				globalMetrics.IncrPeersForwarded()
				forwarded := forwardJobToPeer(peer, jobID, body.ClientID, body.JobType, body.Payload)
				if forwarded != nil {
					client.JobsSubmitted++
					globalMetrics.IncrJobsSubmitted()
					writeJSON(w, forwarded)
					return
				}
				logWarn("Forward to %s failed, executing locally", peer.ServerID)
			}
		}
	}

	jobID := fmt.Sprintf("%s:%s:%d", body.ClientID, body.JobType, time.Now().UnixMilli())
	job := NewJob(jobID, body.ClientID, body.JobType, body.Payload)
	hs.queue.Submit(job)
	client.JobsSubmitted++
	globalMetrics.IncrJobsSubmitted()
	logInfo("Job submitted: %s (type=%s, client=%s)", jobID, body.JobType, body.ClientID)
	writeJSON(w, job)
}

func (hs *HiveServer) handleJobGet(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")
	job := hs.queue.GetJob(jobID)
	if job == nil {
		http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
		return
	}
	writeJSON(w, jobToMap(job))
}

func (hs *HiveServer) handleJobForward(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		JobID    string                 `json:"job_id"`
		ClientID string                 `json:"client_id"`
		JobType  string                 `json:"job_type"`
		Payload  map[string]interface{} `json:"payload"`
		Origin   string                 `json:"origin"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if body.JobID == "" || body.ClientID == "" || body.JobType == "" {
		http.Error(w, "job_id, client_id, and job_type required", http.StatusBadRequest)
		return
	}

	peerJobID := fmt.Sprintf("mesh:%s:%s", body.Origin, body.JobID)
	job := NewJob(peerJobID, body.ClientID, body.JobType, body.Payload)
	hs.queue.Submit(job)

	deadline := time.After(600 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			writeJSON(w, map[string]interface{}{
				"job_id": body.JobID,
				"status": "failed",
				"error":  "forward timeout",
			})
			return
		case <-ticker.C:
			current := hs.queue.GetJob(peerJobID)
			if current != nil && current.Status != JobPending && current.Status != JobRunning {
				if current.Status == JobFailed {
					writeJSON(w, map[string]interface{}{
						"job_id": body.JobID,
						"status": "failed",
						"error":  current.Error,
					})
				} else {
					writeJSON(w, map[string]interface{}{
						"job_id": body.JobID,
						"status": "completed",
						"result": current.Result,
					})
				}
				return
			}
		}
	}
}

func (hs *HiveServer) handleQueue(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, hs.queue.GetQueueStatus())
}

func (hs *HiveServer) handlePeers(w http.ResponseWriter, r *http.Request) {
	if hs.mesh == nil {
		writeJSON(w, map[string]interface{}{
			"peers":       []interface{}{},
			"mesh_enabled": false,
		})
		return
	}
	writeJSON(w, map[string]interface{}{
		"peers":       hs.mesh.GetPeersList(),
		"mesh_enabled": true,
	})
}

func (hs *HiveServer) handlePeerRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if hs.mesh == nil {
		http.Error(w, "mesh not enabled", http.StatusBadRequest)
		return
	}
	var body struct {
		ServerID      string `json:"server_id"`
		Endpoint      string `json:"endpoint"`
		MaxConcurrent int    `json:"max_concurrent"`
		OllamaModel   string `json:"ollama_model"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if body.ServerID == "" || body.Endpoint == "" {
		http.Error(w, "server_id and endpoint required", http.StatusBadRequest)
		return
	}
	if body.MaxConcurrent == 0 {
		body.MaxConcurrent = 2
	}
	hs.mesh.RegisterPeer(body.ServerID, body.Endpoint, body.MaxConcurrent, body.OllamaModel)
	writeJSON(w, map[string]string{"status": "ok"})
}

func (hs *HiveServer) handlePeerIntroduce(w http.ResponseWriter, r *http.Request) {
	if hs.mesh == nil {
		http.Error(w, "mesh not enabled", http.StatusBadRequest)
		return
	}
	var body struct {
		ServerID      string `json:"server_id"`
		Endpoint      string `json:"endpoint"`
		MaxConcurrent int    `json:"max_concurrent"`
		OllamaModel   string `json:"ollama_model"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if body.ServerID == "" || body.Endpoint == "" {
		http.Error(w, "server_id and endpoint required", http.StatusBadRequest)
		return
	}
	if body.ServerID == getServerID() {
		writeJSON(w, map[string]string{"status": "skipped"})
		return
	}
	hs.mesh.RegisterPeer(body.ServerID, body.Endpoint, body.MaxConcurrent, body.OllamaModel)
	logInfo("Peer introduced self: %s at %s", body.ServerID, body.Endpoint)
	writeJSON(w, map[string]string{"status": "registered"})
}

func (hs *HiveServer) handlePeerScan(w http.ResponseWriter, r *http.Request) {
	if hs.mesh == nil {
		http.Error(w, "mesh not enabled", http.StatusBadRequest)
		return
	}
	seedPeers := getSeedPeers()
	hs.mesh.Rescan(seedPeers)
	writeJSON(w, map[string]interface{}{
		"status":     "ok",
		"seed_peers": seedPeers,
	})
}

func (hs *HiveServer) handleMeshDiagnostics(w http.ResponseWriter, r *http.Request) {
	if hs.mesh == nil {
		writeJSON(w, map[string]interface{}{
			"enabled": false,
		})
		return
	}
	diag := hs.mesh.GetDiagnostics()
	diag["server_id"] = getServerID()
	diag["mesh_enabled"] = true
	writeJSON(w, diag)
}

func (hs *HiveServer) handleOllamaHealth(w http.ResponseWriter, r *http.Request) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(hs.cfg.OllamaURL + "/api/tags")
	if err != nil {
		writeJSON(w, map[string]interface{}{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}
	defer resp.Body.Close()
	var result struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		writeJSON(w, map[string]interface{}{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}
	models := make([]string, len(result.Models))
	for i, m := range result.Models {
		models[i] = m.Name
	}
	writeJSON(w, map[string]interface{}{
		"status": "healthy",
		"models": models,
	})
}

func (hs *HiveServer) handleNodes(w http.ResponseWriter, r *http.Request) {
	peers := hs.getPeers()
	nodes := hs.provider.GetNodes(peers)
	writeJSON(w, map[string]interface{}{
		"nodes": nodes,
	})
}

func (hs *HiveServer) handleProviders(w http.ResponseWriter, r *http.Request) {
	providers := hs.provider.GetProviderTypes()
	writeJSON(w, map[string]interface{}{
		"providers": providers,
	})
}

func (hs *HiveServer) handleModels(w http.ResponseWriter, r *http.Request) {
	models := hs.provider.GetAggregatedModels()
	writeJSON(w, map[string]interface{}{
		"models": models,
	})
}

func (hs *HiveServer) getPeers() []*PeerInfo {
	if hs.mesh != nil {
		return hs.mesh.GetAlivePeers()
	}
	return nil
}

func (hs *HiveServer) handleLogs(w http.ResponseWriter, r *http.Request) {
	sinceStr := r.URL.Query().Get("since")
	since := 0.0
	if sinceStr != "" {
		fmt.Sscanf(sinceStr, "%f", &since)
	}
	logs := getLogs(since)
	writeJSON(w, map[string]interface{}{
		"logs": logs,
	})
}

func (hs *HiveServer) handleLogsClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	clearLogs()
	writeJSON(w, map[string]string{"status": "ok"})
}

func (hs *HiveServer) handleUsageTimeSeries(w http.ResponseWriter, r *http.Request) {
	if defaultDB == nil {
		writeJSON(w, map[string]interface{}{"points": []interface{}{}})
		return
	}
	model := r.URL.Query().Get("model")
	sinceStr := r.URL.Query().Get("since")
	since := now() - 3600
	if sinceStr != "" {
		var s float64
		fmt.Sscanf(sinceStr, "%f", &s)
		if s > 0 {
			since = s
		}
	}
	points, err := defaultDB.GetTimeSeries(model, since)
	if err != nil {
		writeJSON(w, map[string]interface{}{"error": err.Error()})
		return
	}
	models, _ := defaultDB.GetModels()
	writeJSON(w, map[string]interface{}{
		"points": points,
		"models": models,
	})
}

func (hs *HiveServer) handleUsageHistogram(w http.ResponseWriter, r *http.Request) {
	if defaultDB == nil {
		writeJSON(w, map[string]interface{}{"error": "no database"})
		return
	}
	model := r.URL.Query().Get("model")
	sinceStr := r.URL.Query().Get("since")
	since := now() - 3600
	if sinceStr != "" {
		var s float64
		fmt.Sscanf(sinceStr, "%f", &s)
		if s > 0 {
			since = s
		}
	}
	binsStr := r.URL.Query().Get("bins")
	numBins := 20
	if binsStr != "" {
		var b int
		fmt.Sscanf(binsStr, "%d", &b)
		if b > 0 && b <= 200 {
			numBins = b
		}
	}
	result, err := defaultDB.GetHistogram(model, since, numBins)
	if err != nil {
		writeJSON(w, map[string]interface{}{"error": err.Error()})
		return
	}
	writeJSON(w, map[string]interface{}{
		"histogram": result,
	})
}

func (hs *HiveServer) handleUsageReport(w http.ResponseWriter, r *http.Request) {
	if defaultDB == nil {
		writeJSON(w, map[string]interface{}{
			"error": "database not available",
		})
		return
	}
	summary, err := defaultDB.GetSummary()
	if err != nil {
		writeJSON(w, map[string]interface{}{"error": err.Error()})
		return
	}
	writeJSON(w, summary)
}

func (hs *HiveServer) handleUsageRecent(w http.ResponseWriter, r *http.Request) {
	if defaultDB == nil {
		writeJSON(w, map[string]interface{}{"records": []interface{}{}})
		return
	}
	records, err := defaultDB.GetRecent(100)
	if err != nil {
		writeJSON(w, map[string]interface{}{"error": err.Error()})
		return
	}
	writeJSON(w, map[string]interface{}{
		"records": records,
	})
}

func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func getServerID() string {
	hostname, _ := os.Hostname()
	id := os.Getenv("SERVER_ID")
	if id != "" {
		return id
	}
	return fmt.Sprintf("hive-%s", hostname)
}

func forwardJobToPeer(peer *PeerInfo, jobID, clientID, jobType string, payload map[string]interface{}) map[string]interface{} {
	client := &http.Client{Timeout: 600 * time.Second}
	body := map[string]interface{}{
		"job_id":    jobID,
		"client_id": clientID,
		"job_type":  jobType,
		"payload":   payload,
		"origin":    getServerID(),
	}
	data, _ := json.Marshal(body)
	resp, err := client.Post(peer.Endpoint+"/api/jobs/forward", "application/json", bytes.NewReader(data))
	if err != nil {
		logError("Forward to %s failed: %v", peer.ServerID, err)
		return nil
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil
	}
	return result
}

func getEnvInt(key string, def int) int {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(val, "%d", &n); err != nil {
		return def
	}
	return n
}

func splitAndTrim(s, sep string) []string {
	var result []string
	for _, part := range strings.Split(s, sep) {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

// Coding Agent API handlers

func (hs *HiveServer) handleAgentSessionCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if hs.codingAgent == nil {
		http.Error(w, `{"error":"coding agent not available (no database)"}`, http.StatusServiceUnavailable)
		return
	}
	var body struct {
		AgentType    string                 `json:"agent_type"`
		Model        string                 `json:"model"`
		SystemPrompt string                 `json:"system_prompt"`
		TokenBudget  int                    `json:"token_budget"`
		Metadata     map[string]interface{} `json:"metadata"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if body.AgentType == "" {
		body.AgentType = AgentTypeCustom
	}

	session, detected, err := hs.codingAgent.CreateSession(body.AgentType, body.Model, body.SystemPrompt, body.TokenBudget, body.Metadata)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"session":        session,
		"model_context":  detected,
	})
}

func (hs *HiveServer) handleAgentSessionList(w http.ResponseWriter, r *http.Request) {
	if hs.codingAgent == nil {
		http.Error(w, `{"error":"coding agent not available"}`, http.StatusServiceUnavailable)
		return
	}
	agentType := r.URL.Query().Get("agent_type")
	sessions, err := hs.codingAgent.ListSessions(agentType)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"sessions": sessions,
		"count":    len(sessions),
	})
}

func (hs *HiveServer) handleAgentSessionGet(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("session_id")
	if sessionID == "" {
		http.Error(w, "session_id required", http.StatusBadRequest)
		return
	}
	if hs.codingAgent == nil {
		http.Error(w, `{"error":"coding agent not available"}`, http.StatusServiceUnavailable)
		return
	}
	session, err := hs.codingAgent.GetSession(sessionID)
	if err != nil || session == nil {
		http.Error(w, `{"error":"session not found"}`, http.StatusNotFound)
		return
	}
	writeJSON(w, session)
}

func (hs *HiveServer) handleAgentSessionDelete(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("session_id")
	if sessionID == "" {
		http.Error(w, "session_id required", http.StatusBadRequest)
		return
	}
	if hs.codingAgent == nil {
		http.Error(w, `{"error":"coding agent not available"}`, http.StatusServiceUnavailable)
		return
	}
	if err := hs.codingAgent.DeleteSession(sessionID); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "deleted", "session_id": sessionID})
}

func (hs *HiveServer) handleAgentSessionArchive(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	sessionID := r.PathValue("session_id")
	if sessionID == "" {
		http.Error(w, "session_id required", http.StatusBadRequest)
		return
	}
	if hs.codingAgent == nil {
		http.Error(w, `{"error":"coding agent not available"}`, http.StatusServiceUnavailable)
		return
	}
	if err := hs.codingAgent.ArchiveSession(sessionID); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "archived", "session_id": sessionID})
}

func (hs *HiveServer) handleAgentMessageSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	sessionID := r.PathValue("session_id")
	if sessionID == "" {
		http.Error(w, "session_id required", http.StatusBadRequest)
		return
	}
	if hs.codingAgent == nil {
		http.Error(w, `{"error":"coding agent not available"}`, http.StatusServiceUnavailable)
		return
	}
	var body struct {
		Role     string                 `json:"role"`
		Content  string                 `json:"content"`
		Metadata map[string]interface{} `json:"metadata"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if body.Role == "" {
		body.Role = "user"
	}
	if body.Content == "" {
		http.Error(w, "content required", http.StatusBadRequest)
		return
	}

	response, session, err := hs.codingAgent.SendMessage(sessionID, body.Role, body.Content, body.Metadata)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"session_id": sessionID,
		"response":   response,
		"agent_type": session.AgentType,
		"model":      session.Model,
		"total_tokens": session.TotalTokens,
		"token_budget": session.TokenBudget,
		"compressions": session.Compressions,
	})
}

func (hs *HiveServer) handleAgentMessagesGet(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("session_id")
	if sessionID == "" {
		http.Error(w, "session_id required", http.StatusBadRequest)
		return
	}
	if hs.codingAgent == nil {
		http.Error(w, `{"error":"coding agent not available"}`, http.StatusServiceUnavailable)
		return
	}
	limit := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}
	msgs, err := hs.codingAgent.GetMessages(sessionID, limit)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"session_id": sessionID,
		"messages":   msgs,
		"count":      len(msgs),
	})
}

func (hs *HiveServer) handleAgentContextStats(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("session_id")
	if sessionID == "" {
		http.Error(w, "session_id required", http.StatusBadRequest)
		return
	}
	if hs.codingAgent == nil {
		http.Error(w, `{"error":"coding agent not available"}`, http.StatusServiceUnavailable)
		return
	}
	stats, err := hs.codingAgent.GetContextStats(sessionID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusNotFound)
		return
	}
	writeJSON(w, stats)
}

func (hs *HiveServer) handleAgentAuditLogs(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("session_id")
	if sessionID == "" {
		http.Error(w, "session_id required", http.StatusBadRequest)
		return
	}
	if hs.codingAgent == nil {
		http.Error(w, `{"error":"coding agent not available"}`, http.StatusServiceUnavailable)
		return
	}
	limit := 500
	if l := r.URL.Query().Get("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}
	logs, err := hs.codingAgent.GetAuditLogs(sessionID, limit)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"session_id": sessionID,
		"audit_logs": logs,
		"count":      len(logs),
	})
}

func (hs *HiveServer) handleAgentSearch(w http.ResponseWriter, r *http.Request) {
	if hs.codingAgent == nil {
		http.Error(w, `{"error":"coding agent not available"}`, http.StatusServiceUnavailable)
		return
	}
	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "q parameter required", http.StatusBadRequest)
		return
	}
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}
	msgs, err := hs.codingAgent.SearchMessages(query, limit)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"query":    query,
		"messages": msgs,
		"count":    len(msgs),
	})
}

func (hs *HiveServer) handleAgentModels(w http.ResponseWriter, r *http.Request) {
	if hs.codingAgent == nil {
		http.Error(w, `{"error":"coding agent not available"}`, http.StatusServiceUnavailable)
		return
	}

	// Get models from the registry
	registryModels := GetAllKnownModels()

	// Also try to query live models from Ollama
	var liveModels []map[string]interface{}
	if hs.cfg.OllamaURL != "" {
		client := &http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get(hs.cfg.OllamaURL + "/api/tags")
		if err == nil {
			defer resp.Body.Close()
			var result struct {
				Models []struct {
					Name string `json:"name"`
					Size int64  `json:"size"`
				} `json:"models"`
			}
			if json.NewDecoder(resp.Body).Decode(&result) == nil {
				for _, m := range result.Models {
					detected := hs.codingAgent.detector.DetectContextWindow(m.Name)
					budget := hs.codingAgent.detector.GetContextBudgetForModel(m.Name, 0)
					liveModels = append(liveModels, map[string]interface{}{
						"name":            m.Name,
						"context_length":  detected.ContextLength,
						"recommended_budget": budget,
						"detection_source":   detected.Source,
						"size_gb":            fmt.Sprintf("%.1f", float64(m.Size)/1e9),
					})
				}
			}
		}
	}

	writeJSON(w, map[string]interface{}{
		"live_models": liveModels,
		"registry":    registryModels,
	})
}

// handleModelPullProxy pulls a model from a peer's Ollama instance
func (hs *HiveServer) handleModelPullProxy(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		Model    string `json:"model"`
		PeerID   string `json:"peer_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if body.Model == "" {
		http.Error(w, "model required", http.StatusBadRequest)
		return
	}

	// Find peer with model or use specified peer
	var peer *PeerInfo
	if body.PeerID != "" && hs.mesh != nil {
		peers := hs.mesh.GetAlivePeers()
		for _, p := range peers {
			if p.ServerID == body.PeerID {
				peer = p
				break
			}
		}
	} else {
		peer = hs.findPeerWithModel(body.Model)
	}

	if peer == nil {
		http.Error(w, `{"error":"no peer found with model"}`, http.StatusNotFound)
		return
	}

	// Forward pull request to peer
	payload := map[string]interface{}{
		"name": body.Model,
	}
	data, _ := json.Marshal(payload)

	client := &http.Client{Timeout: 600 * time.Second}
	resp, err := client.Post(peer.Endpoint+"/api/models/pull", "application/json", bytes.NewReader(data))
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	var result interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	writeJSON(w, map[string]interface{}{
		"status":  "pulling",
		"model":   body.Model,
		"peer":    peer.ServerID,
		"result":  result,
	})
}

// Audit Trail API Handlers

func handleAuditRecent(w http.ResponseWriter, r *http.Request) {
	if globalAuditManager == nil {
		http.Error(w, `{"error":"audit trail not enabled"}`, http.StatusInternalServerError)
		return
	}

	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 1000 {
			limit = parsed
		}
	}

	category := r.URL.Query().Get("category")

	events, err := globalAuditManager.GetRecentEvents(limit, category)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"events": events,
		"count":  len(events),
	})
}

func handleAuditSearch(w http.ResponseWriter, r *http.Request) {
	if globalAuditManager == nil {
		http.Error(w, `{"error":"audit trail not enabled"}`, http.StatusInternalServerError)
		return
	}

	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, `{"error":"query parameter 'q' required"}`, http.StatusBadRequest)
		return
	}

	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 1000 {
			limit = parsed
		}
	}

	events, err := globalAuditManager.SearchEvents(query, limit)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]interface{}{
		"events": events,
		"count":  len(events),
		"query":  query,
	})
}

func handleAuditTimeline(w http.ResponseWriter, r *http.Request) {
	if globalAuditManager == nil {
		http.Error(w, `{"error":"audit trail not enabled"}`, http.StatusInternalServerError)
		return
	}

	requestID := r.PathValue("request_id")
	if requestID == "" {
		http.Error(w, `{"error":"request_id required"}`, http.StatusBadRequest)
		return
	}

	events, err := globalAuditManager.GetRequestTimeline(requestID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if len(events) == 0 {
		http.Error(w, `{"error":"no events found for request"}`, http.StatusNotFound)
		return
	}

	// Calculate total duration
	var totalDurationMs float64
	if len(events) > 1 {
		totalDurationMs = float64(events[len(events)-1].CreatedAt.Sub(events[0].CreatedAt).Milliseconds())
	}

	writeJSON(w, map[string]interface{}{
		"request_id":      requestID,
		"events":          events,
		"total_events":    len(events),
		"total_duration_ms": totalDurationMs,
	})
}

func handleAuditSummary(w http.ResponseWriter, r *http.Request) {
	if globalAuditManager == nil {
		http.Error(w, `{"error":"audit trail not enabled"}`, http.StatusInternalServerError)
		return
	}

	requestID := r.PathValue("request_id")
	if requestID == "" {
		http.Error(w, `{"error":"request_id required"}`, http.StatusBadRequest)
		return
	}

	summary, err := globalAuditManager.GetRequestSummary(requestID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if summary == nil {
		http.Error(w, `{"error":"no events found for request"}`, http.StatusNotFound)
		return
	}

	writeJSON(w, summary)
}
