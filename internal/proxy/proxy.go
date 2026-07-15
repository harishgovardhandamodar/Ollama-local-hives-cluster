package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hive-cluster/hive-serving/internal/balancer"
	"github.com/hive-cluster/hive-serving/internal/cluster"
	"github.com/hive-cluster/hive-serving/internal/queue"
)

type Proxy struct {
	cluster  *cluster.Manager
	balancer *balancer.Balancer
	queue    *queue.Queue
	history  *queue.History
	config   ProxyConfig
	mu       sync.RWMutex
	active   map[string]*queue.Request
}

type ProxyConfig struct {
	MaxConcurrent int
	RequestTimeout time.Duration
	NodeID         string
	OllamaAddr     string
}

func New(cluster *cluster.Manager, bal *balancer.Balancer, q *queue.Queue, history *queue.History, cfg ProxyConfig) *Proxy {
	return &Proxy{
		cluster:  cluster,
		balancer: bal,
		queue:    q,
		history:  history,
		config:   cfg,
		active:   make(map[string]*queue.Request),
	}
}

func (p *Proxy) HandleOllama(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	if strings.HasPrefix(path, "/api/generate") || strings.HasPrefix(path, "/api/chat") {
		p.handleInference(w, r, path)
		return
	}

	p.proxyLocal(w, r)
}

func (p *Proxy) handleInference(w http.ResponseWriter, r *http.Request, apiPath string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var reqBody map[string]interface{}
	if err := json.Unmarshal(body, &reqBody); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	model, _ := reqBody["model"].(string)
	stream, _ := reqBody["stream"].(bool)
	priority := queue.PriorityNormal
	if pri, ok := reqBody["priority"].(float64); ok {
		priority = queue.Priority(int(pri))
	}

	reqID := fmt.Sprintf("%d-%d", time.Now().UnixNano(), randInt())
	req := &queue.Request{
		ID:       reqID,
		Model:    model,
		Priority: priority,
		Metadata: string(body),
	}

	if !p.queue.Enqueue(req) {
		http.Error(w, `{"error":"queue full"}`, http.StatusServiceUnavailable)
		return
	}

	log.Printf("[proxy] Request %s queued: model=%s priority=%d path=%s", reqID, model, priority, apiPath)

	if stream {
		p.handleStreamResponse(w, req, body, model, apiPath)
	} else {
		p.handleSyncResponse(w, req, body, model, apiPath)
	}
}

func (p *Proxy) handleSyncResponse(w http.ResponseWriter, req *queue.Request, body []byte, model string, apiPath string) {
	node := p.balancer.SelectNode(model)
	if node == nil {
		req.Status = queue.StatusFailed
		req.Error = "no available nodes"
		p.history.Add(req)
		http.Error(w, `{"error":"no available nodes in cluster"}`, http.StatusServiceUnavailable)
		return
	}

	req.Status = queue.StatusRunning
	req.NodeID = node.ID
	now := time.Now()
	req.StartedAt = &now

	p.mu.Lock()
	p.active[req.ID] = req
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.active, req.ID)
		p.mu.Unlock()
		completed := time.Now()
		req.CompletedAt = &completed
		p.history.Add(req)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), p.config.RequestTimeout)
	defer cancel()

	proxyURL := fmt.Sprintf("http://%s:%d%s", node.Address, node.Port, apiPath)
	proxyReq, err := http.NewRequestWithContext(ctx, "POST", proxyURL, strings.NewReader(string(body)))
	if err != nil {
		req.Status = queue.StatusFailed
		req.Error = err.Error()
		http.Error(w, `{"error":"proxy error"}`, http.StatusInternalServerError)
		return
	}
	proxyReq.Header.Set("Content-Type", "application/json")
	proxyReq.Header.Set("X-Hive-Request-ID", req.ID)

	client := &http.Client{Timeout: p.config.RequestTimeout}
	resp, err := client.Do(proxyReq)
	if err != nil {
		req.Status = queue.StatusFailed
		req.Error = err.Error()
		http.Error(w, fmt.Sprintf(`{"error":"upstream error: %s"}`, err.Error()), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Hive-Request-ID", req.ID)
	w.Header().Set("X-Hive-Node-ID", node.ID)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	req.Status = queue.StatusComplete
}

func (p *Proxy) handleStreamResponse(w http.ResponseWriter, req *queue.Request, body []byte, model string, apiPath string) {
	node := p.balancer.SelectNode(model)
	if node == nil {
		req.Status = queue.StatusFailed
		req.Error = "no available nodes"
		p.history.Add(req)
		http.Error(w, `{"error":"no available nodes in cluster"}`, http.StatusServiceUnavailable)
		return
	}

	req.Status = queue.StatusRunning
	req.NodeID = node.ID
	now := time.Now()
	req.StartedAt = &now

	p.mu.Lock()
	p.active[req.ID] = req
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.active, req.ID)
		p.mu.Unlock()
		completed := time.Now()
		req.CompletedAt = &completed
		p.history.Add(req)
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Hive-Request-ID", req.ID)
	w.Header().Set("X-Hive-Node-ID", node.ID)
	w.WriteHeader(http.StatusOK)

	proxyURL := fmt.Sprintf("http://%s:%d%s", node.Address, node.Port, apiPath)
	proxyReq, err := http.NewRequest("POST", proxyURL, strings.NewReader(string(body)))
	if err != nil {
		req.Status = queue.StatusFailed
		req.Error = err.Error()
		return
	}
	proxyReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: p.config.RequestTimeout}
	resp, err := client.Do(proxyReq)
	if err != nil {
		req.Status = queue.StatusFailed
		req.Error = err.Error()
		return
	}
	defer resp.Body.Close()

	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			w.Write(buf[:n])
			flusher.Flush()
		}
		if err != nil {
			break
		}
	}
	req.Status = queue.StatusComplete
}

func (p *Proxy) proxyLocal(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, r.Method, p.config.OllamaAddr+r.URL.Path, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header = r.Header.Clone()

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (p *Proxy) HandleCancel(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		RequestID string `json:"request_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if p.queue.Cancel(payload.RequestID) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "cancelled"})
	} else {
		http.Error(w, `{"error":"request not found or already running"}`, http.StatusNotFound)
	}
}

func (p *Proxy) GetActiveRequests() []*queue.Request {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]*queue.Request, 0, len(p.active))
	for _, req := range p.active {
		result = append(result, req)
	}
	return result
}

func randInt() int {
	return int(time.Now().UnixNano() % 100000)
}
