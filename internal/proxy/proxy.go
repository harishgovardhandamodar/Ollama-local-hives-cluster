package proxy

import (
	"bufio"
	"bytes"
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
	"github.com/hive-cluster/hive-serving/internal/semanticcache"
)

type Proxy struct {
	cluster       *cluster.Manager
	balancer      *balancer.Balancer
	queue         *queue.Queue
	history       *queue.History
	config        ProxyConfig
	mu            sync.RWMutex
	active        map[string]*queue.Request
	semanticCache *semanticcache.SemanticCache
}

type ProxyConfig struct {
	MaxConcurrent          int
	RequestTimeout         time.Duration
	NodeID                 string
	OllamaAddr             string
	StreamBufferMaxSize    int
	StreamBufferTimeout    time.Duration
}

func New(cluster *cluster.Manager, bal *balancer.Balancer, q *queue.Queue, history *queue.History, cfg ProxyConfig, cache *semanticcache.SemanticCache) *Proxy {
	if cfg.StreamBufferMaxSize == 0 {
		cfg.StreamBufferMaxSize = 1024 * 1024
	}
	if cfg.StreamBufferTimeout == 0 {
		cfg.StreamBufferTimeout = 30 * time.Second
	}
	return &Proxy{
		cluster:       cluster,
		balancer:      bal,
		queue:         q,
		history:       history,
		config:        cfg,
		active:        make(map[string]*queue.Request),
		semanticCache: cache,
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

	prompt, _ := reqBody["prompt"].(string)
	var messages []map[string]string
	if msgs, ok := reqBody["messages"].([]interface{}); ok {
		for _, m := range msgs {
			if msgMap, ok := m.(map[string]interface{}); ok {
				messages = append(messages, map[string]string{
					"role":    fmt.Sprintf("%v", msgMap["role"]),
					"content": fmt.Sprintf("%v", msgMap["content"]),
				})
			}
		}
	}

	if p.semanticCache != nil {
		ctx := r.Context()
		if cached, found := p.semanticCache.Get(ctx, model, prompt, messages); found {
			log.Printf("[proxy] Cache hit for model=%s prompt=%s", model, truncateStr(prompt, 50))

			if stream {
				p.writeCachedStreamResponse(w, cached, model)
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Cache", "HIT")
				w.Header().Set("X-Cache-Entry-ID", cached.ID)

				response := map[string]interface{}{
					"response":  cached.Response,
					"model":     model,
					"cached":    true,
					"cache_id":  cached.ID,
					"hit_count": cached.HitCount,
				}
				json.NewEncoder(w).Encode(response)
			}
			return
		}
	}

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
		p.handleStreamResponse(w, req, body, model, apiPath, prompt, messages)
	} else {
		p.handleSyncResponse(w, req, body, model, apiPath, prompt, messages)
	}
}

func (p *Proxy) writeCachedStreamResponse(w http.ResponseWriter, cached *semanticcache.CacheEntry, model string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.Header().Set("X-Cache-Entry-ID", cached.ID)
		response := map[string]interface{}{
			"response":  cached.Response,
			"model":     model,
			"cached":    true,
			"cache_id":  cached.ID,
			"hit_count": cached.HitCount,
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Cache", "HIT")
	w.Header().Set("X-Cache-Entry-ID", cached.ID)

	lines := strings.Split(cached.Response, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var chunk map[string]interface{}
		if err := json.Unmarshal([]byte(line), &chunk); err == nil {
			chunk["cached"] = true
			chunk["cache_id"] = cached.ID
			data, _ := json.Marshal(chunk)
			w.Write(append(data, '\n'))
			flusher.Flush()
		}
	}
}

func (p *Proxy) handleSyncResponse(w http.ResponseWriter, req *queue.Request, body []byte, model string, apiPath string, prompt string, messages []map[string]string) {
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

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		req.Status = queue.StatusFailed
		req.Error = err.Error()
		http.Error(w, `{"error":"failed to read response"}`, http.StatusInternalServerError)
		return
	}

	if p.semanticCache != nil && resp.StatusCode == http.StatusOK {
		go func() {
			cacheCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			p.semanticCache.Set(cacheCtx, model, prompt, messages, string(responseBody), "")
		}()
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Hive-Request-ID", req.ID)
	w.Header().Set("X-Hive-Node-ID", node.ID)
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(resp.StatusCode)
	w.Write(responseBody)
	req.Status = queue.StatusComplete
}

func (p *Proxy) handleStreamResponse(w http.ResponseWriter, req *queue.Request, body []byte, model string, apiPath string, prompt string, messages []map[string]string) {
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
	w.Header().Set("X-Cache", "MISS")
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

	var streamBuffer bytes.Buffer
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			w.Write(buf[:n])
			flusher.Flush()

			if streamBuffer.Len() < p.config.StreamBufferMaxSize {
				streamBuffer.Write(buf[:n])
			}
		}
		if err != nil {
			break
		}
	}

	if p.semanticCache != nil && streamBuffer.Len() > 0 && streamBuffer.Len() < p.config.StreamBufferMaxSize {
		fullResponse := p.extractStreamResponse(&streamBuffer)
		if fullResponse != "" {
			go func() {
				cacheCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				p.semanticCache.Set(cacheCtx, model, prompt, messages, fullResponse, "")
			}()
		}
	}

	req.Status = queue.StatusComplete
}

func (p *Proxy) extractStreamResponse(buffer *bytes.Buffer) string {
	var response strings.Builder
	scanner := bufio.NewScanner(buffer)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var chunk map[string]interface{}
		if err := json.Unmarshal([]byte(line), &chunk); err != nil {
			continue
		}
		if resp, ok := chunk["response"].(string); ok {
			response.WriteString(resp)
		}
		if resp, ok := chunk["message"].(map[string]interface{}); ok {
			if content, ok := resp["content"].(string); ok {
				response.WriteString(content)
			}
		}
	}
	return response.String()
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

func (p *Proxy) GetSemanticCache() *semanticcache.SemanticCache {
	return p.semanticCache
}

func truncateStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func randInt() int {
	return int(time.Now().UnixNano() % 100000)
}
