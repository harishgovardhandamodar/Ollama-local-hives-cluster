package proxy

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hive-cluster/hive-serving/internal/balancer"
	"github.com/hive-cluster/hive-serving/internal/cluster"
	"github.com/hive-cluster/hive-serving/internal/queue"
)

func newTestNode(id string) *cluster.Node {
	return &cluster.Node{
		ID: id, Name: id, Address: "127.0.0.1", Port: 0,
		Capacity: 5, ActiveConns: 0, CPUUsage: 10,
		MemoryUsed: 2, MemoryTotal: 16, VRAMUsed: 1, VRAMTotal: 8,
		Status: cluster.NodeOnline, LastHeartbeat: time.Now(),
	}
}

func newTestProxy() (*Proxy, *cluster.Manager, *balancer.Balancer, *queue.Queue, *queue.History) {
	cm := cluster.NewManager(10*time.Second, nil)
	node := newTestNode("test-node-1")
	cm.RegisterNode(node)

	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)

	cfg := ProxyConfig{
		MaxConcurrent:  10,
		RequestTimeout: 5 * time.Second,
		NodeID:         "proxy-1",
		OllamaAddr:     "http://127.0.0.1:11434",
	}

	p := New(cm, bal, q, hist, cfg, nil)
	return p, cm, bal, q, hist
}

type proxyWithServer struct {
	*Proxy
	backend *httptest.Server
}

func (p *proxyWithServer) close() {
	if p.backend != nil {
		p.backend.Close()
	}
}

func newProxyWithBackend() (*proxyWithServer, *cluster.Manager, *balancer.Balancer) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"done":true,"response":"hello"}`))
	}))

	cm := cluster.NewManager(10*time.Second, nil)
	n := newTestNode("test-node")
	n.Address = backend.Listener.Addr().(*net.TCPAddr).IP.String()
	n.Port = backend.Listener.Addr().(*net.TCPAddr).Port
	cm.RegisterNode(n)

	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)

	cfg := ProxyConfig{
		MaxConcurrent:  10,
		RequestTimeout: 5 * time.Second,
		NodeID:         "proxy-1",
		OllamaAddr:     backend.URL,
	}

	p := New(cm, bal, q, hist, cfg, nil)
	return &proxyWithServer{Proxy: p, backend: backend}, cm, bal
}

func TestNewProxy(t *testing.T) {
	p, _, _, _, _ := newTestProxy()
	if p == nil {
		t.Fatal("expected non-nil proxy")
	}
	if p.config.NodeID != "proxy-1" {
		t.Fatalf("expected NodeID proxy-1, got %s", p.config.NodeID)
	}
}

func TestHandleOllamaRouting(t *testing.T) {
	cm := cluster.NewManager(10*time.Second, nil)
	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)
	cfg := ProxyConfig{
		MaxConcurrent:  10,
		RequestTimeout: 1 * time.Second,
		OllamaAddr:     "http://192.0.2.1:1",
	}
	p := New(cm, bal, q, hist, cfg, nil)

	tests := []struct {
		path string
		want int
		desc string
	}{
		{"/api/generate", http.StatusServiceUnavailable, "inference -> 503 (no nodes)"},
		{"/api/chat", http.StatusServiceUnavailable, "inference -> 503 (no nodes)"},
		{"/api/tags", http.StatusBadGateway, "local -> 502 (unreachable)"},
		{"/api/pull", http.StatusBadGateway, "local -> 502 (unreachable)"},
		{"/api/ps", http.StatusBadGateway, "local -> 502 (unreachable)"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", tt.path, strings.NewReader(`{"model":"test"}`))
			p.HandleOllama(w, r)
			if w.Code != tt.want {
				t.Fatalf("expected status %d, got %d", tt.want, w.Code)
			}
		})
	}
}

func TestHandleSyncResponseSuccess(t *testing.T) {
	ps, _, _ := newProxyWithBackend()
	defer ps.close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/generate", strings.NewReader(`{"model":"llama2","stream":false}`))
	ps.HandleOllama(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d. Body: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("expected valid json response: %v", err)
	}
	if resp["done"] != true {
		t.Fatalf("expected done=true in response")
	}
}

func TestHandleSyncResponseNoNodes(t *testing.T) {
	cm := cluster.NewManager(10*time.Second, nil)
	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)
	cfg := ProxyConfig{MaxConcurrent: 10, RequestTimeout: 5 * time.Second}
	p := New(cm, bal, q, hist, cfg, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/generate", strings.NewReader(`{"model":"llama2","stream":false}`))
	p.HandleOllama(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when no nodes, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "no available nodes") {
		t.Fatalf("expected no available nodes error, got %s", w.Body.String())
	}
}

func TestHandleSyncResponseQueueFull(t *testing.T) {
	cm := cluster.NewManager(10*time.Second, nil)
	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(1)
	hist := queue.NewHistory(100)
	cfg := ProxyConfig{MaxConcurrent: 10, RequestTimeout: 5 * time.Second}
	p := New(cm, bal, q, hist, cfg, nil)

	q.Enqueue(&queue.Request{ID: "filler"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/generate", strings.NewReader(`{"model":"llama2","stream":false}`))
	p.HandleOllama(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when queue full, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "queue full") {
		t.Fatalf("expected queue full error, got %s", w.Body.String())
	}
}

func TestHandleSyncResponseUpstreamError(t *testing.T) {
	cm := cluster.NewManager(10*time.Second, nil)
	n := newTestNode("broken-node")
	n.Address = "192.0.2.1"
	n.Port = 1
	cm.RegisterNode(n)

	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)
	cfg := ProxyConfig{MaxConcurrent: 10, RequestTimeout: 1 * time.Second}
	p := New(cm, bal, q, hist, cfg, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/generate", strings.NewReader(`{"model":"llama2","stream":false}`))
	p.HandleOllama(w, r)

	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 on upstream error, got %d. Body: %s", w.Code, w.Body.String())
	}
}

func TestHandleStreamResponseNoNodes(t *testing.T) {
	cm := cluster.NewManager(10*time.Second, nil)
	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)
	cfg := ProxyConfig{MaxConcurrent: 10, RequestTimeout: 5 * time.Second}
	p := New(cm, bal, q, hist, cfg, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/chat", strings.NewReader(`{"model":"llama2","stream":true}`))
	p.HandleOllama(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when no nodes for stream, got %d", w.Code)
	}
}

func TestHandleStreamResponse(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"done":false,"response":"Hello"}` + "\n"))
		w.Write([]byte(`{"done":true,"response":" world"}` + "\n"))
	}))
	defer backend.Close()

	cm := cluster.NewManager(10*time.Second, nil)
	n := newTestNode("stream-node")
	n.Address = backend.Listener.Addr().(*net.TCPAddr).IP.String()
	n.Port = backend.Listener.Addr().(*net.TCPAddr).Port
	cm.RegisterNode(n)

	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)
	cfg := ProxyConfig{MaxConcurrent: 10, RequestTimeout: 5 * time.Second}
	p := New(cm, bal, q, hist, cfg, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/chat", strings.NewReader(`{"model":"llama2","stream":true}`))
	p.HandleOllama(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for stream, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "Hello") || !strings.Contains(body, "world") {
		t.Fatalf("expected streamed content in response, got: %s", body)
	}
}

func TestProxyLocalTags(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"models":[{"name":"llama2"}]}`))
	}))
	defer backend.Close()

	cm := cluster.NewManager(10*time.Second, nil)
	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)
	cfg := ProxyConfig{
		MaxConcurrent:  10,
		RequestTimeout: 5 * time.Second,
		OllamaAddr:     backend.URL,
	}
	p := New(cm, bal, q, hist, cfg, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/tags", nil)
	p.HandleOllama(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for local proxy, got %d", w.Code)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("expected valid json: %v", err)
	}
	models, ok := resp["models"].([]interface{})
	if !ok || len(models) != 1 {
		t.Fatalf("expected models array with 1 entry, got %+v", resp)
	}
}

func TestHandleCancel(t *testing.T) {
	cm := cluster.NewManager(10*time.Second, nil)
	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)
	cfg := ProxyConfig{MaxConcurrent: 10, RequestTimeout: 5 * time.Second}
	p := New(cm, bal, q, hist, cfg, nil)

	q.Enqueue(&queue.Request{ID: "req-1"})

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/cancel", strings.NewReader(`{"request_id":"req-1"}`))
	p.HandleCancel(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for cancel, got %d", w.Code)
	}
	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["status"] != "cancelled" {
		t.Fatalf("expected status cancelled, got %s", resp["status"])
	}
}

func TestHandleCancelNotFound(t *testing.T) {
	cm := cluster.NewManager(10*time.Second, nil)
	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)
	cfg := ProxyConfig{MaxConcurrent: 10, RequestTimeout: 5 * time.Second}
	p := New(cm, bal, q, hist, cfg, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/cancel", strings.NewReader(`{"request_id":"nonexistent"}`))
	p.HandleCancel(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for cancel not found, got %d", w.Code)
	}
}

func TestGetActiveRequests(t *testing.T) {
	p, _, _, _, _ := newTestProxy()
	active := p.GetActiveRequests()
	if len(active) != 0 {
		t.Fatalf("expected 0 active requests, got %d", len(active))
	}

	p.mu.Lock()
	p.active["test-1"] = &queue.Request{ID: "test-1"}
	p.active["test-2"] = &queue.Request{ID: "test-2"}
	p.mu.Unlock()

	active = p.GetActiveRequests()
	if len(active) != 2 {
		t.Fatalf("expected 2 active requests, got %d", len(active))
	}
}

func TestHandleOllamaBadJson(t *testing.T) {
	ps, _, _ := newProxyWithBackend()
	defer ps.close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/generate", strings.NewReader(`not json`))
	ps.HandleOllama(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for bad json, got %d", w.Code)
	}
}

func TestHandleInferencePriorityExtraction(t *testing.T) {
	ps, _, _ := newProxyWithBackend()
	defer ps.close()

	body := `{"model":"llama2","priority":2,"stream":false}`
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/generate", strings.NewReader(body))
	ps.HandleOllama(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestRequestLifecycle(t *testing.T) {
	ps, _, _ := newProxyWithBackend()
	defer ps.close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/generate", strings.NewReader(`{"model":"llama2","stream":false}`))
	ps.HandleOllama(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	active := ps.GetActiveRequests()
	if len(active) != 0 {
		t.Fatalf("expected active map to be empty after sync request, got %d items", len(active))
	}

	hist := ps.history.List(10)
	if len(hist) == 0 {
		t.Fatal("expected history to contain the completed request")
	}
	if hist[0].Status != queue.StatusComplete {
		t.Fatalf("expected completed status in history, got %s", hist[0].Status)
	}
}

type noFlushWriter struct {
	code   int
	header http.Header
}

func (w *noFlushWriter) Header() http.Header        { return w.header }
func (w *noFlushWriter) Write([]byte) (int, error)   { return 0, nil }
func (w *noFlushWriter) WriteHeader(code int)        { w.code = code }

func TestHandleStreamResponseNoFlusher(t *testing.T) {
	cm := cluster.NewManager(10*time.Second, nil)
	n := newTestNode("no-flusher-node")
	cm.RegisterNode(n)

	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)
	cfg := ProxyConfig{MaxConcurrent: 10, RequestTimeout: 5 * time.Second}
	p := New(cm, bal, q, hist, cfg, nil)

	w := &noFlushWriter{header: make(http.Header)}
	req := &queue.Request{ID: "stream-1", Model: "llama2"}
	p.handleStreamResponse(w, req, []byte(`{"model":"llama2"}`), "llama2", "/api/chat", "", nil)

	if w.code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when flusher not available, got %d", w.code)
	}
}

func TestHandleInferenceEmptyBody(t *testing.T) {
	p, _, _, _, _ := newTestProxy()
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/generate", strings.NewReader(``))
	p.HandleOllama(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for empty body, got %d", w.Code)
	}
}

func TestSyncResponseSetsHeaders(t *testing.T) {
	ps, _, _ := newProxyWithBackend()
	defer ps.close()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/generate", strings.NewReader(`{"model":"llama2","stream":false}`))
	ps.HandleOllama(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if w.Header().Get("X-Hive-Request-ID") == "" {
		t.Fatal("expected X-Hive-Request-ID header to be set")
	}
	if w.Header().Get("X-Hive-Node-ID") == "" {
		t.Fatal("expected X-Hive-Node-ID header to be set")
	}
}

func TestStreamResponseSetsHeaders(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"done":true,"response":"ok"}` + "\n"))
	}))
	defer backend.Close()

	cm := cluster.NewManager(10*time.Second, nil)
	n := newTestNode("stream-node")
	n.Address = backend.Listener.Addr().(*net.TCPAddr).IP.String()
	n.Port = backend.Listener.Addr().(*net.TCPAddr).Port
	cm.RegisterNode(n)

	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)
	cfg := ProxyConfig{MaxConcurrent: 10, RequestTimeout: 5 * time.Second}
	p := New(cm, bal, q, hist, cfg, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/chat", strings.NewReader(`{"model":"llama2","stream":true}`))
	p.HandleOllama(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for stream, got %d", w.Code)
	}
	if w.Header().Get("X-Hive-Request-ID") == "" {
		t.Fatal("expected X-Hive-Request-ID header to be set for stream")
	}
	if w.Header().Get("X-Hive-Node-ID") == "" {
		t.Fatal("expected X-Hive-Node-ID header to be set for stream")
	}
}

func TestProxyLocalError(t *testing.T) {
	cm := cluster.NewManager(10*time.Second, nil)
	bal := balancer.New(cm.GetHealthyNodes, balancer.StrategyLeastLoad)
	q := queue.New(100)
	hist := queue.NewHistory(100)
	cfg := ProxyConfig{
		MaxConcurrent:  10,
		RequestTimeout: 1 * time.Second,
		OllamaAddr:     "http://192.0.2.1:1",
	}
	p := New(cm, bal, q, hist, cfg, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/tags", nil)
	p.HandleOllama(w, r)

	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 for unreachable ollama, got %d", w.Code)
	}
}
