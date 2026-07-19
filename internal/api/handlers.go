package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/hive-cluster/hive-serving/internal/balancer"
	"github.com/hive-cluster/hive-serving/internal/cluster"
	"github.com/hive-cluster/hive-serving/internal/proxy"
	"github.com/hive-cluster/hive-serving/internal/queue"
)

type Handlers struct {
	cluster  *cluster.Manager
	balancer *balancer.Balancer
	queue    *queue.Queue
	history  *queue.History
	proxy    *proxy.Proxy
}

func New(cluster *cluster.Manager, bal *balancer.Balancer, q *queue.Queue, history *queue.History, p *proxy.Proxy) *Handlers {
	return &Handlers{
		cluster:  cluster,
		balancer: bal,
		queue:    q,
		history:  history,
		proxy:    p,
	}
}

func (h *Handlers) HandleDashboard(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "web/index.html")
}

func (h *Handlers) HandleAPINodes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	nodes := h.cluster.GetNodes()
	json.NewEncoder(w).Encode(nodes)
}

func (h *Handlers) HandleAPIClusterStatus(w http.ResponseWriter, r *http.Request) {
	nodes := h.cluster.GetNodes()
	healthy := h.cluster.GetHealthyNodes()
	totalSlots := 0
	usedSlots := 0
	for _, n := range nodes {
		totalSlots += n.Capacity
		usedSlots += n.GetActiveConns()
	}

	historyStats := h.history.Stats()
	queueLen := h.queue.Len()
	activeReqs := h.proxy.GetActiveRequests()

	modelSet := make(map[string]bool)
	for _, n := range healthy {
		for _, m := range n.GetModels() {
			modelSet[m] = true
		}
	}
	models := make([]string, 0, len(modelSet))
	for m := range modelSet {
		models = append(models, m)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"cluster": map[string]interface{}{
			"total_nodes":     len(nodes),
			"online_nodes":    len(healthy),
			"total_slots":     totalSlots,
			"used_slots":      usedSlots,
			"queue_depth":     queueLen,
			"active_requests": len(activeReqs),
			"strategy":        h.balancer.GetStrategy(),
		},
		"models": models,
		"queue": map[string]interface{}{
			"depth":     queueLen,
			"max_size":  1000,
			"pending":   historyStats.Pending,
			"completed": historyStats.Completed,
			"failed":    historyStats.Failed,
		},
		"timestamp": time.Now(),
	})
}

func (h *Handlers) HandleAPIQueue(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.queue.List())
}

func (h *Handlers) HandleAPIHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.history.List(50))
}

func (h *Handlers) HandleAPIActive(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.proxy.GetActiveRequests())
}

func (h *Handlers) HandleAPIStrategy(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var payload struct {
			Strategy string `json:"strategy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		h.balancer.SetStrategy(balancer.Strategy(payload.Strategy))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"strategy": payload.Strategy})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"strategy": string(h.balancer.GetStrategy())})
}

func (h *Handlers) HandleAPICancel(w http.ResponseWriter, r *http.Request) {
	h.proxy.HandleCancel(w, r)
}

func (h *Handlers) HandleAPIModels(w http.ResponseWriter, r *http.Request) {
	healthy := h.cluster.GetHealthyNodes()
	modelNodes := make(map[string][]string)
	for _, n := range healthy {
		for _, m := range n.GetModels() {
			modelNodes[m] = append(modelNodes[m], n.Name)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(modelNodes)
}

func (h *Handlers) HandleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	nodes := h.cluster.GetNodes()
	healthy := h.cluster.GetHealthyNodes()
	totalSlots := 0
	usedSlots := 0
	for _, n := range nodes {
		totalSlots += n.Capacity
		usedSlots += n.GetActiveConns()
	}

	data := map[string]interface{}{
		"nodes":         nodes,
		"total_nodes":   len(nodes),
		"online_nodes":  len(healthy),
		"total_slots":   totalSlots,
		"used_slots":    usedSlots,
		"queue_depth":   h.queue.Len(),
		"active":        h.proxy.GetActiveRequests(),
		"history":       h.history.List(20),
		"strategy":      h.balancer.GetStrategy(),
		"timestamp":     time.Now(),
	}

	jsonBytes, _ := json.Marshal(data)
	fprintf, err := w.Write([]byte("data: " + string(jsonBytes) + "\n\n"))
	if err != nil || fprintf == 0 {
		return
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			nodes := h.cluster.GetNodes()
			healthy := h.cluster.GetHealthyNodes()
			totalSlots := 0
			usedSlots := 0
			for _, n := range nodes {
				totalSlots += n.Capacity
				usedSlots += n.GetActiveConns()
			}
			data := map[string]interface{}{
				"nodes":        nodes,
				"total_nodes":  len(nodes),
				"online_nodes": len(healthy),
				"total_slots":  totalSlots,
				"used_slots":   usedSlots,
				"queue_depth":  h.queue.Len(),
				"active":       h.proxy.GetActiveRequests(),
				"history":      h.history.List(20),
				"strategy":     h.balancer.GetStrategy(),
				"timestamp":    time.Now(),
			}
			jsonBytes, _ := json.Marshal(data)
			n, err := w.Write([]byte("data: " + string(jsonBytes) + "\n\n"))
			if err != nil || n == 0 {
				return
			}
			flusher.Flush()
		}
	}
}

func (h *Handlers) HandleAPICacheStats(w http.ResponseWriter, r *http.Request) {
	cache := h.proxy.GetSemanticCache()
	if cache == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"enabled": false,
			"message": "Semantic cache not enabled",
		})
		return
	}

	stats := cache.Stats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (h *Handlers) HandleAPICacheEntries(w http.ResponseWriter, r *http.Request) {
	cache := h.proxy.GetSemanticCache()
	if cache == nil {
		http.Error(w, `{"error":"semantic cache not enabled"}`, http.StatusServiceUnavailable)
		return
	}

	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		fmt.Sscanf(l, "%d", &limit)
	}

	entries := cache.ListEntries(limit)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"entries": entries,
		"count":   len(entries),
	})
}

func (h *Handlers) HandleAPICacheClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	cache := h.proxy.GetSemanticCache()
	if cache == nil {
		http.Error(w, `{"error":"semantic cache not enabled"}`, http.StatusServiceUnavailable)
		return
	}

	cache.Clear()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "cleared"})
}

func (h *Handlers) HandleAPICacheInvalidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	cache := h.proxy.GetSemanticCache()
	if cache == nil {
		http.Error(w, `{"error":"semantic cache not enabled"}`, http.StatusServiceUnavailable)
		return
	}

	var payload struct {
		EntryID string `json:"entry_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	if payload.EntryID == "" {
		http.Error(w, "entry_id required", http.StatusBadRequest)
		return
	}

	if cache.Invalidate(payload.EntryID) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "invalidated"})
	} else {
		http.Error(w, `{"error":"entry not found"}`, http.StatusNotFound)
	}
}
