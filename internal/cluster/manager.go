package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type Manager struct {
	nodes            map[string]*Node
	mu               sync.RWMutex
	heartbeatTimeout time.Duration
 onUpdate         func()
}

func NewManager(heartbeatTimeout time.Duration, onUpdate func()) *Manager {
	return &Manager{
		nodes:            make(map[string]*Node),
		heartbeatTimeout: heartbeatTimeout,
		onUpdate:         onUpdate,
	}
}

func (m *Manager) RegisterNode(node *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.nodes[node.ID]; ok {
		existing.Name = node.Name
		existing.Address = node.Address
		existing.Port = node.Port
		existing.Hardware = node.Hardware
		existing.GPUModel = node.GPUModel
		existing.GPUMemory = node.GPUMemory
		existing.Capacity = node.Capacity
		existing.Models = node.Models
		existing.Status = NodeOnline
		existing.LastHeartbeat = time.Now()
	} else {
		node.Status = NodeOnline
		node.LastHeartbeat = time.Now()
		node.JoinedAt = time.Now()
		m.nodes[node.ID] = node
	}
	log.Printf("[cluster] Node registered: %s (%s) at %s", node.Name, node.ID, node.Address)
	if m.onUpdate != nil {
		m.onUpdate()
	}
}

func (m *Manager) UpdateNodeHeartbeat(nodeID string, cpu float64, memUsed, memTotal, vramUsed, vramTotal float64, activeConns int) {
	m.mu.RLock()
	node, ok := m.nodes[nodeID]
	m.mu.RUnlock()
	if !ok {
		return
	}
	node.UpdateLoad(cpu, memUsed, memTotal, vramUsed, vramTotal, activeConns)
	if m.onUpdate != nil {
		m.onUpdate()
	}
}

func (m *Manager) DeregisterNode(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.nodes[nodeID]; ok {
		delete(m.nodes, nodeID)
		log.Printf("[cluster] Node deregistered: %s", nodeID)
		if m.onUpdate != nil {
			m.onUpdate()
		}
	}
}

func (m *Manager) GetNode(nodeID string) *Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nodes[nodeID]
}

func (m *Manager) GetNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := make([]*Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

func (m *Manager) GetHealthyNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := make([]*Node, 0)
	for _, n := range m.nodes {
		if n.IsHealthy(m.heartbeatTimeout) && n.Status == NodeOnline {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func (m *Manager) CheckStale() {
	ticker := time.NewTicker(m.heartbeatTimeout / 2)
	defer ticker.Stop()
	for range ticker.C {
		m.mu.Lock()
		for id, node := range m.nodes {
			if node.Status == NodeOnline && time.Since(node.LastHeartbeat) > m.heartbeatTimeout {
				node.Status = NodeOffline
				log.Printf("[cluster] Node marked offline: %s", id)
			}
		}
		m.mu.Unlock()
		if m.onUpdate != nil {
			m.onUpdate()
		}
	}
}

type RegistrationPayload struct {
	ID            string       `json:"id"`
	Name          string       `json:"name"`
	Address       string       `json:"address"`
	Port          int          `json:"port"`
	Hardware      HardwareType `json:"hardware"`
	GPUModel      string       `json:"gpu_model"`
	GPUMemory     int          `json:"gpu_memory_gb"`
	Capacity      int          `json:"capacity"`
	Models        []string     `json:"models"`
	OllamaVersion string       `json:"ollama_version"`
}

type HeartbeatPayload struct {
	NodeID       string  `json:"node_id"`
	CPUUsage     float64 `json:"cpu_usage"`
	MemoryUsed   float64 `json:"memory_used_gb"`
	MemoryTotal  float64 `json:"memory_total_gb"`
	VRAMUsed     float64 `json:"vram_used_gb"`
	VRAMTotal    float64 `json:"vram_total_gb"`
	ActiveConns  int     `json:"active_connections"`
	Models       []string `json:"models"`
}

func (m *Manager) HandleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var reg RegistrationPayload
	if err := json.NewDecoder(r.Body).Decode(&reg); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if reg.ID == "" || reg.Address == "" {
		http.Error(w, "id and address required", http.StatusBadRequest)
		return
	}
	node := &Node{
		ID:            reg.ID,
		Name:          reg.Name,
		Address:       reg.Address,
		Port:          reg.Port,
		Hardware:      reg.Hardware,
		GPUModel:      reg.GPUModel,
		GPUMemory:     reg.GPUMemory,
		Capacity:      reg.Capacity,
		Models:        reg.Models,
		OllamaVersion: reg.OllamaVersion,
	}
	m.RegisterNode(node)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "registered", "node_id": node.ID})
}

func (m *Manager) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var hb HeartbeatPayload
	if err := json.NewDecoder(r.Body).Decode(&hb); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	m.UpdateNodeHeartbeat(hb.NodeID, hb.CPUUsage, hb.MemoryUsed, hb.MemoryTotal, hb.VRAMUsed, hb.VRAMTotal, hb.ActiveConns)
	if hb.Models != nil {
		if node := m.GetNode(hb.NodeID); node != nil {
			node.mu.Lock()
			node.Models = hb.Models
			node.mu.Unlock()
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (m *Manager) HandleDeregister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var payload struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	m.DeregisterNode(payload.NodeID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "deregistered"})
}

func (m *Manager) HandleNodes(w http.ResponseWriter, r *http.Request) {
	nodes := m.GetNodes()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}

func (m *Manager) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       "ok",
		"total_nodes":  len(m.GetNodes()),
		"online_nodes": len(m.GetHealthyNodes()),
		"uptime":       time.Since(time.Now()).String(),
	})
}

func (m *Manager) StartHeartbeatChecker() {
	go m.CheckStale()
}

func (m *Manager) ProxyToNode(node *Node, w http.ResponseWriter, r *http.Request) error {
	proxyURL := fmt.Sprintf("http://%s:%d", node.Address, node.Port)
	req, err := http.NewRequest(r.Method, proxyURL+r.URL.Path, r.Body)
	if err != nil {
		return err
	}
	req.Header = r.Header.Clone()
	req.Header.Set("X-Forwarded-For", r.RemoteAddr)
	req.Header.Set("X-Hive-Node-ID", node.ID)

	client := &http.Client{Timeout: 300 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	return nil
}
