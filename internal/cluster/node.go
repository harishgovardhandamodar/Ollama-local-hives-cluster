package cluster

import (
	"encoding/json"
	"sync"
	"time"
)

type HardwareType string

const (
	HardwareAppleSilicon HardwareType = "apple-silicon"
	HardwareNvidiaGPU    HardwareType = "nvidia-gpu"
	HardwareNvidiaDGX    HardwareType = "nvidia-dgx"
	HardwareCPU          HardwareType = "cpu"
	HardwareUnknown      HardwareType = "unknown"
)

type NodeStatus string

const (
	NodeOnline  NodeStatus = "online"
	NodeOffline NodeStatus = "offline"
	NodeDrain   NodeStatus = "drain"
)

type Node struct {
	ID            string       `json:"id"`
	Name          string       `json:"name"`
	Address       string       `json:"address"`
	Port          int          `json:"port"`
	Hardware      HardwareType `json:"hardware"`
	GPUModel      string       `json:"gpu_model"`
	GPUMemory     int          `json:"gpu_memory_gb"`
	VRAMUsed      float64      `json:"vram_used_gb"`
	VRAMTotal     float64      `json:"vram_total_gb"`
	MemoryUsed    float64      `json:"memory_used_gb"`
	MemoryTotal   float64      `json:"memory_total_gb"`
	CPUUsage      float64      `json:"cpu_usage"`
	Capacity      int          `json:"capacity"`
	ActiveConns   int          `json:"active_connections"`
	QueuedReqs    int          `json:"queued_requests"`
	Models        []string     `json:"models"`
	Status        NodeStatus   `json:"status"`
	LastHeartbeat time.Time    `json:"last_heartbeat"`
	JoinedAt      time.Time    `json:"joined_at"`
	OllamaVersion string       `json:"ollama_version"`
	mu            sync.RWMutex `json:"-"`
}

func (n *Node) UpdateLoad(cpu float64, memUsed, memTotal, vramUsed, vramTotal float64, activeConns int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.CPUUsage = cpu
	n.MemoryUsed = memUsed
	n.MemoryTotal = memTotal
	n.VRAMUsed = vramUsed
	n.VRAMTotal = vramTotal
	n.ActiveConns = activeConns
	n.LastHeartbeat = time.Now()
}

func (n *Node) LoadScore() float64 {
	n.mu.RLock()
	defer n.mu.RUnlock()

	cpuScore := n.CPUUsage / 100.0
	memScore := 0.0
	if n.MemoryTotal > 0 {
		memScore = n.MemoryUsed / n.MemoryTotal
	}
	vramScore := 0.0
	if n.VRAMTotal > 0 {
		vramScore = n.VRAMUsed / n.VRAMTotal
	}
	connScore := 0.0
	if n.Capacity > 0 {
		connScore = float64(n.ActiveConns) / float64(n.Capacity)
	}

	return (cpuScore*0.2 + memScore*0.2 + vramScore*0.4 + connScore*0.2)
}

func (n *Node) AvailableSlots() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	slots := n.Capacity - n.ActiveConns
	if slots < 0 {
		return 0
	}
	return slots
}

func (n *Node) IsHealthy(timeout time.Duration) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Status == NodeOnline && time.Since(n.LastHeartbeat) < timeout
}

func (n *Node) GetActiveConns() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.ActiveConns
}

func (n *Node) GetModels() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]string, len(n.Models))
	copy(out, n.Models)
	return out
}

func (n *Node) SetModels(models []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Models = models
}

func (n *Node) MarshalJSON() ([]byte, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	type Alias Node
	return json.Marshal(&Alias{
		ID:            n.ID,
		Name:          n.Name,
		Address:       n.Address,
		Port:          n.Port,
		Hardware:      n.Hardware,
		GPUModel:      n.GPUModel,
		GPUMemory:     n.GPUMemory,
		VRAMUsed:      n.VRAMUsed,
		VRAMTotal:     n.VRAMTotal,
		MemoryUsed:    n.MemoryUsed,
		MemoryTotal:   n.MemoryTotal,
		CPUUsage:      n.CPUUsage,
		Capacity:      n.Capacity,
		ActiveConns:   n.ActiveConns,
		QueuedReqs:    n.QueuedReqs,
		Models:        n.Models,
		Status:        n.Status,
		LastHeartbeat: n.LastHeartbeat,
		JoinedAt:      n.JoinedAt,
		OllamaVersion: n.OllamaVersion,
	})
}
