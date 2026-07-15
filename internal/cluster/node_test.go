package cluster

import (
	"testing"
	"time"
)

func newNode(id string) *Node {
	return &Node{
		ID:       id,
		Capacity: 10,
		Status:   NodeOnline,
	}
}

func TestUpdateLoad(t *testing.T) {
	n := newNode("test-node")
	before := time.Now()
	n.UpdateLoad(50.0, 8.0, 16.0, 4.0, 8.0, 5)
	after := time.Now()

	if n.CPUUsage != 50.0 {
		t.Fatalf("expected cpu 50.0, got %f", n.CPUUsage)
	}
	if n.MemoryUsed != 8.0 {
		t.Fatalf("expected mem used 8.0, got %f", n.MemoryUsed)
	}
	if n.ActiveConns != 5 {
		t.Fatalf("expected active conns 5, got %d", n.ActiveConns)
	}
	if n.LastHeartbeat.Before(before) || n.LastHeartbeat.After(after) {
		t.Fatal("last heartbeat should be updated to current time")
	}
}

func TestLoadScore(t *testing.T) {
	tests := []struct {
		name     string
		node     *Node
		expected float64
	}{
		{
			name: "idle node",
			node: &Node{
				Capacity: 10, CPUUsage: 0, MemoryUsed: 0, MemoryTotal: 16,
				VRAMUsed: 0, VRAMTotal: 8, ActiveConns: 0,
			},
			expected: 0.0,
		},
		{
			name: "fully loaded",
			node: &Node{
				Capacity: 10, CPUUsage: 100, MemoryUsed: 16, MemoryTotal: 16,
				VRAMUsed: 8, VRAMTotal: 8, ActiveConns: 10,
			},
			expected: 1.0,
		},
		{
			name: "half loaded",
			node: &Node{
				Capacity: 10, CPUUsage: 50, MemoryUsed: 8, MemoryTotal: 16,
				VRAMUsed: 4, VRAMTotal: 8, ActiveConns: 5,
			},
			expected: 0.5,
		},
		{
			name: "zero capacity",
			node: &Node{
				Capacity: 0, CPUUsage: 50, MemoryUsed: 8, MemoryTotal: 16,
				VRAMUsed: 4, VRAMTotal: 8, ActiveConns: 5,
			},
			expected: (0.5*0.2 + 0.5*0.2 + 0.5*0.4 + 0.0),
		},
		{
			name: "no memory reported",
			node: &Node{
				Capacity: 10, CPUUsage: 50, MemoryUsed: 0, MemoryTotal: 0,
				VRAMUsed: 0, VRAMTotal: 0, ActiveConns: 5,
			},
			expected: (0.5*0.2 + 0.0 + 0.0 + 0.5*0.2),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			score := tt.node.LoadScore()
			if score != tt.expected {
				t.Fatalf("expected score %f, got %f", tt.expected, score)
			}
		})
	}
}

func TestAvailableSlots(t *testing.T) {
	n := newNode("test")
	n.ActiveConns = 3
	if slots := n.AvailableSlots(); slots != 7 {
		t.Fatalf("expected 7 available slots, got %d", slots)
	}
}

func TestAvailableSlotsNegative(t *testing.T) {
	n := newNode("test")
	n.ActiveConns = 15
	if slots := n.AvailableSlots(); slots != 0 {
		t.Fatalf("expected 0 available slots for negative, got %d", slots)
	}
}

func TestIsHealthy(t *testing.T) {
	n := newNode("test")
	n.Status = NodeOnline
	n.LastHeartbeat = time.Now()

	if !n.IsHealthy(10 * time.Second) {
		t.Fatal("expected node to be healthy")
	}
}

func TestIsHealthyStaleHeartbeat(t *testing.T) {
	n := newNode("test")
	n.Status = NodeOnline
	n.LastHeartbeat = time.Now().Add(-20 * time.Second)

	if n.IsHealthy(10 * time.Second) {
		t.Fatal("expected node with stale heartbeat to be unhealthy")
	}
}

func TestIsHealthyOffline(t *testing.T) {
	n := newNode("test")
	n.Status = NodeOffline
	n.LastHeartbeat = time.Now()

	if n.IsHealthy(10 * time.Second) {
		t.Fatal("expected offline node to be unhealthy")
	}
}

func TestGetActiveConns(t *testing.T) {
	n := newNode("test")
	n.ActiveConns = 7
	if conns := n.GetActiveConns(); conns != 7 {
		t.Fatalf("expected 7, got %d", conns)
	}
}

func TestGetModels(t *testing.T) {
	n := newNode("test")
	n.Models = []string{"llama2", "mistral"}
	models := n.GetModels()
	if len(models) != 2 || models[0] != "llama2" {
		t.Fatal("get models failed")
	}
	models[0] = "modified"
	if n.Models[0] != "llama2" {
		t.Fatal("get models should return a copy")
	}
}

func TestSetModels(t *testing.T) {
	n := newNode("test")
	n.SetModels([]string{"llama3"})
	if len(n.Models) != 1 || n.Models[0] != "llama3" {
		t.Fatal("set models failed")
	}
}

func TestMarshalJSON(t *testing.T) {
	n := newNode("json-test")
	n.Name = "json-node"
	n.Models = []string{"m1"}
	data, err := n.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	expected := `"id":"json-test"`
	if string(data) == "" {
		t.Fatal("expected non-empty json")
	}
	if !contains(string(data), expected) {
		t.Fatalf("expected json to contain %s, got %s", expected, string(data))
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestLoadScoreThreadSafe(t *testing.T) {
	n := newNode("concurrent")
	n.CPUUsage = 30
	n.MemoryUsed = 4
	n.MemoryTotal = 16
	n.VRAMUsed = 2
	n.VRAMTotal = 8
	n.ActiveConns = 2
	n.Capacity = 10

	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			n.LoadScore()
		}
		done <- true
	}()
	go func() {
		for i := 0; i < 100; i++ {
			n.UpdateLoad(50, 8, 16, 4, 8, 5)
		}
		done <- true
	}()
	<-done
	<-done
}
