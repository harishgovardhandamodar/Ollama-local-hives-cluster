package cluster

import (
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	m := NewManager(10*time.Second, nil)
	if m == nil {
		t.Fatal("expected non-nil manager")
	}
}

func TestRegisterNode(t *testing.T) {
	m := NewManager(10*time.Second, nil)
	n := newNode("node-1")
	n.Name = "Test Node"
	n.Address = "192.168.1.1"
	n.Port = 11434

	m.RegisterNode(n)

	got := m.GetNode("node-1")
	if got == nil {
		t.Fatal("expected to find registered node")
	}
	if got.Name != "Test Node" {
		t.Fatalf("expected name Test Node, got %s", got.Name)
	}
	if got.Status != NodeOnline {
		t.Fatalf("expected status online, got %s", got.Status)
	}
}

func TestRegisterNodeUpdatesExisting(t *testing.T) {
	m := NewManager(10*time.Second, nil)
	now := time.Now()

	n1 := newNode("node-1")
	n1.Name = "Original"
	n1.Address = "10.0.0.1"
	n1.Status = NodeOffline
	n1.LastHeartbeat = now.Add(-1 * time.Hour)
	m.RegisterNode(n1)

	n2 := newNode("node-1")
	n2.Name = "Updated"
	n2.Address = "10.0.0.2"
	n2.Capacity = 20
	m.RegisterNode(n2)

	got := m.GetNode("node-1")
	if got.Name != "Updated" {
		t.Fatalf("expected name Updated, got %s", got.Name)
	}
	if got.Address != "10.0.0.2" {
		t.Fatalf("expected address 10.0.0.2, got %s", got.Address)
	}
	if got.Status != NodeOnline {
		t.Fatalf("expected status to be reset to online, got %s", got.Status)
	}
	if got.LastHeartbeat.Before(now) {
		t.Fatal("expected heartbeat to be updated")
	}
}

func TestDeregisterNode(t *testing.T) {
	m := NewManager(10*time.Second, nil)
	m.RegisterNode(newNode("node-1"))
	m.RegisterNode(newNode("node-2"))

	m.DeregisterNode("node-1")
	if m.GetNode("node-1") != nil {
		t.Fatal("expected node-1 to be removed")
	}
	if m.GetNode("node-2") == nil {
		t.Fatal("expected node-2 to remain")
	}
}

func TestDeregisterNodeNonExistent(t *testing.T) {
	m := NewManager(10*time.Second, nil)
	m.DeregisterNode("ghost")
}

func TestGetNodes(t *testing.T) {
	m := NewManager(10*time.Second, nil)
	m.RegisterNode(newNode("a"))
	m.RegisterNode(newNode("b"))

	nodes := m.GetNodes()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
}

func TestGetHealthyNodes(t *testing.T) {
	m := NewManager(10*time.Second, nil)

	healthy := newNode("healthy")
	m.RegisterNode(healthy)

	stale := newNode("stale")
	m.RegisterNode(stale)
	stale.mu.Lock()
	stale.LastHeartbeat = time.Now().Add(-20 * time.Second)
	stale.mu.Unlock()

	offline := newNode("offline")
	m.RegisterNode(offline)
	offline.mu.Lock()
	offline.Status = NodeOffline
	offline.mu.Unlock()

	healthyNodes := m.GetHealthyNodes()
	if len(healthyNodes) != 1 {
		t.Fatalf("expected 1 healthy node, got %d", len(healthyNodes))
	}
	if healthyNodes[0].ID != "healthy" {
		t.Fatalf("expected healthy node id healthy, got %s", healthyNodes[0].ID)
	}
}

func TestUpdateNodeHeartbeat(t *testing.T) {
	m := NewManager(10*time.Second, nil)
	m.RegisterNode(newNode("node-1"))

	before := time.Now()
	m.UpdateNodeHeartbeat("node-1", 75.0, 12.0, 16.0, 6.0, 8.0, 8)
	after := time.Now()

	n := m.GetNode("node-1")
	if n.CPUUsage != 75.0 {
		t.Fatalf("expected cpu 75.0, got %f", n.CPUUsage)
	}
	if n.ActiveConns != 8 {
		t.Fatalf("expected active conns 8, got %d", n.ActiveConns)
	}
	if n.LastHeartbeat.Before(before) || n.LastHeartbeat.After(after) {
		t.Fatal("heartbeat time should be updated")
	}
}

func TestUpdateNodeHeartbeatNonExistent(t *testing.T) {
	m := NewManager(10*time.Second, nil)
	m.UpdateNodeHeartbeat("ghost", 50, 8, 16, 4, 8, 5)
}

func TestCheckStale(t *testing.T) {
	m := NewManager(100*time.Millisecond, nil)
	n := newNode("fast-stale")
	m.RegisterNode(n)

	n.mu.Lock()
	n.LastHeartbeat = time.Now().Add(-1 * time.Second)
	n.mu.Unlock()

	m.StartHeartbeatChecker()
	time.Sleep(200 * time.Millisecond)

	got := m.GetNode("fast-stale")
	if got.Status != NodeOffline {
		t.Fatalf("expected stale node to be marked offline, got %s", got.Status)
	}
}

func TestOnUpdateCallback(t *testing.T) {
	callCount := 0
	cb := func() { callCount++ }

	m := NewManager(10*time.Second, cb)
	m.RegisterNode(newNode("n1"))
	if callCount != 1 {
		t.Fatalf("expected 1 callback call, got %d", callCount)
	}

	m.DeregisterNode("n1")
	if callCount != 2 {
		t.Fatalf("expected 2 callback calls, got %d", callCount)
	}

	m.RegisterNode(newNode("n1"))
	m.UpdateNodeHeartbeat("n1", 50, 8, 16, 4, 8, 5)
	if callCount != 4 {
		t.Fatalf("expected 4 callback calls, got %d", callCount)
	}
}

func TestManagerConcurrency(t *testing.T) {
	m := NewManager(10*time.Second, nil)

	done := make(chan bool)
	go func() {
		for i := 0; i < 50; i++ {
			m.RegisterNode(newNode("n"))
		}
		done <- true
	}()
	go func() {
		for i := 0; i < 50; i++ {
			m.GetNodes()
		}
		done <- true
	}()
	go func() {
		for i := 0; i < 50; i++ {
			m.GetHealthyNodes()
		}
		done <- true
	}()
	<-done
	<-done
	<-done
}
