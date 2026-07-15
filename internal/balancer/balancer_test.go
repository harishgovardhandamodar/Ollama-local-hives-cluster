package balancer

import (
	"testing"
	"time"

	"github.com/hive-cluster/hive-serving/internal/cluster"
)

func newNode(id string, loadScore float64, slots int) *cluster.Node {
	n := &cluster.Node{
		ID: id, Capacity: 10, ActiveConns: 10 - slots,
		CPUUsage: loadScore * 100, Status: cluster.NodeOnline, LastHeartbeat: time.Now(),
	}
	if slots <= 0 {
		n.ActiveConns = n.Capacity
	}
	return n
}

func TestNewBalancer(t *testing.T) {
	b := New(func() []*cluster.Node { return nil }, "")
	if b == nil {
		t.Fatal("expected non-nil balancer")
	}
	if b.GetStrategy() != StrategyLeastLoad {
		t.Fatalf("expected default strategy least_load, got %s", b.GetStrategy())
	}
}

func TestSelectNodeNoNodes(t *testing.T) {
	b := New(func() []*cluster.Node { return nil }, StrategyLeastLoad)
	if n := b.SelectNode("model-x"); n != nil {
		t.Fatal("expected nil when no nodes")
	}
}

func TestSelectNodeNoHealthy(t *testing.T) {
	n := newNode("offline", 0.5, 5)
	n.Status = cluster.NodeOffline
	b := New(func() []*cluster.Node { return []*cluster.Node{n} }, StrategyLeastLoad)
	if n := b.SelectNode("model-x"); n != nil {
		t.Fatal("expected nil when no healthy nodes")
	}
}

func TestSelectNodeNoSlots(t *testing.T) {
	n := newNode("full", 0.5, 0)
	b := New(func() []*cluster.Node { return []*cluster.Node{n} }, StrategyLeastLoad)
	if n := b.SelectNode("model-x"); n != nil {
		t.Fatal("expected nil when no available slots")
	}
}

func TestLeastLoadStrategy(t *testing.T) {
	heavy := newNode("heavy", 0.9, 3)
	light := newNode("light", 0.1, 8)
	medium := newNode("medium", 0.5, 5)

	b := New(func() []*cluster.Node { return []*cluster.Node{heavy, light, medium} }, StrategyLeastLoad)
	for i := 0; i < 10; i++ {
		selected := b.SelectNode("model-x")
		if selected == nil {
			t.Fatal("expected non-nil selection")
		}
		if selected.ID != "light" {
			t.Fatalf("expected least load node 'light', got %s", selected.ID)
		}
	}
}

func TestRandomStrategy(t *testing.T) {
	nodes := []*cluster.Node{newNode("a", 0.5, 5), newNode("b", 0.5, 5), newNode("c", 0.5, 5)}
	b := New(func() []*cluster.Node { return nodes }, StrategyRandom)

	seen := make(map[string]int)
	for i := 0; i < 100; i++ {
		selected := b.SelectNode("model-x")
		if selected == nil {
			t.Fatal("expected non-nil selection")
		}
		seen[selected.ID]++
	}
	if len(seen) != 3 {
		t.Fatal("expected to see all 3 nodes with random strategy")
	}
}

func TestRoundRobinStrategy(t *testing.T) {
	nodes := []*cluster.Node{newNode("a", 0.5, 5), newNode("b", 0.5, 5), newNode("c", 0.5, 5)}
	b := New(func() []*cluster.Node { return nodes }, StrategyRoundRobin)

	for i := 0; i < 3; i++ {
		selected := b.SelectNode("model-x")
		expected := []string{"a", "b", "c"}[i]
		if selected.ID != expected {
			t.Fatalf("round robin: expected %s at iter %d, got %s", expected, i, selected.ID)
		}
	}
}

func TestCapacityStrategy(t *testing.T) {
	almostFull := newNode("almost-full", 0.9, 1)
	empty := newNode("empty", 0.0, 10)
	half := newNode("half", 0.5, 5)

	b := New(func() []*cluster.Node { return []*cluster.Node{almostFull, empty, half} }, StrategyCapacity)
	for i := 0; i < 10; i++ {
		selected := b.SelectNode("model-x")
		if selected == nil {
			t.Fatal("expected non-nil selection")
		}
		if selected.ID != "empty" {
			t.Fatalf("expected 'empty' (most capacity), got %s", selected.ID)
		}
	}
}

func TestSetGetStrategy(t *testing.T) {
	b := New(func() []*cluster.Node { return nil }, StrategyLeastLoad)
	b.SetStrategy(StrategyRoundRobin)
	if s := b.GetStrategy(); s != StrategyRoundRobin {
		t.Fatalf("expected StrategyRoundRobin, got %s", s)
	}
}

func TestMultipleModels(t *testing.T) {
	nodes := []*cluster.Node{newNode("n1", 0.3, 5), newNode("n2", 0.7, 3)}
	b := New(func() []*cluster.Node { return nodes }, StrategyLeastLoad)
	if n := b.SelectNode("any-model"); n == nil || n.ID != "n1" {
		t.Fatal("expected to select lowest load node regardless of model")
	}
}
