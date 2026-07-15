package balancer

import (
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/hive-cluster/hive-serving/internal/cluster"
)

type Strategy string

const (
	StrategyLeastLoad Strategy = "least_load"
	StrategyRoundRobin Strategy = "round_robin"
	StrategyRandom    Strategy = "random"
	StrategyCapacity  Strategy = "capacity"
)

type Balancer struct {
	nodes     func() []*cluster.Node
	strategy  Strategy
	rrIndex   int
	mu        sync.RWMutex
}

func New(nodeFunc func() []*cluster.Node, strategy Strategy) *Balancer {
	if strategy == "" {
		strategy = StrategyLeastLoad
	}
	return &Balancer{
		nodes:    nodeFunc,
		strategy: strategy,
	}
}

func (b *Balancer) SelectNode(model string) *cluster.Node {
	nodes := b.nodes()
	if len(nodes) == 0 {
		return nil
	}

	healthy := make([]*cluster.Node, 0)
	for _, n := range nodes {
		if n.IsHealthy(10*time.Second) && n.AvailableSlots() > 0 {
			healthy = append(healthy, n)
		}
	}
	if len(healthy) == 0 {
		return nil
	}

	switch b.strategy {
	case StrategyLeastLoad:
		return b.leastLoad(healthy)
	case StrategyRoundRobin:
		return b.roundRobin(healthy)
	case StrategyRandom:
		return b.random(healthy)
	case StrategyCapacity:
		return b.bestCapacity(healthy)
	default:
		return b.leastLoad(healthy)
	}
}

func (b *Balancer) leastLoad(nodes []*cluster.Node) *cluster.Node {
	if len(nodes) == 0 {
		return nil
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].LoadScore() < nodes[j].LoadScore()
	})
	return nodes[0]
}

func (b *Balancer) roundRobin(nodes []*cluster.Node) *cluster.Node {
	b.mu.Lock()
	defer b.mu.Unlock()
	idx := b.rrIndex % len(nodes)
	b.rrIndex++
	return nodes[idx]
}

func (b *Balancer) random(nodes []*cluster.Node) *cluster.Node {
	return nodes[rand.Intn(len(nodes))]
}

func (b *Balancer) bestCapacity(nodes []*cluster.Node) *cluster.Node {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].AvailableSlots() > nodes[j].AvailableSlots()
	})
	return nodes[0]
}

func (b *Balancer) SetStrategy(s Strategy) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.strategy = s
}

func (b *Balancer) GetStrategy() Strategy {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.strategy
}
