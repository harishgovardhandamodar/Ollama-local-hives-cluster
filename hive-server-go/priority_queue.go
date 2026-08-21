package main

import (
	"sync"
	"time"
)

// JobPriority defines the priority levels for jobs in the queue
type JobPriority int

const (
	PriorityRealtime JobPriority = 0 // Interactive chat, streaming - highest priority
	PriorityHigh     JobPriority = 1 // Coding agent sessions
	PriorityNormal   JobPriority = 2 // Batch processing - default
	PriorityLow      JobPriority = 3 // Background tasks - lowest priority
)

// String returns the string representation of JobPriority
func (p JobPriority) String() string {
	switch p {
	case PriorityRealtime:
		return "realtime"
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	default:
		return "unknown"
	}
}

// PriorityQueue implements a multi-tier priority queue with deadline awareness
type PriorityQueue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	queues   [4]*concurrentDeque // One deque per priority level
	closed   bool
	
	// Stats
	enqueued   int64
	dequeued   int64
	dropped    int64
}

// concurrentDeque is a thread-safe double-ended queue
type concurrentDeque struct {
	items []*Job
	head  int
	tail  int
	mu    sync.RWMutex
}

func newConcurrentDeque(capacity int) *concurrentDeque {
	return &concurrentDeque{
		items: make([]*Job, 0, capacity),
	}
}

func (d *concurrentDeque) pushBack(job *Job) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.items = append(d.items, job)
}

func (d *concurrentDeque) pushFront(job *Job) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.items = append([]*Job{job}, d.items...)
}

func (d *concurrentDeque) popFront() *Job {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.items) == 0 {
		return nil
	}
	job := d.items[0]
	d.items = d.items[1:]
	return job
}

func (d *concurrentDeque) peekFront() *Job {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.items) == 0 {
		return nil
	}
	return d.items[0]
}

func (d *concurrentDeque) len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.items)
}

func (d *concurrentDeque) remove(jobID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, job := range d.items {
		if job.ID == jobID {
			d.items = append(d.items[:i], d.items[i+1:]...)
			return true
		}
	}
	return false
}

// NewPriorityQueue creates a new priority queue
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		queues: [4]*concurrentDeque{
			newConcurrentDeque(100), // Realtime
			newConcurrentDeque(200), // High
			newConcurrentDeque(500), // Normal
			newConcurrentDeque(1000), // Low
		},
	}
	pq.cond = sync.NewCond(&pq.mu)
	return pq
}

// Enqueue adds a job to the appropriate priority queue
func (pq *PriorityQueue) Enqueue(job *Job) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.closed {
		return false
	}

	if job.Priority < PriorityRealtime || job.Priority > PriorityLow {
		job.Priority = PriorityNormal
	}

	pq.queues[job.Priority].pushBack(job)
	pq.enqueued++

	// Signal waiting workers
	pq.cond.Signal()
	return true
}

// Dequeue removes and returns the highest priority job
// Considers both priority and deadline urgency
func (pq *PriorityQueue) Dequeue() *Job {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for {
		// Check for urgent deadline jobs first
		if urgent := pq.findUrgentDeadlineJob(); urgent != nil {
			pq.dequeued++
			return urgent
		}

		// Otherwise, serve by priority
		for p := PriorityRealtime; p <= PriorityLow; p++ {
			if job := pq.queues[p].popFront(); job != nil {
				pq.dequeued++
				return job
			}
		}

		// Queue is empty, wait
		pq.cond.Wait()

		if pq.closed {
			return nil
		}
	}
}

// findUrgentDeadlineJob finds jobs that are close to their deadline
// and promotes them ahead of other jobs
func (pq *PriorityQueue) findUrgentDeadlineJob() *Job {
	urgencyThreshold := 5 * time.Second // Jobs within 5s of deadline are urgent

	for p := PriorityRealtime; p <= PriorityLow; p++ {
		queue := pq.queues[p]
		queue.mu.RLock()
		for _, job := range queue.items {
			if !job.Deadline.IsZero() {
				timeUntilDeadline := time.Until(job.Deadline)
				if timeUntilDeadline <= urgencyThreshold && timeUntilDeadline > 0 {
					queue.mu.RUnlock()
					// Remove from current position and return
					if queue.remove(job.ID) {
						return job
					}
					continue
				} else if timeUntilDeadline <= 0 {
					// Already past deadline - still process but log warning
					queue.mu.RUnlock()
					if queue.remove(job.ID) {
						return job
					}
					continue
				}
			}
		}
		queue.mu.RUnlock()
	}
	return nil
}

// Peek returns the next job without removing it
func (pq *PriorityQueue) Peek() *Job {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if urgent := pq.findUrgentDeadlineJob(); urgent != nil {
		return urgent
	}

	for p := PriorityRealtime; p <= PriorityLow; p++ {
		if job := pq.queues[p].peekFront(); job != nil {
			return job
		}
	}
	return nil
}

// Len returns the total number of jobs in all queues
func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	total := 0
	for _, q := range pq.queues {
		total += q.len()
	}
	return total
}

// LenByPriority returns the count for a specific priority level
func (pq *PriorityQueue) LenByPriority(priority JobPriority) int {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if priority < PriorityRealtime || priority > PriorityLow {
		return 0
	}
	return pq.queues[priority].len()
}

// Cancel removes a job from the queue
func (pq *PriorityQueue) Cancel(jobID string) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for _, q := range pq.queues {
		if q.remove(jobID) {
			return true
		}
	}
	return false
}

// Close closes the queue and wakes up all waiting workers
func (pq *PriorityQueue) Close() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.closed = true
	pq.cond.Broadcast()
}

// Stats returns queue statistics
func (pq *PriorityQueue) Stats() map[string]interface{} {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return map[string]interface{}{
		"total":      pq.Len(),
		"realtime":   pq.queues[PriorityRealtime].len(),
		"high":       pq.queues[PriorityHigh].len(),
		"normal":     pq.queues[PriorityNormal].len(),
		"low":        pq.queues[PriorityLow].len(),
		"enqueued":   pq.enqueued,
		"dequeued":   pq.dequeued,
		"dropped":    pq.dropped,
	}
}

// GetStats returns typed queue statistics for metrics
func (pq *PriorityQueue) GetStats() QueueStats {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	return QueueStats{
		RealtimeDepth: pq.queues[PriorityRealtime].len(),
		HighDepth:     pq.queues[PriorityHigh].len(),
		NormalDepth:   pq.queues[PriorityNormal].len(),
		LowDepth:      pq.queues[PriorityLow].len(),
		TotalEnqueued: pq.enqueued,
		TotalDequeued: pq.dequeued,
		TotalDropped:  pq.dropped,
	}
}

// QueueStats holds typed queue statistics
type QueueStats struct {
	RealtimeDepth int
	HighDepth     int
	NormalDepth   int
	LowDepth      int
	TotalEnqueued int64
	TotalDequeued int64
	TotalDropped  int64
}

// DetermineJobPriority determines the appropriate priority for a job based on type and context
func DetermineJobPriority(jobType string, stream bool, hasInteractiveClient bool) JobPriority {
	// Streaming requests from interactive clients get realtime priority
	if stream && hasInteractiveClient {
		return PriorityRealtime
	}

	switch jobType {
	case "chat", "chat_stream":
		if stream {
			return PriorityRealtime
		}
		return PriorityHigh

	case "coding_agent_chat", "agent_message":
		return PriorityHigh

	case "generate", "generate_stream":
		if stream {
			return PriorityRealtime
		}
		return PriorityNormal

	case "embed", "get_embedding":
		// Embeddings are usually batch operations
		return PriorityNormal

	case "list_models", "pull_model":
		return PriorityLow

	default:
		return PriorityNormal
	}
}

// CalculateDeadline calculates an appropriate deadline based on job type and token count
func CalculateDeadline(jobType string, estimatedTokens int, maxTimeout time.Duration) time.Time {
	// Base timeout by job type
	var baseTimeout time.Duration

	switch jobType {
	case "chat", "chat_stream":
		baseTimeout = 60 * time.Second
	case "coding_agent_chat":
		baseTimeout = 120 * time.Second
	case "generate":
		baseTimeout = 90 * time.Second
	case "embed":
		baseTimeout = 30 * time.Second
	default:
		baseTimeout = 60 * time.Second
	}

	// Add time based on estimated tokens (assume ~20 tokens/sec average)
	tokenTime := time.Duration(estimatedTokens/20) * time.Second

	totalTimeout := baseTimeout + tokenTime
	if totalTimeout > maxTimeout {
		totalTimeout = maxTimeout
	}

	return time.Now().Add(totalTimeout)
}
