package queue

import (
	"sync"
	"time"
)

type Priority int

const (
	PriorityLow    Priority = 0
	PriorityNormal Priority = 1
	PriorityHigh   Priority = 2
)

type RequestStatus string

const (
	StatusPending  RequestStatus = "pending"
	StatusRunning  RequestStatus = "running"
	StatusComplete RequestStatus = "complete"
	StatusFailed   RequestStatus = "failed"
	StatusTimeout  RequestStatus = "timeout"
)

type Request struct {
	ID          string        `json:"id"`
	Model       string        `json:"model"`
	Priority    Priority      `json:"priority"`
	Status      RequestStatus `json:"status"`
	NodeID      string        `json:"node_id,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
	StartedAt   *time.Time    `json:"started_at,omitempty"`
	CompletedAt *time.Time    `json:"completed_at,omitempty"`
	Error       string        `json:"error,omitempty"`
	Metadata    string        `json:"metadata,omitempty"`
}

type Queue struct {
	items  []*Request
	mu     sync.RWMutex
	maxSize int
	notify chan struct{}
}

func New(maxSize int) *Queue {
	q := &Queue{
		items:  make([]*Request, 0, maxSize),
		maxSize: maxSize,
		notify: make(chan struct{}, 1),
	}
	return q
}

func (q *Queue) Enqueue(req *Request) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) >= q.maxSize {
		return false
	}
	req.Status = StatusPending
	req.CreatedAt = time.Now()

	insertIdx := len(q.items)
	for i, item := range q.items {
		if item.Priority < req.Priority {
			insertIdx = i
			break
		}
	}
	q.items = append(q.items, nil)
	copy(q.items[insertIdx+1:], q.items[insertIdx:])
	q.items[insertIdx] = req

	select {
	case q.notify <- struct{}{}:
	default:
	}
	return true
}

func (q *Queue) Dequeue() *Request {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	req := q.items[0]
	q.items = q.items[1:]
	return req
}

func (q *Queue) Peek() *Request {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if len(q.items) == 0 {
		return nil
	}
	return q.items[0]
}

func (q *Queue) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.items)
}

func (q *Queue) List() []*Request {
	q.mu.RLock()
	defer q.mu.RUnlock()
	result := make([]*Request, len(q.items))
	copy(result, q.items)
	return result
}

func (q *Queue) Cancel(reqID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, req := range q.items {
		if req.ID == reqID && req.Status == StatusPending {
			q.items = append(q.items[:i], q.items[i+1:]...)
			req.Status = StatusFailed
			return true
		}
	}
	return false
}

func (q *Queue) Notify() <-chan struct{} {
	return q.notify
}

type Stats struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

type History struct {
	requests []*Request
	mu       sync.RWMutex
	maxSize  int
}

func NewHistory(maxSize int) *History {
	return &History{
		requests: make([]*Request, 0, maxSize),
		maxSize:  maxSize,
	}
}

func (h *History) Add(req *Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.requests = append(h.requests, req)
	if len(h.requests) > h.maxSize {
		h.requests = h.requests[len(h.requests)-h.maxSize:]
	}
}

func (h *History) List(limit int) []*Request {
	h.mu.RLock()
	defer h.mu.RUnlock()
	start := 0
	if len(h.requests) > limit {
		start = len(h.requests) - limit
	}
	result := make([]*Request, len(h.requests[start:]))
	copy(result, h.requests[start:])
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

func (h *History) Stats() Stats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	s := Stats{}
	for _, req := range h.requests {
		switch req.Status {
		case StatusComplete:
			s.Completed++
		case StatusFailed, StatusTimeout:
			s.Failed++
		}
	}
	return s
}
