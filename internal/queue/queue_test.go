package queue

import (
	"testing"
	"time"
)

func TestNewQueue(t *testing.T) {
	q := New(10)
	if q == nil {
		t.Fatal("expected non-nil queue")
	}
	if q.Len() != 0 {
		t.Fatalf("expected empty queue, got %d", q.Len())
	}
}

func TestEnqueueDequeue(t *testing.T) {
	q := New(10)
	req := &Request{ID: "test-1", Model: "llama2", Priority: PriorityNormal}

	if !q.Enqueue(req) {
		t.Fatal("expected enqueue to succeed")
	}
	if q.Len() != 1 {
		t.Fatalf("expected len 1, got %d", q.Len())
	}

	dq := q.Dequeue()
	if dq == nil {
		t.Fatal("expected non-nil dequeue")
	}
	if dq.ID != "test-1" {
		t.Fatalf("expected id test-1, got %s", dq.ID)
	}
	if q.Len() != 0 {
		t.Fatalf("expected empty queue, got %d", q.Len())
	}
}

func TestEnqueueFull(t *testing.T) {
	q := New(2)
	q.Enqueue(&Request{ID: "1", Priority: PriorityNormal})
	q.Enqueue(&Request{ID: "2", Priority: PriorityNormal})
	if q.Enqueue(&Request{ID: "3", Priority: PriorityNormal}) {
		t.Fatal("expected enqueue to fail when full")
	}
}

func TestDequeueEmpty(t *testing.T) {
	q := New(10)
	if dq := q.Dequeue(); dq != nil {
		t.Fatal("expected nil from empty queue")
	}
}

func TestPeek(t *testing.T) {
	q := New(10)
	if p := q.Peek(); p != nil {
		t.Fatal("expected nil peek from empty queue")
	}
	q.Enqueue(&Request{ID: "first", Priority: PriorityNormal})
	p := q.Peek()
	if p == nil || p.ID != "first" {
		t.Fatalf("expected peek to return 'first', got %v", p)
	}
}

func TestPriorityOrdering(t *testing.T) {
	q := New(10)
	q.Enqueue(&Request{ID: "low", Priority: PriorityLow})
	q.Enqueue(&Request{ID: "high", Priority: PriorityHigh})
	q.Enqueue(&Request{ID: "normal", Priority: PriorityNormal})

	ids := []string{q.Dequeue().ID, q.Dequeue().ID, q.Dequeue().ID}
	expected := []string{"high", "normal", "low"}
	for i, id := range ids {
		if id != expected[i] {
			t.Fatalf("expected %s at position %d, got %s", expected[i], i, id)
		}
	}
}

func TestCancel(t *testing.T) {
	q := New(10)
	q.Enqueue(&Request{ID: "cancel-me", Priority: PriorityNormal})
	q.Enqueue(&Request{ID: "keep-me", Priority: PriorityNormal})

	if !q.Cancel("cancel-me") {
		t.Fatal("expected cancel to return true")
	}
	if q.Len() != 1 {
		t.Fatalf("expected len 1 after cancel, got %d", q.Len())
	}
	if q.Dequeue().ID != "keep-me" {
		t.Fatal("expected remaining item to be keep-me")
	}
}

func TestCancelNotFound(t *testing.T) {
	q := New(10)
	if q.Cancel("nonexistent") {
		t.Fatal("expected cancel to return false for nonexistent id")
	}
}

func TestCancelRunning(t *testing.T) {
	q := New(10)
	req := &Request{ID: "running", Priority: PriorityNormal, Status: StatusRunning}
	q.items = append(q.items, req)
	if q.Cancel("running") {
		t.Fatal("expected cancel to return false for running request")
	}
}

func TestList(t *testing.T) {
	q := New(10)
	q.Enqueue(&Request{ID: "a", Priority: PriorityNormal})
	q.Enqueue(&Request{ID: "b", Priority: PriorityNormal})
	items := q.List()
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].ID != "a" || items[1].ID != "b" {
		t.Fatal("list returned items in wrong order")
	}
}

func TestNotify(t *testing.T) {
	q := New(10)
	ch := q.Notify()
	select {
	case <-ch:
		t.Fatal("expected no notification on empty queue")
	default:
	}
	q.Enqueue(&Request{ID: "test", Priority: PriorityNormal})
	select {
	case <-ch:
	default:
		t.Fatal("expected notification after enqueue")
	}
}

func TestNewHistory(t *testing.T) {
	h := NewHistory(100)
	if h == nil {
		t.Fatal("expected non-nil history")
	}
}

func TestHistoryAddAndList(t *testing.T) {
	h := NewHistory(10)
	h.Add(&Request{ID: "req-1", Status: StatusComplete})
	h.Add(&Request{ID: "req-2", Status: StatusComplete})

	items := h.List(10)
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0].ID != "req-2" {
		t.Fatalf("expected newest first (req-2), got %s", items[0].ID)
	}
}

func TestHistoryListLimit(t *testing.T) {
	h := NewHistory(10)
	for i := 0; i < 5; i++ {
		h.Add(&Request{ID: "r", Status: StatusComplete})
	}
	items := h.List(2)
	if len(items) != 2 {
		t.Fatalf("expected 2 items with limit, got %d", len(items))
	}
}

func TestHistoryMaxSize(t *testing.T) {
	h := NewHistory(3)
	for i := 0; i < 5; i++ {
		h.Add(&Request{ID: "r", Status: StatusComplete})
	}
	if len(h.requests) != 3 {
		t.Fatalf("expected history to be capped at 3, got %d", len(h.requests))
	}
}

func TestHistoryStats(t *testing.T) {
	h := NewHistory(10)
	h.Add(&Request{ID: "c1", Status: StatusComplete})
	h.Add(&Request{ID: "f1", Status: StatusFailed})
	h.Add(&Request{ID: "c2", Status: StatusComplete})
	h.Add(&Request{ID: "t1", Status: StatusTimeout})

	stats := h.Stats()
	if stats.Completed != 2 {
		t.Fatalf("expected 2 completed, got %d", stats.Completed)
	}
	if stats.Failed != 2 {
		t.Fatalf("expected 2 failed/timeout, got %d", stats.Failed)
	}
}

func TestEnqueueSetsTimestamps(t *testing.T) {
	q := New(10)
	before := time.Now()
	q.Enqueue(&Request{ID: "t1", Priority: PriorityNormal})
	after := time.Now()

	req := q.Dequeue()
	if req.CreatedAt.Before(before) || req.CreatedAt.After(after) {
		t.Fatal("created_at should be set to current time")
	}
	if req.Status != StatusPending {
		t.Fatalf("expected status pending, got %s", req.Status)
	}
}
