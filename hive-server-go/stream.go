package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type JobStream struct {
	mu       sync.RWMutex
	clients  map[string][]chan JobEvent
}

type JobEvent struct {
	JobID    string      `json:"job_id"`
	Status   string      `json:"status"`
	Result   interface{} `json:"result,omitempty"`
	Error    string      `json:"error,omitempty"`
	Progress float64     `json:"progress,omitempty"`
}

var globalJobStream = &JobStream{
	clients: make(map[string][]chan JobEvent),
}

func (js *JobStream) Subscribe(jobID string) chan JobEvent {
	js.mu.Lock()
	defer js.mu.Unlock()

	ch := make(chan JobEvent, 10)
	js.clients[jobID] = append(js.clients[jobID], ch)
	return ch
}

func (js *JobStream) Unsubscribe(jobID string, ch chan JobEvent) {
	js.mu.Lock()
	defer js.mu.Unlock()

	clients := js.clients[jobID]
	for i, c := range clients {
		if c == ch {
			js.clients[jobID] = append(clients[:i], clients[i+1:]...)
			close(ch)
			break
		}
	}
	if len(js.clients[jobID]) == 0 {
		delete(js.clients, jobID)
	}
}

func (js *JobStream) Publish(event JobEvent) {
	js.mu.RLock()
	clients := js.clients[event.JobID]
	js.mu.RUnlock()

	for _, ch := range clients {
		select {
		case ch <- event:
		default:
			// Client too slow, skip
		}
	}
}

// SSE endpoint for job streaming
func handleJobStream(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		http.Error(w, "job_id required", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := globalJobStream.Subscribe(jobID)
	defer globalJobStream.Unsubscribe(jobID, ch)

	// Send initial state if job exists (using global queue reference)
	if globalQueue != nil {
		if job := globalQueue.GetJob(jobID); job != nil {
			event := JobEvent{
				JobID:  job.ID,
				Status: string(job.Status),
				Result: job.Result,
				Error:  job.Error,
			}
			data, _ := json.Marshal(event)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()

			if job.Status == JobCompleted || job.Status == JobFailed {
				return
			}
		}
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			data, _ := json.Marshal(event)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()

			if event.Status == "completed" || event.Status == "failed" {
				return
			}
		case <-time.After(60 * time.Second):
			// Keepalive
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

// NotifyJobUpdate publishes job status to subscribers
func NotifyJobUpdate(job *Job) {
	globalJobStream.Publish(JobEvent{
		JobID:  job.ID,
		Status: string(job.Status),
		Result: job.Result,
		Error:  job.Error,
	})
}
