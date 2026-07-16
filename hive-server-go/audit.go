package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

type AuditEvent struct {
	ID            int64                  `json:"id"`
	RequestID     string                 `json:"request_id"`
	ParentID      string                 `json:"parent_id,omitempty"`
	EventType     string                 `json:"event_type"` // request, response, error, forward, cache_hit, job_submit, job_complete
	Category      string                 `json:"category"`  // coding_agent, openai_compat, job_queue, mesh, system
	Method        string                 `json:"method,omitempty"`
	Path          string                 `json:"path,omitempty"`
	StatusCode    int                    `json:"status_code,omitempty"`
	ClientID      string                 `json:"client_id,omitempty"`
	AgentType     string                 `json:"agent_type,omitempty"`
	Model         string                 `json:"model,omitempty"`
	JobType       string                 `json:"job_type,omitempty"`
	JobID         string                 `json:"job_id,omitempty"`
	Content       string                 `json:"content,omitempty"`
	Query         string                 `json:"query,omitempty"`
	Prompt        string                 `json:"prompt,omitempty"`
	Overrides     map[string]interface{} `json:"overrides,omitempty"`
	DurationMs    float64                `json:"duration_ms,omitempty"`
	TokenCount    int                    `json:"token_count,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
	ErrorMessage  string                 `json:"error_message,omitempty"`
	CreatedAt     time.Time              `json:"created_at"`
}

type AuditTrailManager struct {
	db      *DBStore
	mu      sync.RWMutex
	buffer  []AuditEvent
	flushCh chan struct{}
	stopCh  chan struct{}
}

func NewAuditTrailManager(db *DBStore) *AuditTrailManager {
	atm := &AuditTrailManager{
		db:      db,
		buffer:  make([]AuditEvent, 0, 100),
		flushCh: make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
	}

	// Create table
	atm.createTable()

	// Start background flusher
	go atm.flushLoop()

	return atm
}

func (atm *AuditTrailManager) createTable() {
	query := `CREATE TABLE IF NOT EXISTS request_audit_trail (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		request_id TEXT NOT NULL,
		parent_id TEXT DEFAULT '',
		event_type TEXT NOT NULL,
		category TEXT NOT NULL DEFAULT 'system',
		method TEXT DEFAULT '',
		path TEXT DEFAULT '',
		status_code INTEGER DEFAULT 0,
		client_id TEXT DEFAULT '',
		agent_type TEXT DEFAULT '',
		model TEXT DEFAULT '',
		job_type TEXT DEFAULT '',
		job_id TEXT DEFAULT '',
		content TEXT DEFAULT '',
		query TEXT DEFAULT '',
		prompt TEXT DEFAULT '',
		overrides TEXT DEFAULT '{}',
		duration_ms REAL DEFAULT 0,
		token_count INTEGER DEFAULT 0,
		metadata TEXT DEFAULT '{}',
		error_message TEXT DEFAULT '',
		created_at REAL NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_audit_request ON request_audit_trail(request_id);
	CREATE INDEX IF NOT EXISTS idx_audit_parent ON request_audit_trail(parent_id);
	CREATE INDEX IF NOT EXISTS idx_audit_event ON request_audit_trail(event_type);
	CREATE INDEX IF NOT EXISTS idx_audit_category ON request_audit_trail(category);
	CREATE INDEX IF NOT EXISTS idx_audit_created ON request_audit_trail(created_at);
	CREATE INDEX IF NOT EXISTS idx_audit_model ON request_audit_trail(model);
	CREATE INDEX IF NOT EXISTS idx_audit_job ON request_audit_trail(job_id);`

	if _, err := atm.db.db.ExecContext(context.Background(), query); err != nil {
		logError("Failed to create audit trail table: %v", err)
	}
}

func (atm *AuditTrailManager) LogEvent(event AuditEvent) {
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now()
	}
	if event.RequestID == "" {
		event.RequestID = generateRequestID()
	}

	atm.mu.Lock()
	atm.buffer = append(atm.buffer, event)
	atm.mu.Unlock()

	// Trigger async flush
	select {
	case atm.flushCh <- struct{}{}:
	default:
	}
}

func (atm *AuditTrailManager) LogRequest(r *http.Request, requestBody []byte, clientID string) string {
	requestID := generateRequestID()

	var prompt, query, model, jobType string
	overrides := make(map[string]interface{})

	if len(requestBody) > 0 {
		var body map[string]interface{}
		if json.Unmarshal(requestBody, &body) == nil {
			if p, ok := body["prompt"].(string); ok {
				prompt = p
			}
			if q, ok := body["query"].(string); ok {
				query = q
			}
			if m, ok := body["model"].(string); ok {
				model = m
			}
			if jt, ok := body["job_type"].(string); ok {
				jobType = jt
			}
			// Handle nested payload (job submit format)
			if payload, ok := body["payload"].(map[string]interface{}); ok {
				if p, ok := payload["prompt"].(string); ok && prompt == "" {
					prompt = p
				}
				if m, ok := payload["model"].(string); ok && model == "" {
					model = m
				}
				if msgs, ok := payload["messages"].([]interface{}); ok && len(msgs) > 0 && prompt == "" {
					if lastMsg, ok := msgs[len(msgs)-1].(map[string]interface{}); ok {
						if content, ok := lastMsg["content"].(string); ok {
							prompt = content
						}
					}
				}
			}
			if msgs, ok := body["messages"].([]interface{}); ok && len(msgs) > 0 && prompt == "" {
				if lastMsg, ok := msgs[len(msgs)-1].(map[string]interface{}); ok {
					if content, ok := lastMsg["content"].(string); ok {
						prompt = content
					}
				}
			}
			overrides = body
		}
	}

	category := "system"
	path := r.URL.Path
	if strings.HasPrefix(path, "/api/agent") {
		category = "coding_agent"
	} else if strings.HasPrefix(path, "/v1/") {
		category = "openai_compat"
	} else if strings.HasPrefix(path, "/api/jobs") {
		category = "job_queue"
	} else if strings.HasPrefix(path, "/api/peers") {
		category = "mesh"
	}

	atm.LogEvent(AuditEvent{
		RequestID: requestID,
		EventType: "request",
		Category:  category,
		Method:    r.Method,
		Path:      path,
		ClientID:  clientID,
		Model:     model,
		JobType:   jobType,
		Query:     query,
		Prompt:    prompt,
		Overrides: overrides,
		Metadata: map[string]interface{}{
			"remote_addr": r.RemoteAddr,
			"user_agent":  r.UserAgent(),
		},
	})

	return requestID
}

func (atm *AuditTrailManager) LogResponse(requestID string, statusCode int, responseBody []byte, durationMs float64) {
	var content string
	var tokenCount int

	if len(responseBody) > 0 {
		var body map[string]interface{}
		if json.Unmarshal(responseBody, &body) == nil {
			// Extract content from response
			if c, ok := body["content"].(string); ok {
				content = c
			}
			// Extract from choices
			if choices, ok := body["choices"].([]interface{}); ok && len(choices) > 0 {
				if choice, ok := choices[0].(map[string]interface{}); ok {
					if msg, ok := choice["message"].(map[string]interface{}); ok {
						if c, ok := msg["content"].(string); ok {
							content = c
						}
					}
				}
			}
			// Extract token count
			if usage, ok := body["usage"].(map[string]interface{}); ok {
				if total, ok := usage["total_tokens"].(float64); ok {
					tokenCount = int(total)
				}
			}
		}
	}

	atm.LogEvent(AuditEvent{
		RequestID:  requestID,
		EventType:  "response",
		StatusCode: statusCode,
		Content:    content,
		TokenCount: tokenCount,
		DurationMs: durationMs,
	})
}

func (atm *AuditTrailManager) LogJobEvent(requestID, jobID, jobType, model, eventType string, metadata map[string]interface{}) {
	atm.LogEvent(AuditEvent{
		RequestID: requestID,
		EventType: eventType,
		Category:  "job_queue",
		JobID:     jobID,
		JobType:   jobType,
		Model:     model,
		Metadata:  metadata,
	})
}

func (atm *AuditTrailManager) LogForwardEvent(requestID, peerID, model, eventType string) {
	atm.LogEvent(AuditEvent{
		RequestID: requestID,
		EventType: eventType,
		Category:  "mesh",
		Model:     model,
		Metadata: map[string]interface{}{
			"peer_id": peerID,
		},
	})
}

func (atm *AuditTrailManager) LogError(requestID, errorMsg string, metadata map[string]interface{}) {
	atm.LogEvent(AuditEvent{
		RequestID:    requestID,
		EventType:    "error",
		ErrorMessage: errorMsg,
		Metadata:     metadata,
	})
}

func (atm *AuditTrailManager) flushLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-atm.stopCh:
			atm.flush()
			return
		case <-atm.flushCh:
			atm.flush()
		case <-ticker.C:
			atm.flush()
		}
	}
}

func (atm *AuditTrailManager) flush() {
	atm.mu.Lock()
	if len(atm.buffer) == 0 {
		atm.mu.Unlock()
		return
	}

	events := make([]AuditEvent, len(atm.buffer))
	copy(events, atm.buffer)
	atm.buffer = atm.buffer[:0]
	atm.mu.Unlock()

	for _, event := range events {
		overridesJSON, _ := json.Marshal(event.Overrides)
		metadataJSON, _ := json.Marshal(event.Metadata)

		query := `INSERT INTO request_audit_trail
			(request_id, parent_id, event_type, category, method, path, status_code,
			 client_id, agent_type, model, job_type, job_id, content, query, prompt,
			 overrides, duration_ms, token_count, metadata, error_message, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

		_, err := atm.db.db.ExecContext(context.Background(), query,
			event.RequestID, event.ParentID, event.EventType, event.Category,
			event.Method, event.Path, event.StatusCode, event.ClientID,
			event.AgentType, event.Model, event.JobType, event.JobID,
			event.Content, event.Query, event.Prompt, string(overridesJSON),
			event.DurationMs, event.TokenCount, string(metadataJSON),
			event.ErrorMessage, event.CreatedAt.Unix())

		if err != nil {
			logError("Failed to insert audit event: %v", err)
		}
	}
}

func (atm *AuditTrailManager) Stop() {
	close(atm.stopCh)
}

// Query methods
func (atm *AuditTrailManager) GetRequestTimeline(requestID string) ([]AuditEvent, error) {
	query := `SELECT id, request_id, parent_id, event_type, category, method, path,
		status_code, client_id, agent_type, model, job_type, job_id, content, query, prompt,
		overrides, duration_ms, token_count, metadata, error_message, created_at
		FROM request_audit_trail WHERE request_id = ? ORDER BY created_at ASC`

	rows, err := atm.db.db.QueryContext(context.Background(), query, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return atm.scanEvents(rows)
}

func (atm *AuditTrailManager) GetRecentEvents(limit int, category string) ([]AuditEvent, error) {
	var query string
	var args []interface{}

	if category != "" {
		query = `SELECT id, request_id, parent_id, event_type, category, method, path,
			status_code, client_id, agent_type, model, job_type, job_id, content, query, prompt,
			overrides, duration_ms, token_count, metadata, error_message, created_at
			FROM request_audit_trail WHERE category = ? ORDER BY created_at DESC LIMIT ?`
		args = append(args, category, limit)
	} else {
		query = `SELECT id, request_id, parent_id, event_type, category, method, path,
			status_code, client_id, agent_type, model, job_type, job_id, content, query, prompt,
			overrides, duration_ms, token_count, metadata, error_message, created_at
			FROM request_audit_trail ORDER BY created_at DESC LIMIT ?`
		args = append(args, limit)
	}

	rows, err := atm.db.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return atm.scanEvents(rows)
}

func (atm *AuditTrailManager) SearchEvents(searchQuery string, limit int) ([]AuditEvent, error) {
	query := `SELECT id, request_id, parent_id, event_type, category, method, path,
		status_code, client_id, agent_type, model, job_type, job_id, content, query, prompt,
		overrides, duration_ms, token_count, metadata, error_message, created_at
		FROM request_audit_trail
		WHERE content LIKE ? OR query LIKE ? OR prompt LIKE ? OR model LIKE ?
		OR path LIKE ? OR method LIKE ? OR job_type LIKE ? OR error_message LIKE ?
		ORDER BY created_at DESC LIMIT ?`

	searchPattern := "%" + searchQuery + "%"
	rows, err := atm.db.db.QueryContext(context.Background(), query,
		searchPattern, searchPattern, searchPattern, searchPattern,
		searchPattern, searchPattern, searchPattern, searchPattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return atm.scanEvents(rows)
}

func (atm *AuditTrailManager) GetRequestSummary(requestID string) (map[string]interface{}, error) {
	events, err := atm.GetRequestTimeline(requestID)
	if err != nil {
		return nil, err
	}

	if len(events) == 0 {
		return nil, nil
	}

	summary := map[string]interface{}{
		"request_id":  requestID,
		"first_event": events[0].CreatedAt,
		"last_event":  events[len(events)-1].CreatedAt,
		"event_count": len(events),
		"events":      events,
	}

	// Calculate total duration
	if len(events) > 1 {
		totalDuration := events[len(events)-1].CreatedAt.Sub(events[0].CreatedAt)
		summary["total_duration_ms"] = totalDuration.Seconds() * 1000
	}

	// Count by event type
	eventCounts := make(map[string]int)
	for _, e := range events {
		eventCounts[e.EventType]++
	}
	summary["event_counts"] = eventCounts

	return summary, nil
}

func (atm *AuditTrailManager) scanEvents(rows *sql.Rows) ([]AuditEvent, error) {
	var events []AuditEvent
	for rows.Next() {
		var event AuditEvent
		var overrides, metadata string
		var createdAt float64

		err := rows.Scan(
			&event.ID, &event.RequestID, &event.ParentID, &event.EventType,
			&event.Category, &event.Method, &event.Path, &event.StatusCode,
			&event.ClientID, &event.AgentType, &event.Model, &event.JobType,
			&event.JobID, &event.Content, &event.Query, &event.Prompt,
			&overrides, &event.DurationMs, &event.TokenCount, &metadata,
			&event.ErrorMessage, &createdAt,
		)
		if err != nil {
			return nil, err
		}

		event.Overrides = make(map[string]interface{})
		json.Unmarshal([]byte(overrides), &event.Overrides)

		event.Metadata = make(map[string]interface{})
		json.Unmarshal([]byte(metadata), &event.Metadata)

		event.CreatedAt = time.Unix(int64(createdAt), 0)
		events = append(events, event)
	}
	return events, nil
}

func generateRequestID() string {
	return fmt.Sprintf("req_%d_%s", time.Now().UnixMilli(), randomHex(8))
}

func randomHex(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = "0123456789abcdef"[time.Now().UnixNano()%16]
		time.Sleep(time.Nanosecond)
	}
	return string(b)
}
