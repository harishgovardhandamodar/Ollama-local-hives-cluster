package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Agent types supported by the coding agent API
const (
	AgentTypeOpenCode = "opencode"
	AgentTypeHermes   = "hermes"
	AgentTypeCodex    = "codex"
	AgentTypeCustom   = "custom"
)

// Session status
const (
	SessionActive   = "active"
	SessionArchived = "archived"
)

// Default context budget in tokens (approximate)
const (
	DefaultTokenBudget   = 32000
	MinTokenBudget       = 4096
	MaxTokenBudget       = 200000
	CompressThresholdPct = 0.85
	RecentMessageKeep    = 6
	SummarySystemPrefix  = "context_summary"
)

// CodingAgentSession represents a persistent coding agent session
type CodingAgentSession struct {
	ID            string                 `json:"session_id"`
	AgentType     string                 `json:"agent_type"`
	Model         string                 `json:"model"`
	TokenBudget   int                    `json:"token_budget"`
	Status        string                 `json:"status"`
	SystemPrompt  string                 `json:"system_prompt,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt     float64                `json:"created_at"`
	UpdatedAt     float64                `json:"updated_at"`
	TotalMessages int                    `json:"total_messages"`
	TotalTokens   int                    `json:"total_tokens"`
	Compressions  int                    `json:"compressions"`
}

// CodingAgentMessage represents a single message in a session
type CodingAgentMessage struct {
	ID              string  `json:"id"`
	SessionID       string  `json:"session_id"`
	Role            string  `json:"role"`
	Content         string  `json:"content"`
	TokenCount      int     `json:"token_count"`
	CompressGroup   int     `json:"compress_group"`
	IsSummary       bool    `json:"is_summary"`
	MetadataJSON    string  `json:"metadata,omitempty"`
	CreatedAt       float64 `json:"created_at"`
}

// CodingAgentAuditEntry logs every prompt/response for traceability
type CodingAgentAuditEntry struct {
	ID            int64   `json:"id"`
	SessionID     string  `json:"session_id"`
	AgentType     string  `json:"agent_type"`
	Model         string  `json:"model"`
	Role          string  `json:"role"`
	Content       string  `json:"content"`
	TokenCount    int     `json:"token_count"`
	CompressGroup int     `json:"compress_group"`
	IsSummary     bool    `json:"is_summary"`
	CreatedAt     float64 `json:"created_at"`
}

// CodingAgentManager manages coding agent sessions and context
type CodingAgentManager struct {
	db       *DBStore
	queue    *OllamaQueue
	mesh     *MeshDiscovery
	detector *ModelContextDetector
	serverID string
	mu       sync.RWMutex
	sessions map[string]*CodingAgentSession
}

// NewCodingAgentManager creates a new coding agent manager
func NewCodingAgentManager(db *DBStore, queue *OllamaQueue, mesh *MeshDiscovery, serverID string) *CodingAgentManager {
	return &CodingAgentManager{
		db:       db,
		queue:    queue,
		mesh:     mesh,
		detector: NewModelContextDetector(queue.ollamaURL),
		serverID: serverID,
		sessions: make(map[string]*CodingAgentSession),
	}
}

// CreateSession creates a new coding agent session
func (cam *CodingAgentManager) CreateSession(agentType, model, systemPrompt string, tokenBudget int, metadata map[string]interface{}) (*CodingAgentSession, *ModelContextWindow, error) {
	if model == "" {
		model = cam.queue.ollamaModel
	}

	// Auto-detect token budget from model if not explicitly set
	detected := cam.detector.DetectContextWindow(model)
	tokenBudget = cam.detector.GetContextBudgetForModel(model, tokenBudget)

	session := &CodingAgentSession{
		ID:           fmt.Sprintf("ca-%s-%d", agentType, time.Now().UnixMilli()),
		AgentType:    agentType,
		Model:        model,
		TokenBudget:  tokenBudget,
		Status:       SessionActive,
		SystemPrompt: systemPrompt,
		Metadata:     metadata,
		CreatedAt:    now(),
		UpdatedAt:    now(),
	}

	if err := cam.db.InsertCodingAgentSession(session); err != nil {
		return nil, nil, fmt.Errorf("insert session: %w", err)
	}

	// Add system prompt as first message if provided
	if systemPrompt != "" {
		msg := &CodingAgentMessage{
			ID:        fmt.Sprintf("msg-%s-sys-%d", session.ID, time.Now().UnixMilli()),
			SessionID: session.ID,
			Role:      "system",
			Content:   systemPrompt,
			TokenCount: estimateTokens(systemPrompt),
			CreatedAt: now(),
		}
		if err := cam.db.InsertCodingAgentMessage(msg); err != nil {
			logError("Failed to insert system message: %v", err)
		}
		session.TotalMessages++
		session.TotalTokens += msg.TokenCount
	}

	cam.mu.Lock()
	cam.sessions[session.ID] = session
	cam.mu.Unlock()

	logInfo("Coding agent session created: %s (agent=%s, model=%s, budget=%d, detected_ctx=%s, source=%s)",
		session.ID, agentType, model, tokenBudget,
		FormatContextWindow(detected.ContextLength), detected.Source)
	return session, detected, nil
}

// GetSession retrieves a session by ID
func (cam *CodingAgentManager) GetSession(sessionID string) (*CodingAgentSession, error) {
	cam.mu.RLock()
	if s, ok := cam.sessions[sessionID]; ok {
		cam.mu.RUnlock()
		return s, nil
	}
	cam.mu.RUnlock()

	s, err := cam.db.GetCodingAgentSession(sessionID)
	if err != nil {
		return nil, err
	}
	if s != nil {
		cam.mu.Lock()
		cam.sessions[s.ID] = s
		cam.mu.Unlock()
	}
	return s, nil
}

// ListSessions returns all sessions, optionally filtered by agent type
func (cam *CodingAgentManager) ListSessions(agentType string) ([]*CodingAgentSession, error) {
	return cam.db.ListCodingAgentSessions(agentType)
}

// GetMessages returns all messages for a session in chronological order
func (cam *CodingAgentManager) GetMessages(sessionID string, limit int) ([]*CodingAgentMessage, error) {
	return cam.db.GetCodingAgentMessages(sessionID, limit)
}

// SendMessage adds a user/assistant message to a session and returns the assistant response
// This is the core endpoint for coding agent chat
func (cam *CodingAgentManager) SendMessage(sessionID, role, content string, metadata map[string]interface{}) (string, *CodingAgentSession, error) {
	session, err := cam.GetSession(sessionID)
	if err != nil || session == nil {
		return "", nil, fmt.Errorf("session not found: %s", sessionID)
	}

	if session.Status != SessionActive {
		return "", nil, fmt.Errorf("session is archived: %s", sessionID)
	}

	// Persist the incoming message
	inMsg := &CodingAgentMessage{
		ID:        fmt.Sprintf("msg-%s-%d", sessionID, time.Now().UnixMilli()),
		SessionID: sessionID,
		Role:      role,
		Content:   content,
		TokenCount: estimateTokens(content),
		CreatedAt: now(),
	}
	if metadata != nil {
		md, _ := json.Marshal(metadata)
		inMsg.MetadataJSON = string(md)
	}
	if err := cam.db.InsertCodingAgentMessage(inMsg); err != nil {
		return "", nil, fmt.Errorf("insert message: %w", err)
	}
	session.TotalMessages++
	session.TotalTokens += inMsg.TokenCount

	// Audit log the incoming message
	cam.auditLog(session, role, content, inMsg.TokenCount, inMsg.CompressGroup, false)

	// Check if context compression is needed
	compressed, err := cam.maybeCompress(session)
	if err != nil {
		logWarn("Context compression failed for session %s: %v", sessionID, err)
	}

	// Build messages for Ollama
	messages, err := cam.buildOllamaMessages(session)
	if err != nil {
		return "", nil, fmt.Errorf("build messages: %w", err)
	}

	// ── Submit to job queue for visibility ──────────────────────────
	// Build the Ollama chat payload so the queue worker can execute it
	jobID := fmt.Sprintf("agent:%s:%d", sessionID, time.Now().UnixMilli())
	jobPayload := map[string]interface{}{
		"model":    session.Model,
		"messages": messages,
		"stream":   false,
		"source":   "coding_agent",
		"session_id": sessionID,
	}

	job := NewJob(jobID, "coding-agent", "coding_agent_chat", jobPayload)
	if !cam.queue.Submit(job) {
		// Queue full — try forwarding to a less-loaded peer
		if cam.mesh != nil {
			peer := cam.mesh.GetBestPeer()
			if peer != nil {
				logInfo("Agent queue full, forwarding %s to peer %s (load=%.1f)", jobID, peer.ServerID, peer.Load)
				response, err := cam.forwardToPeer(peer, session.Model, messages)
				if err == nil {
					return cam.finishMessage(session, inMsg, response, compressed)
				}
				logWarn("Forward to %s failed: %v, trying direct Ollama", peer.ServerID, err)
			}
		}
		// All peers full or unreachable — fall back to direct call
		logWarn("Queue full for agent job %s, calling Ollama directly", jobID)
		response, err := cam.callOllamaChat(session.Model, messages)
		if err != nil {
			return "", nil, fmt.Errorf("ollama call failed: %w", err)
		}
		return cam.finishMessage(session, inMsg, response, compressed)
	}

	// Wait for the queue worker to pick it up and complete
	deadline := time.After(600 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return "", nil, fmt.Errorf("job timeout: %s", jobID)
		case <-ticker.C:
			j := cam.queue.GetJob(jobID)
			if j == nil {
				continue
			}
			if j.Status == JobFailed {
				return "", nil, fmt.Errorf("ollama call failed: %s", j.Error)
			}
			if j.Status == JobCompleted {
				// Extract the response from the queue job result
				response := extractChatResponse(j.Result)
				return cam.finishMessage(session, inMsg, response, compressed)
			}
		}
	}
}

// finishMessage persists the assistant response, updates session stats, and returns
func (cam *CodingAgentManager) finishMessage(session *CodingAgentSession, inMsg *CodingAgentMessage, response string, compressed bool) (string, *CodingAgentSession, error) {
	// Persist the assistant response
	outMsg := &CodingAgentMessage{
		ID:        fmt.Sprintf("msg-%s-%d", session.ID, time.Now().UnixMilli()),
		SessionID: session.ID,
		Role:      "assistant",
		Content:   response,
		TokenCount: estimateTokens(response),
		CreatedAt: now(),
	}
	if err := cam.db.InsertCodingAgentMessage(outMsg); err != nil {
		logError("Failed to insert assistant message: %v", err)
	}
	session.TotalMessages++
	session.TotalTokens += outMsg.TokenCount
	session.UpdatedAt = now()

	// Audit log the response
	cam.auditLog(session, "assistant", response, outMsg.TokenCount, outMsg.CompressGroup, false)

	// Update session stats
	if compressed {
		session.Compressions++
	}
	cam.db.UpdateCodingAgentSession(session)

	// Update in-memory cache
	cam.mu.Lock()
	cam.sessions[session.ID] = session
	cam.mu.Unlock()

	logInfo("Coding agent message: session=%s role=%s tokens=%d (compressed=%v)", session.ID, inMsg.Role, inMsg.TokenCount, compressed)
	return response, session, nil
}

// forwardToPeer sends a chat completion request to a peer and waits for the response.
// The peer executes the inference on its local Ollama while session state stays here.
func (cam *CodingAgentManager) forwardToPeer(peer *PeerInfo, model string, messages []map[string]string) (string, error) {
	// Use the peer's /api/jobs/forward endpoint which blocks until completion
	payload := map[string]interface{}{
		"model":    model,
		"messages": messages,
		"stream":   false,
		"source":   "coding_agent",
	}

	body := map[string]interface{}{
		"job_id":    fmt.Sprintf("mesh-agent:%d", time.Now().UnixMilli()),
		"client_id": "mesh-agent",
		"job_type":  "coding_agent_chat",
		"payload":   payload,
		"origin":    cam.serverID,
	}

	data, _ := json.Marshal(body)
	client := &http.Client{Timeout: 600 * time.Second}
	resp, err := client.Post(peer.Endpoint+"/api/jobs/forward", "application/json", bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("forward request failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Status string      `json:"status"`
		Result interface{} `json:"result"`
		Error  string      `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode forward response failed: %w", err)
	}

	if result.Status == "failed" {
		return "", fmt.Errorf("peer execution failed: %s", result.Error)
	}

	return extractChatResponse(result.Result), nil
}

// extractChatResponse pulls the response text from a queue job result
func extractChatResponse(result interface{}) string {
	if result == nil {
		return ""
	}
	m, ok := result.(map[string]interface{})
	if !ok {
		return fmt.Sprintf("%v", result)
	}
	// Try Ollama chat format: { "message": { "content": "..." } }
	if msg, ok := m["message"].(map[string]interface{}); ok {
		if content, ok := msg["content"].(string); ok {
			return content
		}
	}
	// Try generate format: { "response": "..." }
	if resp, ok := m["response"].(string); ok {
		return resp
	}
	// Try raw_response (from extractJSONFromResponse fallback)
	if raw, ok := m["raw_response"].(string); ok {
		return raw
	}
	return fmt.Sprintf("%v", result)
}

// buildOllamaMessages constructs the message array for Ollama from DB messages
func (cam *CodingAgentManager) buildOllamaMessages(session *CodingAgentSession) ([]map[string]string, error) {
	msgs, err := cam.db.GetCodingAgentMessages(session.ID, 0) // 0 = no limit
	if err != nil {
		return nil, err
	}

	var ollamaMsgs []map[string]string
	for _, m := range msgs {
		ollamaMsgs = append(ollamaMsgs, map[string]string{
			"role":    m.Role,
			"content": m.Content,
		})
	}
	return ollamaMsgs, nil
}

// callOllamaChat calls the Ollama chat API directly (bypasses job queue for low latency)
func (cam *CodingAgentManager) callOllamaChat(model string, messages []map[string]string) (string, error) {
	body := map[string]interface{}{
		"model":    model,
		"messages": messages,
		"stream":   false,
	}

	data, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	url := cam.queue.ollamaURL + "/api/chat"
	client := &http.Client{Timeout: 600 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("ollama request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&errResp)
		return "", fmt.Errorf("ollama error (status %d): %v", resp.StatusCode, errResp)
	}

	var result struct {
		Message struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
		Response    string  `json:"response"`
		EvalCount   float64 `json:"eval_count"`
		PromptEval  float64 `json:"prompt_eval_count"`
		EvalDur     float64 `json:"eval_duration"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("ollama decode failed: %w", err)
	}

	if result.Message.Content != "" {
		return result.Message.Content, nil
	}
	return result.Response, nil
}

// maybeCompress checks if context needs compression and compresses if so
func (cam *CodingAgentManager) maybeCompress(session *CodingAgentSession) (bool, error) {
	threshold := int(float64(session.TokenBudget) * CompressThresholdPct)
	if session.TotalTokens <= threshold {
		return false, nil
	}

	logInfo("Context compression triggered for session %s: tokens=%d threshold=%d", session.ID, session.TotalTokens, threshold)
	return true, cam.compressContext(session)
}

// compressContext performs sliding-window context compression
// Keeps the most recent N messages and summarizes the rest
func (cam *CodingAgentManager) compressContext(session *CodingAgentSession) error {
	msgs, err := cam.db.GetCodingAgentMessages(session.ID, 0)
	if err != nil {
		return err
	}

	if len(msgs) <= RecentMessageKeep+2 {
		return nil // too few messages to compress
	}

	// Find the split point: keep system message + recent messages
	splitIdx := 0
	keepCount := 0
	for i, m := range msgs {
		if m.Role == "system" && !m.IsSummary {
			// always keep the original system prompt at position 0
			splitIdx = i + 1
			continue
		}
		keepCount++
		if keepCount >= RecentMessageKeep {
			splitIdx = i
			break
		}
	}

	if splitIdx <= 1 {
		return nil // nothing to compress
	}

	// Collect messages to summarize (skip system message at index 0)
	toSummarize := msgs[1:splitIdx]
	if len(toSummarize) == 0 {
		return nil
	}

	// Build summary prompt
	var summaryParts []string
	for _, m := range toSummarize {
		role := m.Role
		if m.IsSummary {
			role = "context_summary"
		}
		summaryParts = append(summaryParts, fmt.Sprintf("[%s]: %s", role, truncStr(m.Content, 2000)))
	}

	summaryPrompt := fmt.Sprintf(`You are a context compression assistant for a coding workflow. Summarize the following conversation history into a concise context summary. Preserve:
- Key decisions and their rationale
- File names, function names, variable names mentioned
- Error messages and their solutions
- Important code snippets or patterns
- Current task/goal state

Conversation to summarize:
%s

Return ONLY the summary text, no JSON or markdown. Keep it under 1500 characters.`, strings.Join(summaryParts, "\n"))

	// Call Ollama for summary
	summaryBody := map[string]interface{}{
		"model":       session.Model,
		"prompt":      summaryPrompt,
		"stream":      false,
		"temperature": 0.1,
	}

	data, _ := json.Marshal(summaryBody)
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Post(cam.queue.ollamaURL+"/api/generate", "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("summary generation failed: %w", err)
	}
	defer resp.Body.Close()

	var summaryResult struct {
		Response string `json:"response"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&summaryResult); err != nil {
		return fmt.Errorf("summary decode failed: %w", err)
	}

	if summaryResult.Response == "" {
		return fmt.Errorf("empty summary response")
	}

	// Delete old messages and insert summary
	compressGroup := msgs[splitIdx-1].CompressGroup + 1
	if err := cam.db.CompressCodingAgentMessages(session.ID, splitIdx, summaryResult.Response, compressGroup); err != nil {
		return fmt.Errorf("compress messages: %w", err)
	}

	// Audit log the compression
	cam.auditLog(session, "system", "[Context compressed] "+truncStr(summaryResult.Response, 500), estimateTokens(summaryResult.Response), compressGroup, true)

	// Recalculate token count
	msgs2, err := cam.db.GetCodingAgentMessages(session.ID, 0)
	if err == nil {
		total := 0
		for _, m := range msgs2 {
			total += m.TokenCount
		}
		session.TotalTokens = total
	}

	logInfo("Context compressed for session %s: removed %d messages, summary=%d chars", session.ID, len(toSummarize), len(summaryResult.Response))
	return nil
}

// ArchiveSession archives a session
func (cam *CodingAgentManager) ArchiveSession(sessionID string) error {
	s, err := cam.GetSession(sessionID)
	if err != nil || s == nil {
		return fmt.Errorf("session not found: %s", sessionID)
	}
	s.Status = SessionArchived
	s.UpdatedAt = now()
	return cam.db.UpdateCodingAgentSession(s)
}

// DeleteSession permanently removes a session and all its messages
func (cam *CodingAgentManager) DeleteSession(sessionID string) error {
	cam.mu.Lock()
	delete(cam.sessions, sessionID)
	cam.mu.Unlock()
	return cam.db.DeleteCodingAgentSession(sessionID)
}

// GetAuditLogs returns audit log entries for a session
func (cam *CodingAgentManager) GetAuditLogs(sessionID string, limit int) ([]*CodingAgentAuditEntry, error) {
	return cam.db.GetCodingAgentAuditLogs(sessionID, limit)
}

// SearchMessages searches for messages containing a query string
func (cam *CodingAgentManager) SearchMessages(query string, limit int) ([]*CodingAgentMessage, error) {
	return cam.db.SearchCodingAgentMessages(query, limit)
}

// GetContextStats returns context usage statistics for a session
func (cam *CodingAgentManager) GetContextStats(sessionID string) (map[string]interface{}, error) {
	session, err := cam.GetSession(sessionID)
	if err != nil || session == nil {
		return nil, fmt.Errorf("session not found: %s", sessionID)
	}

	msgs, err := cam.db.GetCodingAgentMessages(sessionID, 0)
	if err != nil {
		return nil, err
	}

	totalTokens := 0
	summaryCount := 0
	roleBreakdown := map[string]int{}
	for _, m := range msgs {
		totalTokens += m.TokenCount
		roleBreakdown[m.Role] += m.TokenCount
		if m.IsSummary {
			summaryCount++
		}
	}

	budgetUsed := float64(totalTokens) / float64(session.TokenBudget) * 100

	// Detect the model's full context window
	detected := cam.detector.DetectContextWindow(session.Model)

	return map[string]interface{}{
		"session_id":        sessionID,
		"token_budget":      session.TokenBudget,
		"total_tokens":      totalTokens,
		"budget_used_pct":   budgetUsed,
		"total_messages":    len(msgs),
		"summary_messages":  summaryCount,
		"compressions":      session.Compressions,
		"role_breakdown":    roleBreakdown,
		"needs_compression": budgetUsed >= float64(CompressThresholdPct*100),
		"model_context": map[string]interface{}{
			"model":          session.Model,
			"context_length": detected.ContextLength,
			"detection_source": detected.Source,
			"budget_ratio":   fmt.Sprintf("%d%%", int(float64(session.TokenBudget)/float64(detected.ContextLength)*100)),
		},
	}, nil
}

// auditLog writes an entry to the persistent audit log
func (cam *CodingAgentManager) auditLog(session *CodingAgentSession, role, content string, tokenCount, compressGroup int, isSummary bool) {
	entry := &CodingAgentAuditEntry{
		SessionID:     session.ID,
		AgentType:     session.AgentType,
		Model:         session.Model,
		Role:          role,
		Content:       content,
		TokenCount:    tokenCount,
		CompressGroup: compressGroup,
		IsSummary:     isSummary,
		CreatedAt:     now(),
	}
	if err := cam.db.InsertCodingAgentAuditEntry(entry); err != nil {
		logError("Failed to write audit log: %v", err)
	}
}

// estimateTokens provides a rough token count for a string
// Uses ~4 chars per token heuristic (works well for English code/text)
func estimateTokens(s string) int {
	if len(s) == 0 {
		return 0
	}
	// Rough heuristic: 1 token ≈ 4 characters for English text/code
	tokens := len(s) / 4
	if tokens < 1 {
		tokens = 1
	}
	return tokens
}
