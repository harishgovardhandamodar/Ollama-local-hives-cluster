package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OpenAI-compatible request/response types

type OpenAIChatRequest struct {
	Model       string              `json:"model"`
	Messages    []OpenAIMessage     `json:"messages"`
	Stream      bool                `json:"stream"`
	Temperature *float64            `json:"temperature,omitempty"`
	MaxTokens   *int                `json:"max_tokens,omitempty"`
	TopP        *float64            `json:"top_p,omitempty"`
	Tools       []interface{}       `json:"tools,omitempty"`
	ToolChoice  interface{}         `json:"tool_choice,omitempty"`
}

type OpenAIMessage struct {
	Role       string      `json:"role"`
	Content    string      `json:"content"`
	ToolCalls  interface{} `json:"tool_calls,omitempty"`
	ToolCallID string      `json:"tool_call_id,omitempty"`
}

type OpenAIChatResponse struct {
	ID                string             `json:"id"`
	Object            string             `json:"object"`
	Created           int64              `json:"created"`
	Model             string             `json:"model"`
	Choices           []OpenAIChoice     `json:"choices"`
	Usage             *OpenAIUsage       `json:"usage,omitempty"`
	SystemFingerprint string             `json:"system_fingerprint,omitempty"`
}

type OpenAIChoice struct {
	Index        int          `json:"index"`
	Message      OpenAIMessage `json:"message"`
	FinishReason string       `json:"finish_reason"`
}

type OpenAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type OpenAIModelList struct {
	Object string         `json:"object"`
	Data   []OpenAIModel  `json:"data"`
}

type OpenAIModel struct {
	ID       string `json:"id"`
	Object   string `json:"object"`
	Created  int64  `json:"created"`
	OwnedBy  string `json:"owned_by"`
}

// handleOpenAIChatCompletions handles POST /v1/chat/completions
// This is the main entry point for Hermes, Codex, and other OpenAI-compatible agents
func (hs *HiveServer) handleOpenAIChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var req OpenAIChatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if len(req.Messages) == 0 {
		http.Error(w, "messages required", http.StatusBadRequest)
		return
	}

	// Determine model — use request model or server default
	model := req.Model
	if model == "" {
		model = hs.cfg.OllamaModel
	}

	// Extract system prompt and conversation messages
	var systemPrompt string
	var conversationMsgs []map[string]string

	for _, m := range req.Messages {
		role := strings.ToLower(m.Role)
		if role == "system" && systemPrompt == "" {
			systemPrompt = m.Content
			continue
		}
		// Map OpenAI roles to Ollama roles
		if role == "developer" {
			role = "system"
		}
		if m.Content != "" {
			conversationMsgs = append(conversationMsgs, map[string]string{
				"role":    role,
				"content": m.Content,
			})
		}
	}

	// If no explicit system prompt, use a default for coding
	if systemPrompt == "" {
		systemPrompt = "You are a helpful coding assistant. Write clean, production-ready code."
	}

	// Use coding agent session if available, otherwise fall through to direct queue
	if hs.codingAgent != nil {
		resp, usage, err := hs.handleOpenAIViaAgent(model, systemPrompt, conversationMsgs, req)
		if err != nil {
			// Fall through to direct queue on error
			logWarn("OpenAI proxy agent failed, falling back to direct: %v", err)
		} else {
			writeJSON(w, resp)
			_ = usage // usage is embedded in response
			return
		}
	}

	// Direct queue fallback — no session management, just proxy to Ollama
	response, usage, err := hs.handleOpenAIDirect(model, conversationMsgs)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":{"message":"%s","type":"server_error"}}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// Build OpenAI-format response
	resp := OpenAIChatResponse{
		ID:      fmt.Sprintf("chatcmpl-hive-%d", time.Now().UnixMilli()),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []OpenAIChoice{
			{
				Index: 0,
				Message: OpenAIMessage{
					Role:    "assistant",
					Content: response,
				},
				FinishReason: "stop",
			},
		},
		Usage: usage,
	}

	writeJSON(w, resp)
}

// handleOpenAIViaAgent routes through the coding agent session for context management
func (hs *HiveServer) handleOpenAIViaAgent(model, systemPrompt string, msgs []map[string]string, req OpenAIChatRequest) (*OpenAIChatResponse, *OpenAIUsage, error) {
	// Create a session for this conversation
	agentType := "custom"
	if strings.Contains(model, "hermes") || strings.Contains(model, "Hermes") {
		agentType = "hermes"
	} else if strings.Contains(model, "codex") || strings.Contains(model, "Codex") {
		agentType = "codex"
	} else if strings.Contains(model, "opencode") || strings.Contains(model, "OpenCode") {
		agentType = "opencode"
	}

	session, _, err := hs.codingAgent.CreateSession(agentType, model, systemPrompt, 0, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("create session: %w", err)
	}

	// Replay conversation messages into the session
	for _, m := range msgs {
		role := m["role"]
		content := m["content"]
		if role == "" {
			role = "user"
		}
		// Skip system messages (already set as session system prompt)
		if role == "system" {
			continue
		}
		if _, _, err := hs.codingAgent.SendMessage(session.ID, role, content, nil); err != nil {
			logWarn("Failed to replay message into session %s: %v", session.ID, err)
		}
	}

	// Get the last response (the one we want to return)
	auditLogs, err := hs.codingAgent.GetAuditLogs(session.ID, 5)
	if err != nil || len(auditLogs) == 0 {
		return nil, nil, fmt.Errorf("no response in session")
	}

	// Find the last assistant response
	var lastResponse string
	for i := len(auditLogs) - 1; i >= 0; i-- {
		if auditLogs[i].Role == "assistant" {
			lastResponse = auditLogs[i].Content
			break
		}
	}

	if lastResponse == "" {
		return nil, nil, fmt.Errorf("no assistant response found")
	}

	// Get context stats for usage
	stats, _ := hs.codingAgent.GetContextStats(session.ID)
	usage := &OpenAIUsage{
		PromptTokens:     session.TotalTokens - estimateTokens(lastResponse),
		CompletionTokens: estimateTokens(lastResponse),
		TotalTokens:      session.TotalTokens,
	}
	if stats != nil {
		if pt, ok := stats["total_tokens"].(int); ok {
			usage.TotalTokens = pt
			usage.PromptTokens = pt - usage.CompletionTokens
		}
	}

	resp := &OpenAIChatResponse{
		ID:      fmt.Sprintf("chatcmpl-hive-%d", time.Now().UnixMilli()),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   model,
		Choices: []OpenAIChoice{
			{
				Index: 0,
				Message: OpenAIMessage{
					Role:    "assistant",
					Content: lastResponse,
				},
				FinishReason: "stop",
			},
		},
		Usage: usage,
	}

	return resp, usage, nil
}

// handleOpenAIDirect proxies directly to Ollama without session management
func (hs *HiveServer) handleOpenAIDirect(model string, msgs []map[string]string) (string, *OpenAIUsage, error) {
	// Build Ollama chat payload
	ollamaBody := map[string]interface{}{
		"model":    model,
		"messages": msgs,
		"stream":   false,
	}

	data, err := json.Marshal(ollamaBody)
	if err != nil {
		return "", nil, err
	}

	// Submit as a regular chat job for queue visibility
	jobID := fmt.Sprintf("openai:%d", time.Now().UnixMilli())
	jobPayload := map[string]interface{}{
		"model":    model,
		"messages": msgs,
		"stream":   false,
	}

	job := NewJob(jobID, "openai-compat", "chat", jobPayload)
	if !hs.queue.Submit(job) {
		// Queue full — try forwarding to a less-loaded peer
		if hs.mesh != nil {
			peer := hs.mesh.GetBestPeer()
			if peer != nil {
				logInfo("OpenAI queue full, forwarding to peer %s (load=%.1f)", peer.ServerID, peer.Load)
				resp, usage, err := hs.forwardOpenAIToPeer(peer, model, msgs)
				if err == nil {
					return resp, usage, nil
				}
				logWarn("Forward to %s failed: %v, trying direct Ollama", peer.ServerID, err)
			}
		}
		// All peers full or unreachable — call Ollama directly
		logWarn("Queue full for openai-compat job %s, calling Ollama directly", jobID)
		return hs.callOllamaDirect(model, data)
	}

	// Wait for completion
	deadline := time.After(600 * time.Second)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return "", nil, fmt.Errorf("job timeout")
		case <-ticker.C:
			j := hs.queue.GetJob(jobID)
			if j == nil {
				continue
			}
			if j.Status == JobFailed {
				return "", nil, fmt.Errorf("ollama error: %s", j.Error)
			}
			if j.Status == JobCompleted {
				response := extractChatResponse(j.Result)
				usage := &OpenAIUsage{
					PromptTokens:     j.PromptTokens,
					CompletionTokens: j.EvalTokens,
					TotalTokens:      j.TotalTokens,
				}
				return response, usage, nil
			}
		}
	}
}

// forwardOpenAIToPeer sends an OpenAI chat completion request to a peer and waits for the response
func (hs *HiveServer) forwardOpenAIToPeer(peer *PeerInfo, model string, msgs []map[string]string) (string, *OpenAIUsage, error) {
	// Apply model mapping for cross-platform mesh (e.g. MLX -> NVIDIA)
	forwardModel := model
	if hs.mesh != nil {
		forwardModel = hs.mesh.MapModel(model)
	}

	payload := map[string]interface{}{
		"model":    forwardModel,
		"messages": msgs,
		"stream":   false,
	}

	body := map[string]interface{}{
		"job_id":    fmt.Sprintf("mesh-openai:%d", time.Now().UnixMilli()),
		"client_id": "mesh-openai",
		"job_type":  "chat",
		"payload":   payload,
		"origin":    getServerID(),
	}

	data, _ := json.Marshal(body)
	client := &http.Client{Timeout: 600 * time.Second}
	resp, err := client.Post(peer.Endpoint+"/api/jobs/forward", "application/json", bytes.NewReader(data))
	if err != nil {
		return "", nil, fmt.Errorf("forward request failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Status string      `json:"status"`
		Result interface{} `json:"result"`
		Error  string      `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", nil, fmt.Errorf("decode forward response failed: %w", err)
	}

	if result.Status == "failed" {
		return "", nil, fmt.Errorf("peer execution failed: %s", result.Error)
	}

	response := extractChatResponse(result.Result)
	usage := &OpenAIUsage{} // token metrics not available from peer forwarding
	return response, usage, nil
}

// callOllamaDirect calls Ollama as a last resort
func (hs *HiveServer) callOllamaDirect(model string, data []byte) (string, *OpenAIUsage, error) {
	client := &http.Client{Timeout: 600 * time.Second}
	resp, err := client.Post(hs.cfg.OllamaURL+"/api/chat", "application/json", bytes.NewReader(data))
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Message struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
		EvalCount   float64 `json:"eval_count"`
		PromptEval  float64 `json:"prompt_eval_count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", nil, err
	}

	usage := &OpenAIUsage{
		PromptTokens:     int(result.PromptEval),
		CompletionTokens: int(result.EvalCount),
		TotalTokens:      int(result.PromptEval + result.EvalCount),
	}

	return result.Message.Content, usage, nil
}

// handleOpenAIListModels handles GET /v1/models
// Returns models from local Ollama + all mesh peers so Hermes /model sees everything
func (hs *HiveServer) handleOpenAIListModels(w http.ResponseWriter, r *http.Request) {
	models := make(map[string]OpenAIModel)
	modelOrder := make([]string, 0)

	// 1. Get live models from local Ollama
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(hs.cfg.OllamaURL + "/api/tags")
	if err == nil {
		defer resp.Body.Close()
		var result struct {
			Models []struct {
				Name string `json:"name"`
			} `json:"models"`
		}
		if json.NewDecoder(resp.Body).Decode(&result) == nil {
			for _, m := range result.Models {
				models[m.Name] = OpenAIModel{
					ID:      m.Name,
					Object:  "model",
					Created: time.Now().Unix(),
					OwnedBy: "local",
				}
				modelOrder = append(modelOrder, m.Name)
			}
		}
	}

	// 2. Fetch models from all alive mesh peers
	if hs.mesh != nil {
		peers := hs.mesh.GetAlivePeers()
		for _, peer := range peers {
			peerModels := hs.fetchPeerModels(peer.Endpoint)
			for _, pm := range peerModels {
				// Apply reverse model map so mapped names appear correctly
				// e.g. if this Mac maps gemma4:31b-mlx->gemma4:31b when forwarding,
				//       and peer has gemma4:31b, show it as gemma4:31b (already correct)
				//       but if peer has gemma4:31b-mlx and we map it, show the mapped name
				if _, exists := models[pm]; !exists {
					models[pm] = OpenAIModel{
						ID:      pm,
						Object:  "model",
						Created: time.Now().Unix(),
						OwnedBy: peer.ServerID,
					}
					modelOrder = append(modelOrder, pm)
				}
			}
		}
	}

	// 3. Ensure default model is always listed
	if _, exists := models[hs.cfg.OllamaModel]; !exists {
		models[hs.cfg.OllamaModel] = OpenAIModel{
			ID:      hs.cfg.OllamaModel,
			Object:  "model",
			Created: time.Now().Unix(),
			OwnedBy: "local",
		}
		modelOrder = append(modelOrder, hs.cfg.OllamaModel)
	}

	// 4. Build response in insertion order
	result := make([]OpenAIModel, 0, len(models))
	for _, name := range modelOrder {
		if m, ok := models[name]; ok {
			result = append(result, m)
		}
	}

	writeJSON(w, OpenAIModelList{
		Object: "list",
		Data:   result,
	})
}

// fetchPeerModels queries a peer's /v1/models and returns the model names
func (hs *HiveServer) fetchPeerModels(peerEndpoint string) []string {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(peerEndpoint + "/v1/models")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil
	}

	var names []string
	for _, m := range result.Data {
		if m.ID != "" {
			names = append(names, m.ID)
		}
	}
	return names
}

// handleOpenAIHealth handles GET /v1/health (non-standard but useful)
func (hs *HiveServer) handleOpenAIHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]interface{}{
		"status":  "ok",
		"server":  "hive-server-go",
		"version": serverVersion,
	})
}
