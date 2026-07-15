package main

import (
	"os"
)

type TokenRecord struct {
	Provider          string  `json:"provider"`
	Model             string  `json:"model"`
	PromptTokens      int     `json:"prompt_tokens"`
	CompletionTokens  int     `json:"completion_tokens"`
	TotalTokens       int     `json:"total_tokens"`
	DurationSeconds   float64 `json:"duration_seconds"`
	TokensPerSecond   float64 `json:"tokens_per_second"`
	JobType           string  `json:"job_type"`
	ClientID          string  `json:"client_id"`
	JobID             string  `json:"job_id"`
	ServingNode       string  `json:"serving_node"`
	ServingType       string  `json:"serving_type"`
	Timestamp         float64 `json:"timestamp"`
}

var (
	defaultDB     *DBStore
	servingNodeID string
	servingType   string
)

func initDB() {
	path := os.Getenv("HIVE_DB_PATH")
	if path == "" {
		path = "./hive-server.db"
	}

	db, err := NewDBStore(path)
	if err != nil {
		logError("Failed to open token usage DB: %v", err)
		return
	}
	defaultDB = db
	servingNodeID = getServerID()
	servingType = detectServingType()
	logInfo("Token usage DB: %s (serving_type=%s)", path, servingType)
}

func recordFromResult(job *Job, result interface{}) *TokenRecord {
	m, ok := result.(map[string]interface{})
	if !ok {
		return nil
	}

	model, _ := m["model"].(string)
	if model == "" {
		model = "unknown"
	}

	promptTokens := 0
	if v, ok := m["prompt_eval_count"].(float64); ok {
		promptTokens = int(v)
	}

	completionTokens := 0
	if v, ok := m["eval_count"].(float64); ok {
		completionTokens = int(v)
	}

	if promptTokens == 0 && completionTokens == 0 {
		return nil
	}

	totalTokens := promptTokens + completionTokens
	duration := 0.0
	if job.StartedAt != nil && job.CompletedAt != nil {
		duration = *job.CompletedAt - *job.StartedAt
	}

	tps := 0.0
	if duration > 0 {
		tps = float64(totalTokens) / duration
	}

	return &TokenRecord{
		Provider:          "ollama",
		Model:             model,
		PromptTokens:      promptTokens,
		CompletionTokens:  completionTokens,
		TotalTokens:       totalTokens,
		DurationSeconds:   duration,
		TokensPerSecond:   tps,
		JobType:           job.JobType,
		ClientID:          job.ClientID,
		JobID:             job.ID,
		ServingNode:       servingNodeID,
		ServingType:       servingType,
		Timestamp:         now(),
	}
}
