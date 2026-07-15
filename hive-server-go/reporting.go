package main

import (
	"sync"
)

type TokenRecord struct {
	Provider          string  `json:"provider"`
	Model             string  `json:"model"`
	PromptTokens      int     `json:"prompt_tokens"`
	CompletionTokens  int     `json:"completion_tokens"`
	TotalTokens       int     `json:"total_tokens"`
	DurationSeconds   float64 `json:"duration_seconds"`
	TokensPerSecond   float64 `json:"tokens_per_second"`
	Timestamp         float64 `json:"timestamp"`
	JobID             string  `json:"job_id"`
	ClientID          string  `json:"client_id"`
	JobType           string  `json:"job_type"`
}

type UsageReport struct {
	Provider              string  `json:"provider"`
	Model                 string  `json:"model"`
	TotalPrompts          int     `json:"total_prompts"`
	TotalCompletions      int     `json:"total_completions"`
	TotalTokens           int     `json:"total_tokens"`
	TotalDurationSeconds  float64 `json:"total_duration_seconds"`
	AvgTokensPerSecond    float64 `json:"avg_tokens_per_second"`
	AvgDurationSeconds    float64 `json:"avg_duration_seconds"`
	JobCount              int     `json:"job_count"`
}

type UsageTracker struct {
	mu      sync.Mutex
	records []TokenRecord
	maxAge  float64
}

func NewUsageTracker() *UsageTracker {
	return &UsageTracker{
		records: make([]TokenRecord, 0),
		maxAge:  86400 * 7,
	}
}

func (ut *UsageTracker) Record(rec TokenRecord) {
	ut.mu.Lock()
	defer ut.mu.Unlock()
	ut.records = append(ut.records, rec)
	ut.prune()
}

func (ut *UsageTracker) GetReports() []UsageReport {
	ut.mu.Lock()
	defer ut.mu.Unlock()
	ut.prune()

	agg := make(map[string]*UsageReport)
	key := func(p, m string) string { return p + "|" + m }

	for _, r := range ut.records {
		k := key(r.Provider, r.Model)
		if _, ok := agg[k]; !ok {
			agg[k] = &UsageReport{
				Provider: r.Provider,
				Model:    r.Model,
			}
		}
		agg[k].TotalPrompts += r.PromptTokens
		agg[k].TotalCompletions += r.CompletionTokens
		agg[k].TotalTokens += r.TotalTokens
		agg[k].TotalDurationSeconds += r.DurationSeconds
		agg[k].JobCount++
	}

	reports := make([]UsageReport, 0, len(agg))
	for _, r := range agg {
		if r.JobCount > 0 {
			r.AvgTokensPerSecond = float64(r.TotalTokens) / r.TotalDurationSeconds
			r.AvgDurationSeconds = r.TotalDurationSeconds / float64(r.JobCount)
		}
		reports = append(reports, *r)
	}
	return reports
}

func (ut *UsageTracker) GetRecentUsage(limit int) []TokenRecord {
	ut.mu.Lock()
	defer ut.mu.Unlock()

	if limit <= 0 || limit > len(ut.records) {
		limit = len(ut.records)
	}
	out := make([]TokenRecord, limit)
	copy(out, ut.records[len(ut.records)-limit:])
	return out
}

func (ut *UsageTracker) GetSummary() map[string]interface{} {
	reports := ut.GetReports()
	totalPrompts := 0
	totalCompletions := 0
	totalTokens := 0
	totalJobs := 0
	totalDuration := 0.0

	for _, r := range reports {
		totalPrompts += r.TotalPrompts
		totalCompletions += r.TotalCompletions
		totalTokens += r.TotalTokens
		totalJobs += r.JobCount
		totalDuration += r.TotalDurationSeconds
	}

	avgTPS := 0.0
	avgDur := 0.0
	if totalJobs > 0 && totalDuration > 0 {
		avgTPS = float64(totalTokens) / totalDuration
		avgDur = totalDuration / float64(totalJobs)
	}

	return map[string]interface{}{
		"total_prompts":          totalPrompts,
		"total_completions":      totalCompletions,
		"total_tokens":           totalTokens,
		"total_jobs":             totalJobs,
		"total_duration_seconds": totalDuration,
		"avg_tokens_per_second":  avgTPS,
		"avg_duration_seconds":   avgDur,
		"by_provider_model":      reports,
	}
}

func (ut *UsageTracker) prune() {
	cutoff := now() - ut.maxAge
	var kept []TokenRecord
	for _, r := range ut.records {
		if r.Timestamp >= cutoff {
			kept = append(kept, r)
		}
	}
	ut.records = kept
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
		Timestamp:         now(),
		JobID:             job.ID,
		ClientID:          job.ClientID,
		JobType:           job.JobType,
	}
}

var defaultTracker = NewUsageTracker()
