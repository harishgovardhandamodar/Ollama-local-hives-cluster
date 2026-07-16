package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"time"
)

type JobStatus string

const (
	JobPending   JobStatus = "pending"
	JobRunning   JobStatus = "running"
	JobCompleted JobStatus = "completed"
	JobFailed    JobStatus = "failed"
)

type JobPriority int

const (
	PriorityLow    JobPriority = 0
	PriorityNormal JobPriority = 1
	PriorityHigh   JobPriority = 2
	PriorityMax    JobPriority = 3
)

type Job struct {
	ID            string                 `json:"job_id"`
	ClientID      string                 `json:"client_id"`
	JobType       string                 `json:"job_type"`
	Payload       []byte                 `json:"-"`
	PayloadMap    map[string]interface{} `json:"payload"`
	Status        JobStatus              `json:"status"`
	Result        interface{}            `json:"result,omitempty"`
	Error         string                 `json:"error,omitempty"`
	CreatedAt     float64                `json:"created_at"`
	StartedAt     *float64               `json:"started_at,omitempty"`
	CompletedAt   *float64               `json:"completed_at,omitempty"`
	QueuePosition int                    `json:"queue_position"`
	Model         string                 `json:"model,omitempty"`
	PromptTokens  int                    `json:"prompt_tokens,omitempty"`
	EvalTokens    int                    `json:"completion_tokens,omitempty"`
	TotalTokens   int                    `json:"total_tokens,omitempty"`
	EvalDuration  float64                `json:"-"`
	Priority      JobPriority            `json:"priority"`
}

func NewJob(id, clientID, jobType string, payload map[string]interface{}) *Job {
	// Assign priority based on job type
	var priority JobPriority
	switch jobType {
	case "coding_agent_chat", "chat":
		priority = PriorityHigh
	case "generate", "embed", "get_embedding":
		priority = PriorityNormal
	default:
		priority = PriorityLow
	}

	return &Job{
		ID:         id,
		ClientID:   clientID,
		JobType:    jobType,
		PayloadMap: payload,
		Status:     JobPending,
		CreatedAt:  now(),
		Priority:   priority,
	}
}

func now() float64 {
	return float64(time.Now().UnixMilli()) / 1000
}

type OllamaQueue struct {
	jobCh         chan *Job
	stopCh        chan struct{}
	wg            sync.WaitGroup

	mu            sync.RWMutex
	running       map[string]*Job
	completed     map[string]*Job
	maxConcurrent int
	ollamaURL     string
	ollamaModel   string

	// Priority queues
	priorityQueues [4]chan *Job // indexed by JobPriority

	httpClient    *http.Client
	httpClientMu  sync.Mutex
}

func NewOllamaQueue(maxConcurrent int, ollamaURL, ollamaModel string) *OllamaQueue {
	q := &OllamaQueue{
		jobCh:         make(chan *Job, 1000),
		stopCh:        make(chan struct{}),
		running:       make(map[string]*Job),
		completed:     make(map[string]*Job),
		maxConcurrent: maxConcurrent,
		ollamaURL:     ollamaURL,
		ollamaModel:   ollamaModel,
		httpClient: &http.Client{
			Timeout: 600 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        maxConcurrent + 5,
				MaxIdleConnsPerHost: maxConcurrent + 5,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}

	// Initialize priority queues with different capacities
	q.priorityQueues[PriorityLow] = make(chan *Job, 200)
	q.priorityQueues[PriorityNormal] = make(chan *Job, 500)
	q.priorityQueues[PriorityHigh] = make(chan *Job, 200)
	q.priorityQueues[PriorityMax] = make(chan *Job, 100)

	return q
}

func (q *OllamaQueue) Start() {
	for i := 0; i < q.maxConcurrent; i++ {
		q.wg.Add(1)
		go q.worker(i)
	}
}

func (q *OllamaQueue) Stop() {
	close(q.stopCh)
	q.wg.Wait()
}

func (q *OllamaQueue) Submit(job *Job) bool {
	// Use priority queue if available
	if int(job.Priority) < len(q.priorityQueues) {
		select {
		case q.priorityQueues[job.Priority] <- job:
			return true
		default:
			// Priority queue full, try main queue
		}
	}

	select {
	case q.jobCh <- job:
		return true
	default:
		return false
	}
}

func (q *OllamaQueue) worker(id int) {
	defer q.wg.Done()
	for {
		select {
		case <-q.stopCh:
			return
		default:
			// Try high priority first, then normal, then low
			var job *Job
			for i := PriorityMax; i >= PriorityLow; i-- {
				select {
				case job = <-q.priorityQueues[i]:
					goto process
				default:
					continue
				}
			}

			// Try main queue
			select {
			case job = <-q.jobCh:
			case <-q.stopCh:
				return
			}

		process:
			if job == nil {
				continue
			}

			job.Status = JobRunning
			now := now()
			job.StartedAt = &now

			q.mu.Lock()
			q.running[job.ID] = job
			q.mu.Unlock()

			q.executeJob(job)

			q.mu.Lock()
			delete(q.running, job.ID)
			q.completed[job.ID] = job
			q.mu.Unlock()

			// Notify subscribers
			NotifyJobUpdate(job)

			// Track metrics
			if job.StartedAt != nil && job.CompletedAt != nil {
				duration := *job.CompletedAt - *job.StartedAt
				globalMetrics.RecordJobDuration(duration)
			}
			globalMetrics.RecordTokenCount(job.TotalTokens)
			if job.TotalTokens > 0 && job.StartedAt != nil && job.CompletedAt != nil {
				duration := *job.CompletedAt - *job.StartedAt
				if duration > 0 {
					globalMetrics.RecordTPS(float64(job.TotalTokens) / duration)
				}
			}
			if job.Status == JobCompleted {
				globalMetrics.IncrJobsCompleted()
			} else {
				globalMetrics.IncrJobsFailed()
			}
		}
	}
}

var resultPool = sync.Pool{
	New: func() interface{} {
		m := make(map[string]interface{}, 16)
		return m
	},
}

func (q *OllamaQueue) executeJob(job *Job) {
	var result interface{}
	var err error

	switch job.JobType {
	case "generate":
		result, err = q.callOllamaGenerate(job.PayloadMap)
	case "chat":
		result, err = q.callOllamaChat(job.PayloadMap)
	case "coding_agent_chat":
		result, err = q.callOllamaChatPayload(job.PayloadMap)
	case "embed", "get_embedding":
		result, err = q.callOllamaEmbed(job.PayloadMap)
	case "list_models":
		result, err = q.callOllamaListModels()
	case "pull_model":
		result, err = q.callOllamaPullModel(job.PayloadMap)
	case "custom_prompt", "generate_digest", "generate_help_noob", "generate_illustration", "generate_textbook_map", "analyze_paper", "find_cross_relations", "categorize_papers", "extract_abstract_primitives", "extract_concept_graph", "zero_shot_compare", "extract_structured_graph", "advanced_overlap_analyze":
		result, err = q.callOllamaGeneric(job.PayloadMap, job.JobType)
	default:
		result, err = q.callOllamaGeneric(job.PayloadMap, job.JobType)
	}

	now := now()
	job.CompletedAt = &now
	if err != nil {
		job.Status = JobFailed
		job.Error = err.Error()
		job.Result = nil
		logError("Job %s failed: %v", job.ID, err)
	} else {
		job.Status = JobCompleted
		job.Result = result
		parseTokenMetrics(job, result)
		if rec := recordFromResult(job, result); rec != nil && defaultDB != nil {
			insertDB(defaultDB, rec)
		}
		logInfo("Job %s completed: type=%s tokens=%d tps=%.1f", job.ID, job.JobType, job.TotalTokens, calcTPS(job))
	}
}

func calcTPS(job *Job) float64 {
	if job.StartedAt != nil && job.CompletedAt != nil {
		dur := *job.CompletedAt - *job.StartedAt
		if dur > 0 && job.TotalTokens > 0 {
			return float64(job.TotalTokens) / dur
		}
	}
	if job.EvalDuration > 0 && job.EvalTokens > 0 {
		return float64(job.EvalTokens) / (job.EvalDuration / 1e9)
	}
	return 0
}

func insertDB(db *DBStore, rec *TokenRecord) {
	go func() {
		if err := db.Insert(*rec); err != nil {
			logError("Failed to record token usage: %v", err)
		}
	}()

}

func (q *OllamaQueue) callOllamaGenerate(payload map[string]interface{}) (interface{}, error) {
	model := q.ollamaModel
	if m, ok := payload["model"].(string); ok && m != "" {
		model = m
	}
	prompt, _ := payload["prompt"].(string)
	system, _ := payload["system"].(string)
	stream := false
	if s, ok := payload["stream"].(bool); ok {
		stream = s
	}
	temperature := 0.1
	if t, ok := payload["temperature"].(float64); ok {
		temperature = t
	}

	body := map[string]interface{}{
		"model":       model,
		"prompt":      prompt,
		"stream":      stream,
		"temperature": temperature,
	}
	if system != "" {
		body["system"] = system
	}

	return q.postOllama("/api/generate", body)
}

func (q *OllamaQueue) callOllamaChat(payload map[string]interface{}) (interface{}, error) {
	model := q.ollamaModel
	if m, ok := payload["model"].(string); ok && m != "" {
		model = m
	}
	messages, _ := payload["messages"].([]interface{})
	stream := false
	if s, ok := payload["stream"].(bool); ok {
		stream = s
	}

	body := map[string]interface{}{
		"model":    model,
		"messages": messages,
		"stream":   stream,
	}
	return q.postOllama("/api/chat", body)
}

// callOllamaChatPayload sends a pre-built chat payload directly to Ollama /api/chat
// Used by coding_agent_chat jobs where the payload is already in the correct format
func (q *OllamaQueue) callOllamaChatPayload(payload map[string]interface{}) (interface{}, error) {
	// Payload already has model, messages, stream in the right shape
	body := map[string]interface{}{
		"model":    payload["model"],
		"messages": payload["messages"],
		"stream":   false,
	}
	return q.postOllama("/api/chat", body)
}

func (q *OllamaQueue) callOllamaEmbed(payload map[string]interface{}) (interface{}, error) {
	model := "nomic-embed-text"
	if m, ok := payload["model"].(string); ok && m != "" {
		model = m
	}
	prompt, _ := payload["prompt"].(string)
	if prompt == "" {
		prompt, _ = payload["text"].(string)
	}
	body := map[string]interface{}{
		"model":  model,
		"prompt": prompt,
	}
	return q.postOllama("/api/embeddings", body)
}

func (q *OllamaQueue) callOllamaListModels() (interface{}, error) {
	return q.getOllama("/api/tags")
}

func (q *OllamaQueue) callOllamaPullModel(payload map[string]interface{}) (interface{}, error) {
	name, _ := payload["name"].(string)
	if name == "" {
		return nil, fmt.Errorf("model name required")
	}
	body := map[string]interface{}{
		"name":   name,
		"stream": false,
	}
	return q.postOllama("/api/pull", body)
}

func (q *OllamaQueue) callOllamaCustom(payload map[string]interface{}) (interface{}, error) {
	path, _ := payload["path"].(string)
	if path == "" {
		path = "/api/generate"
	}
	delete(payload, "path")
	return q.postOllama(path, payload)
}

func (q *OllamaQueue) callOllamaGeneric(payload map[string]interface{}, jobType string) (interface{}, error) {
	text, _ := payload["text"].(string)
	if text == "" {
		if t, ok := payload["prompt"].(string); ok {
			text = t
		}
	}
	prompt := fmt.Sprintf("Job type: %s\n\nPayload: %s\n\nPlease process this request and return a structured JSON response.", jobType, truncPayload(payload))
	if text != "" {
		prompt = fmt.Sprintf("Job type: %s\n\nText:\n%s\n\nAnalyze the above text and return a structured JSON response according to the job type.", jobType, truncStr(text, 16000))
	}
	body := map[string]interface{}{
		"model":       q.ollamaModel,
		"prompt":      prompt,
		"stream":      false,
		"temperature": 0.1,
	}
	resp, err := q.postOllama("/api/generate", body)
	if err != nil {
		return nil, err
	}
	respMap, ok := resp.(map[string]interface{})
	if ok {
		extracted := extractJSONFromResponse(respMap)

		if ec, ok := respMap["eval_count"].(float64); ok {
			extracted["eval_count"] = ec
		}
		if pd, ok := respMap["prompt_eval_count"].(float64); ok {
			extracted["prompt_eval_count"] = pd
		}
		if ed, ok := respMap["eval_duration"].(float64); ok {
			extracted["eval_duration"] = ed
		}
		if m, ok := respMap["model"].(string); ok {
			extracted["model"] = m
		}

		return extracted, nil
	}
	return resp, nil
}

func truncPayload(payload map[string]interface{}) string {
	cleaned := make(map[string]interface{})
	for k, v := range payload {
		if s, ok := v.(string); ok && len(s) > 200 {
			cleaned[k] = truncStr(s, 200)
		} else {
			cleaned[k] = v
		}
	}
	data, _ := json.Marshal(cleaned)
	return string(data)
}

func truncStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

func extractJSONFromResponse(resp map[string]interface{}) map[string]interface{} {
	response, _ := resp["response"].(string)
	if response == "" {
		return resp
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(response), &parsed); err == nil {
		return parsed
	}
	rePatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?s)` + "```json\\s*(\\{.*?\\})\\s*```"),
		regexp.MustCompile(`(?s)` + "```\\s*(\\{.*?\\})\\s*```"),
		regexp.MustCompile(`(?s)(\{.*\})`),
	}
	for _, pat := range rePatterns {
		matches := pat.FindStringSubmatch(response)
		if len(matches) > 1 {
			if err := json.Unmarshal([]byte(matches[1]), &parsed); err == nil {
				return parsed
			}
		}
	}
	resp["raw_response"] = response
	return resp
}

func (q *OllamaQueue) postOllama(path string, body map[string]interface{}) (interface{}, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}
	url := q.ollamaURL + path
	resp, err := q.getHTTPClient().Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("ollama request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var errResp map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("ollama error (status %d): %v", resp.StatusCode, errResp)
	}
	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("ollama decode failed: %w", err)
	}
	return result, nil
}

func (q *OllamaQueue) getHTTPClient() *http.Client {
	q.httpClientMu.Lock()
	defer q.httpClientMu.Unlock()
	return q.httpClient
}

func (q *OllamaQueue) getOllama(path string) (interface{}, error) {
	url := q.ollamaURL + path
	resp, err := q.getHTTPClient().Get(url)
	if err != nil {
		return nil, fmt.Errorf("ollama request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ollama error status %d", resp.StatusCode)
	}
	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("ollama decode failed: %w", err)
	}
	return result, nil
}

func (q *OllamaQueue) GetQueueStatus() map[string]interface{} {
	q.mu.RLock()
	defer q.mu.RUnlock()
	recentCompleted := 0
	for _, j := range q.completed {
		if j.CompletedAt != nil && now()-(*j.CompletedAt) < 3600 {
			recentCompleted++
		}
	}
	return map[string]interface{}{
		"pending":          len(q.jobCh),
		"running":          len(q.running),
		"completed_recent": recentCompleted,
		"max_concurrent":   q.maxConcurrent,
		"ollama_url":       q.ollamaURL,
		"ollama_model":     q.ollamaModel,
	}
}

func (q *OllamaQueue) GetAvailableCapacity() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return max(0, q.maxConcurrent-len(q.running))
}

func (q *OllamaQueue) GetJob(jobID string) *Job {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if j, ok := q.running[jobID]; ok {
		return j
	}
	if j, ok := q.completed[jobID]; ok {
		return j
	}
	return nil
}

func (q *OllamaQueue) GetAllJobs() []map[string]interface{} {
	q.mu.RLock()
	defer q.mu.RUnlock()
	jobs := make([]map[string]interface{}, 0, len(q.running)+len(q.completed))
	for _, j := range q.running {
		jobs = append(jobs, jobToMap(j))
	}
	for _, j := range q.completed {
		if j.CompletedAt != nil && now()-(*j.CompletedAt) < 3600 {
			jobs = append(jobs, jobToMap(j))
		}
	}
	return jobs
}

func jobToMap(j *Job) map[string]interface{} {
	m := map[string]interface{}{
		"job_id":         j.ID,
		"client_id":      j.ClientID,
		"job_type":       j.JobType,
		"payload":        j.PayloadMap,
		"status":         string(j.Status),
		"result":         j.Result,
		"error":          j.Error,
		"created_at":     j.CreatedAt,
		"started_at":     j.StartedAt,
		"completed_at":   j.CompletedAt,
		"queue_position": 0,
	}

	if j.Model != "" {
		m["model"] = j.Model
	}

	pt := j.PromptTokens
	ct := j.EvalTokens
	if pt > 0 {
		m["prompt_tokens"] = pt
	}
	if ct > 0 {
		m["completion_tokens"] = ct
	}
	if pt > 0 || ct > 0 {
		m["total_tokens"] = pt + ct
	}

	tps := calcTPS(j)
	if tps > 0 {
		m["tokens_per_second"] = tps
	}

	if j.StartedAt != nil && j.CompletedAt != nil {
		m["duration_seconds"] = *j.CompletedAt - *j.StartedAt
	}

	return m
}

func (q *OllamaQueue) CleanupOldJobs(maxAgeHours int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	cutoff := now() - float64(maxAgeHours*3600)
	for id, j := range q.completed {
		if j.CompletedAt != nil && *j.CompletedAt < cutoff {
			delete(q.completed, id)
		}
	}
}

func parseTokenMetrics(job *Job, result interface{}) {
	resMap, ok := result.(map[string]interface{})
	if !ok {
		return
	}
	if m, _ := resMap["model"].(string); m != "" {
		job.Model = m
	}
	if pc, ok := resMap["prompt_eval_count"].(float64); ok {
		job.PromptTokens = int(pc)
	}
	if ec, ok := resMap["eval_count"].(float64); ok {
		job.EvalTokens = int(ec)
	}
	job.TotalTokens = job.PromptTokens + job.EvalTokens
	if ed, ok := resMap["eval_duration"].(float64); ok {
		job.EvalDuration = ed
	}

	if job.PromptTokens == 0 && job.EvalTokens == 0 {
		if usage, ok := resMap["usage"].(map[string]interface{}); ok {
			if pt, ok := usage["prompt_tokens"].(float64); ok {
				job.PromptTokens = int(pt)
			}
			if ct, ok := usage["completion_tokens"].(float64); ok {
				job.EvalTokens = int(ct)
			}
			job.TotalTokens = job.PromptTokens + job.EvalTokens
		}
	}
}
