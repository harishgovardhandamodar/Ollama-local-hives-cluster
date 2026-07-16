package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

type DBStore struct {
	db *sql.DB
}

func NewDBStore(path string) (*DBStore, error) {
	if path == "" {
		path = os.Getenv("HIVE_DB_PATH")
	}
	if path == "" {
		path = "/data/hive-server.db"
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Enable WAL mode for concurrent reads during writes
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		logWarn("Failed to enable WAL mode: %v", err)
	}
	// Busy timeout for concurrent access
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		logWarn("Failed to set busy timeout: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	s := &DBStore{db: db}
	if err := s.migrate(); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return s, nil
}

func (s *DBStore) migrate() error {
	query := `CREATE TABLE IF NOT EXISTS token_usage (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		provider TEXT NOT NULL DEFAULT '',
		model TEXT NOT NULL DEFAULT '',
		prompt_tokens INTEGER NOT NULL DEFAULT 0,
		completion_tokens INTEGER NOT NULL DEFAULT 0,
		total_tokens INTEGER NOT NULL DEFAULT 0,
		duration_seconds REAL NOT NULL DEFAULT 0,
		tokens_per_second REAL NOT NULL DEFAULT 0,
		job_type TEXT NOT NULL DEFAULT '',
		client_id TEXT NOT NULL DEFAULT '',
		job_id TEXT NOT NULL DEFAULT '',
		serving_node TEXT NOT NULL DEFAULT '',
		serving_type TEXT NOT NULL DEFAULT '',
		created_at REAL NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_token_usage_created ON token_usage(created_at);
	CREATE INDEX IF NOT EXISTS idx_token_usage_model ON token_usage(provider, model);

	CREATE TABLE IF NOT EXISTS coding_agent_sessions (
		id TEXT PRIMARY KEY,
		agent_type TEXT NOT NULL DEFAULT '',
		model TEXT NOT NULL DEFAULT '',
		token_budget INTEGER NOT NULL DEFAULT 32000,
		status TEXT NOT NULL DEFAULT 'active',
		system_prompt TEXT NOT NULL DEFAULT '',
		metadata TEXT NOT NULL DEFAULT '{}',
		created_at REAL NOT NULL,
		updated_at REAL NOT NULL,
		total_messages INTEGER NOT NULL DEFAULT 0,
		total_tokens INTEGER NOT NULL DEFAULT 0,
		compressions INTEGER NOT NULL DEFAULT 0
	);
	CREATE INDEX IF NOT EXISTS idx_ca_sessions_agent ON coding_agent_sessions(agent_type);
	CREATE INDEX IF NOT EXISTS idx_ca_sessions_status ON coding_agent_sessions(status);
	CREATE INDEX IF NOT EXISTS idx_ca_sessions_created ON coding_agent_sessions(created_at);

	CREATE TABLE IF NOT EXISTS coding_agent_messages (
		id TEXT PRIMARY KEY,
		session_id TEXT NOT NULL,
		role TEXT NOT NULL,
		content TEXT NOT NULL,
		token_count INTEGER NOT NULL DEFAULT 0,
		compress_group INTEGER NOT NULL DEFAULT 0,
		is_summary INTEGER NOT NULL DEFAULT 0,
		metadata TEXT NOT NULL DEFAULT '{}',
		created_at REAL NOT NULL,
		FOREIGN KEY (session_id) REFERENCES coding_agent_sessions(id)
	);
	CREATE INDEX IF NOT EXISTS idx_ca_msgs_session ON coding_agent_messages(session_id);
	CREATE INDEX IF NOT EXISTS idx_ca_msgs_created ON coding_agent_messages(created_at);
	CREATE INDEX IF NOT EXISTS idx_ca_msgs_role ON coding_agent_messages(role);

	CREATE TABLE IF NOT EXISTS coding_agent_audit (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		session_id TEXT NOT NULL,
		agent_type TEXT NOT NULL DEFAULT '',
		model TEXT NOT NULL DEFAULT '',
		role TEXT NOT NULL,
		content TEXT NOT NULL,
		token_count INTEGER NOT NULL DEFAULT 0,
		compress_group INTEGER NOT NULL DEFAULT 0,
		is_summary INTEGER NOT NULL DEFAULT 0,
		created_at REAL NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_ca_audit_session ON coding_agent_audit(session_id);
	CREATE INDEX IF NOT EXISTS idx_ca_audit_created ON coding_agent_audit(created_at);
	CREATE INDEX IF NOT EXISTS idx_ca_audit_agent ON coding_agent_audit(agent_type);`
	_, err := s.db.ExecContext(context.Background(), query)
	return err
}

func (s *DBStore) Insert(rec TokenRecord) error {
	query := `INSERT INTO token_usage
		(provider, model, prompt_tokens, completion_tokens, total_tokens,
		 duration_seconds, tokens_per_second, job_type, client_id, job_id,
		 serving_node, serving_type, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.ExecContext(context.Background(), query,
		rec.Provider, rec.Model, rec.PromptTokens, rec.CompletionTokens, rec.TotalTokens,
		rec.DurationSeconds, rec.TokensPerSecond, rec.JobType, rec.ClientID, rec.JobID,
		rec.ServingNode, rec.ServingType, rec.Timestamp)
	return err
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
	ServingNode           string  `json:"serving_node"`
	ServingType           string  `json:"serving_type"`
}

func (s *DBStore) GetReports() ([]UsageReport, error) {
	query := `SELECT
		COALESCE(provider, ''), COALESCE(model, ''), COALESCE(serving_node, ''), COALESCE(serving_type, ''),
		COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0),
		COALESCE(SUM(total_tokens), 0), COALESCE(SUM(duration_seconds), 0),
		COUNT(*)
	FROM token_usage
	GROUP BY provider, model, serving_node, serving_type
	ORDER BY SUM(total_tokens) DESC`

	rows, err := s.db.QueryContext(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("query reports: %w", err)
	}
	defer rows.Close()

	var reports []UsageReport
	for rows.Next() {
		var r UsageReport
		if err := rows.Scan(
			&r.Provider, &r.Model, &r.ServingNode, &r.ServingType,
			&r.TotalPrompts, &r.TotalCompletions, &r.TotalTokens,
			&r.TotalDurationSeconds, &r.JobCount,
		); err != nil {
			continue
		}
		if r.JobCount > 0 && r.TotalDurationSeconds > 0 {
			r.AvgTokensPerSecond = float64(r.TotalTokens) / r.TotalDurationSeconds
			r.AvgDurationSeconds = r.TotalDurationSeconds / float64(r.JobCount)
		}
		reports = append(reports, r)
	}
	return reports, rows.Err()
}

func (s *DBStore) GetSummary() (map[string]interface{}, error) {
	reports, err := s.GetReports()
	if err != nil {
		return nil, err
	}

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
	}, nil
}

func (s *DBStore) GetRecent(limit int) ([]TokenRecord, error) {
	query := `SELECT
		COALESCE(provider, ''), COALESCE(model, ''), prompt_tokens, completion_tokens, total_tokens,
		duration_seconds, tokens_per_second,
		COALESCE(job_type, ''), COALESCE(client_id, ''), COALESCE(job_id, ''),
		COALESCE(serving_node, ''), COALESCE(serving_type, ''), created_at
	FROM token_usage
	ORDER BY id DESC LIMIT ?`

	rows, err := s.db.QueryContext(context.Background(), query, limit)
	if err != nil {
		return nil, fmt.Errorf("query recent: %w", err)
	}
	defer rows.Close()

	var records []TokenRecord
	for rows.Next() {
		var rec TokenRecord
		if err := rows.Scan(
			&rec.Provider, &rec.Model, &rec.PromptTokens, &rec.CompletionTokens, &rec.TotalTokens,
			&rec.DurationSeconds, &rec.TokensPerSecond,
			&rec.JobType, &rec.ClientID, &rec.JobID,
			&rec.ServingNode, &rec.ServingType, &rec.Timestamp,
		); err != nil {
			continue
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

type TimeSeriesPoint struct {
	Timestamp float64 `json:"timestamp"`
	TPS       float64 `json:"tps"`
	Model     string  `json:"model"`
}

func (s *DBStore) GetTimeSeries(model string, since float64) ([]TimeSeriesPoint, error) {
	query := `SELECT created_at, tokens_per_second, model FROM token_usage WHERE created_at >= ?`
	args := []interface{}{since}
	if model != "" {
		query += ` AND model = ?`
		args = append(args, model)
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, fmt.Errorf("query timeseries: %w", err)
	}
	defer rows.Close()

	var points []TimeSeriesPoint
	for rows.Next() {
		var p TimeSeriesPoint
		if err := rows.Scan(&p.Timestamp, &p.TPS, &p.Model); err != nil {
			continue
		}
		points = append(points, p)
	}
	return points, rows.Err()
}

type HistogramBin struct {
	MinTPS float64 `json:"min_tps"`
	MaxTPS float64 `json:"max_tps"`
	Count  int     `json:"count"`
}

type HistogramResult struct {
	Bins   []HistogramBin `json:"bins"`
	MinTPS float64        `json:"min_tps"`
	MaxTPS float64        `json:"max_tps"`
	AvgTPS float64        `json:"avg_tps"`
	Median float64        `json:"median_tps"`
	Count  int            `json:"count"`
	Model  string         `json:"model"`
	Since  float64        `json:"since"`
}

func (s *DBStore) GetHistogram(model string, since float64, numBins int) (*HistogramResult, error) {
	args := []interface{}{since}
	query := `SELECT tokens_per_second FROM token_usage WHERE created_at >= ? AND tokens_per_second > 0`
	if model != "" {
		query += ` AND model = ?`
		args = append(args, model)
	}
	query += ` ORDER BY tokens_per_second ASC`

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, fmt.Errorf("query histogram: %w", err)
	}
	defer rows.Close()

	var values []float64
	for rows.Next() {
		var v float64
		if err := rows.Scan(&v); err != nil {
			continue
		}
		values = append(values, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	res := &HistogramResult{
		MinTPS: 0,
		MaxTPS: 0,
		AvgTPS: 0,
		Count:  len(values),
		Model:  model,
		Since:  since,
	}

	if len(values) == 0 {
		res.Bins = make([]HistogramBin, 0)
		return res, nil
	}

	res.MinTPS = values[0]
	res.MaxTPS = values[len(values)-1]

	var sum float64
	for _, v := range values {
		sum += v
	}
	res.AvgTPS = sum / float64(len(values))

	mid := len(values) / 2
	if len(values)%2 == 0 {
		res.Median = (values[mid-1] + values[mid]) / 2
	} else {
		res.Median = values[mid]
	}

	rng := res.MaxTPS - res.MinTPS
	if rng == 0 {
		rng = 1
	}
	binSize := rng / float64(numBins)
	counts := make([]int, numBins)
	var maxCount int
	for _, v := range values {
		idx := int((v - res.MinTPS) / binSize)
		if idx >= numBins {
			idx = numBins - 1
		}
		counts[idx]++
		if counts[idx] > maxCount {
			maxCount = counts[idx]
		}
	}

	res.Bins = make([]HistogramBin, numBins)
	for i := 0; i < numBins; i++ {
		res.Bins[i] = HistogramBin{
			MinTPS: res.MinTPS + float64(i)*binSize,
			MaxTPS: res.MinTPS + float64(i+1)*binSize,
			Count:  counts[i],
		}
	}

	return res, nil
}

func (s *DBStore) GetModels() ([]string, error) {
	rows, err := s.db.QueryContext(context.Background(),
		`SELECT DISTINCT model FROM token_usage ORDER BY model`)
	if err != nil {
		return nil, fmt.Errorf("query models: %w", err)
	}
	defer rows.Close()

	var models []string
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			continue
		}
		models = append(models, m)
	}
	return models, rows.Err()
}

func (s *DBStore) Prune(maxAge float64) error {
	cutoff := now() - maxAge
	_, err := s.db.ExecContext(context.Background(), "DELETE FROM token_usage WHERE created_at < ?", cutoff)
	return err
}

func (s *DBStore) Close() error {
	return s.db.Close()
}

// Coding Agent CRUD methods

func (s *DBStore) InsertCodingAgentSession(session *CodingAgentSession) error {
	metadataJSON := "{}"
	if session.Metadata != nil {
		md, _ := json.Marshal(session.Metadata)
		metadataJSON = string(md)
	}
	query := `INSERT INTO coding_agent_sessions
		(id, agent_type, model, token_budget, status, system_prompt, metadata,
		 created_at, updated_at, total_messages, total_tokens, compressions)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.ExecContext(context.Background(), query,
		session.ID, session.AgentType, session.Model, session.TokenBudget,
		session.Status, session.SystemPrompt, metadataJSON,
		session.CreatedAt, session.UpdatedAt, session.TotalMessages,
		session.TotalTokens, session.Compressions)
	return err
}

func (s *DBStore) GetCodingAgentSession(id string) (*CodingAgentSession, error) {
	query := `SELECT id, agent_type, model, token_budget, status, system_prompt, metadata,
		created_at, updated_at, total_messages, total_tokens, compressions
		FROM coding_agent_sessions WHERE id = ?`
	var session CodingAgentSession
	var metadataJSON string
	err := s.db.QueryRowContext(context.Background(), query, id).Scan(
		&session.ID, &session.AgentType, &session.Model, &session.TokenBudget,
		&session.Status, &session.SystemPrompt, &metadataJSON,
		&session.CreatedAt, &session.UpdatedAt, &session.TotalMessages,
		&session.TotalTokens, &session.Compressions)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if metadataJSON != "" && metadataJSON != "{}" {
		json.Unmarshal([]byte(metadataJSON), &session.Metadata)
	}
	return &session, nil
}

func (s *DBStore) ListCodingAgentSessions(agentType string) ([]*CodingAgentSession, error) {
	query := `SELECT id, agent_type, model, token_budget, status, system_prompt, metadata,
		created_at, updated_at, total_messages, total_tokens, compressions
		FROM coding_agent_sessions`
	args := []interface{}{}
	if agentType != "" {
		query += ` WHERE agent_type = ?`
		args = append(args, agentType)
	}
	query += ` ORDER BY created_at DESC LIMIT 200`

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []*CodingAgentSession
	for rows.Next() {
		var session CodingAgentSession
		var metadataJSON string
		if err := rows.Scan(
			&session.ID, &session.AgentType, &session.Model, &session.TokenBudget,
			&session.Status, &session.SystemPrompt, &metadataJSON,
			&session.CreatedAt, &session.UpdatedAt, &session.TotalMessages,
			&session.TotalTokens, &session.Compressions); err != nil {
			continue
		}
		if metadataJSON != "" && metadataJSON != "{}" {
			json.Unmarshal([]byte(metadataJSON), &session.Metadata)
		}
		sessions = append(sessions, &session)
	}
	return sessions, rows.Err()
}

func (s *DBStore) UpdateCodingAgentSession(session *CodingAgentSession) error {
	query := `UPDATE coding_agent_sessions SET
		status = ?, total_messages = ?, total_tokens = ?, compressions = ?, updated_at = ?
		WHERE id = ?`
	_, err := s.db.ExecContext(context.Background(), query,
		session.Status, session.TotalMessages, session.TotalTokens,
		session.Compressions, session.UpdatedAt, session.ID)
	return err
}

func (s *DBStore) DeleteCodingAgentSession(id string) error {
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(context.Background(),
		`DELETE FROM coding_agent_messages WHERE session_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(context.Background(),
		`DELETE FROM coding_agent_audit WHERE session_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(context.Background(),
		`DELETE FROM coding_agent_sessions WHERE id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// Coding Agent Messages CRUD

func (s *DBStore) InsertCodingAgentMessage(msg *CodingAgentMessage) error {
	isSummary := 0
	if msg.IsSummary {
		isSummary = 1
	}
	if msg.MetadataJSON == "" {
		msg.MetadataJSON = "{}"
	}
	query := `INSERT INTO coding_agent_messages
		(id, session_id, role, content, token_count, compress_group, is_summary, metadata, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.ExecContext(context.Background(), query,
		msg.ID, msg.SessionID, msg.Role, msg.Content, msg.TokenCount,
		msg.CompressGroup, isSummary, msg.MetadataJSON, msg.CreatedAt)
	return err
}

func (s *DBStore) GetCodingAgentMessages(sessionID string, limit int) ([]*CodingAgentMessage, error) {
	query := `SELECT id, session_id, role, content, token_count, compress_group, is_summary, metadata, created_at
		FROM coding_agent_messages WHERE session_id = ? ORDER BY created_at ASC`
	args := []interface{}{sessionID}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*CodingAgentMessage
	for rows.Next() {
		var msg CodingAgentMessage
		var isSummary int
		if err := rows.Scan(&msg.ID, &msg.SessionID, &msg.Role, &msg.Content,
			&msg.TokenCount, &msg.CompressGroup, &isSummary, &msg.MetadataJSON, &msg.CreatedAt); err != nil {
			continue
		}
		msg.IsSummary = isSummary == 1
		msgs = append(msgs, &msg)
	}
	return msgs, rows.Err()
}

func (s *DBStore) CompressCodingAgentMessages(sessionID string, splitIdx int, summaryContent string, compressGroup int) error {
	msgs, err := s.GetCodingAgentMessages(sessionID, 0)
	if err != nil {
		return err
	}

	if splitIdx > len(msgs) {
		splitIdx = len(msgs)
	}
	if splitIdx <= 1 {
		return nil // don't remove system message
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete old messages (keep system message at index 0 and recent messages)
	for i := 1; i < splitIdx; i++ {
		if _, err := tx.ExecContext(context.Background(),
			`DELETE FROM coding_agent_messages WHERE id = ?`, msgs[i].ID); err != nil {
			return err
		}
	}

	// Insert summary message
	summaryMsg := &CodingAgentMessage{
		ID:           fmt.Sprintf("msg-%s-summary-%d", sessionID, time.Now().UnixMilli()),
		SessionID:    sessionID,
		Role:         "system",
		Content:      summaryContent,
		TokenCount:   estimateTokens(summaryContent),
		CompressGroup: compressGroup,
		IsSummary:    true,
		MetadataJSON: `{"type":"context_summary"}`,
		CreatedAt:    now(),
	}
	if _, err := tx.ExecContext(context.Background(),
		`INSERT INTO coding_agent_messages (id, session_id, role, content, token_count, compress_group, is_summary, metadata, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		summaryMsg.ID, summaryMsg.SessionID, summaryMsg.Role, summaryMsg.Content,
		summaryMsg.TokenCount, summaryMsg.CompressGroup, 1, summaryMsg.MetadataJSON, summaryMsg.CreatedAt); err != nil {
		return err
	}

	return tx.Commit()
}

// Coding Agent Audit Log

func (s *DBStore) InsertCodingAgentAuditEntry(entry *CodingAgentAuditEntry) error {
	isSummary := 0
	if entry.IsSummary {
		isSummary = 1
	}
	query := `INSERT INTO coding_agent_audit
		(session_id, agent_type, model, role, content, token_count, compress_group, is_summary, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.ExecContext(context.Background(), query,
		entry.SessionID, entry.AgentType, entry.Model, entry.Role,
		entry.Content, entry.TokenCount, entry.CompressGroup, isSummary, entry.CreatedAt)
	return err
}

func (s *DBStore) GetCodingAgentAuditLogs(sessionID string, limit int) ([]*CodingAgentAuditEntry, error) {
	if limit <= 0 {
		limit = 500
	}
	query := `SELECT id, session_id, agent_type, model, role, content, token_count, compress_group, is_summary, created_at
		FROM coding_agent_audit WHERE session_id = ? ORDER BY id DESC LIMIT ?`

	rows, err := s.db.QueryContext(context.Background(), query, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*CodingAgentAuditEntry
	for rows.Next() {
		var entry CodingAgentAuditEntry
		var isSummary int
		if err := rows.Scan(&entry.ID, &entry.SessionID, &entry.AgentType, &entry.Model,
			&entry.Role, &entry.Content, &entry.TokenCount, &entry.CompressGroup,
			&isSummary, &entry.CreatedAt); err != nil {
			continue
		}
		entry.IsSummary = isSummary == 1
		entries = append(entries, &entry)
	}
	return entries, rows.Err()
}

func (s *DBStore) SearchCodingAgentMessages(query string, limit int) ([]*CodingAgentMessage, error) {
	if limit <= 0 {
		limit = 50
	}
	q := `SELECT id, session_id, role, content, token_count, compress_group, is_summary, metadata, created_at
		FROM coding_agent_messages WHERE content LIKE ? ORDER BY created_at DESC LIMIT ?`
	searchPattern := "%" + query + "%"

	rows, err := s.db.QueryContext(context.Background(), q, searchPattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*CodingAgentMessage
	for rows.Next() {
		var msg CodingAgentMessage
		var isSummary int
		if err := rows.Scan(&msg.ID, &msg.SessionID, &msg.Role, &msg.Content,
			&msg.TokenCount, &msg.CompressGroup, &isSummary, &msg.MetadataJSON, &msg.CreatedAt); err != nil {
			continue
		}
		msg.IsSummary = isSummary == 1
		msgs = append(msgs, &msg)
	}
	return msgs, rows.Err()
}

func detectServingType() string {
	if v := os.Getenv("SERVING_TYPE"); v != "" {
		return v
	}
	if _, err := os.Stat("/dev/nvidia0"); err == nil {
		return "GPU"
	}
	if _, err := os.Stat("/dev/dri/renderD128"); err == nil {
		return "GPU"
	}
	if len(detectNvidiaGPU()) > 0 {
		return "GPU"
	}
	return "CPU"
}
