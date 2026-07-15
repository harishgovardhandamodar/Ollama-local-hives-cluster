package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

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
		path = "/app/hive-server.db"
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

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
	CREATE INDEX IF NOT EXISTS idx_token_usage_model ON token_usage(provider, model);`
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
