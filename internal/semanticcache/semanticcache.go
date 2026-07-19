package semanticcache

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type CacheConfig struct {
	Enabled             bool              `yaml:"enabled" json:"enabled"`
	MaxEntries          int               `yaml:"max_entries" json:"max_entries"`
	TTLSeconds          int               `yaml:"ttl_seconds" json:"ttl_seconds"`
	SimilarityThreshold float64           `yaml:"similarity_threshold" json:"similarity_threshold"`
	EmbeddingModel      string            `yaml:"embedding_model" json:"embedding_model"`
	ModelEmbeddingMap   map[string]string `yaml:"model_embedding_map,omitempty" json:"model_embedding_map,omitempty"`
	OllamaURL           string            `yaml:"-" json:"-"`
	IndexType           string            `yaml:"index_type,omitempty" json:"index_type,omitempty"`
	EmbeddingCacheSize  int               `yaml:"embedding_cache_size,omitempty" json:"embedding_cache_size,omitempty"`
	WarmupFile          string            `yaml:"warmup_file,omitempty" json:"warmup_file,omitempty"`
}

func DefaultConfig() CacheConfig {
	return CacheConfig{
		Enabled:            false,
		MaxEntries:         1000,
		TTLSeconds:         300,
		SimilarityThreshold: 0.92,
		EmbeddingModel:     "nomic-embed-text",
		ModelEmbeddingMap:  make(map[string]string),
		IndexType:          "lsh",
		EmbeddingCacheSize: 5000,
	}
}

type CacheEntry struct {
	ID          string    `json:"id"`
	Key         string    `json:"key"`
	Model       string    `json:"model"`
	Prompt      string    `json:"prompt"`
	Response    string    `json:"response"`
	Embedding   []float32 `json:"-"`
	HitCount    int       `json:"hit_count"`
	CreatedAt   time.Time `json:"created_at"`
	LastHitAt   time.Time `json:"last_hit_at"`
	TTLSeconds  int       `json:"ttl_seconds"`
	Metadata    string    `json:"metadata,omitempty"`
}

type CacheStats struct {
	Entries             int     `json:"entries"`
	MaxEntries          int     `json:"max_entries"`
	Hits                int64   `json:"hits"`
	Misses              int64   `json:"misses"`
	HitRate             float64 `json:"hit_rate"`
	TTLSeconds          int     `json:"ttl_seconds"`
	SimilarityThreshold float64 `json:"similarity_threshold"`
	EmbeddingModel      string  `json:"embedding_model"`
	TotalSavedTokens    int64   `json:"total_saved_tokens"`
	AvgResponseSize     float64 `json:"avg_response_size"`
	IndexType           string  `json:"index_type"`
	IndexSize           int     `json:"index_size"`
	EmbeddingCacheHits  int64   `json:"embedding_cache_hits"`
	EmbeddingCacheMisses int64  `json:"embedding_cache_misses"`
}

type SemanticCache struct {
	mu         sync.RWMutex
	entries    []*CacheEntry
	embeddings map[string][]float32
	config     CacheConfig
	db         *sql.DB
	hits       int64
	misses     int64
	totalSavedTokens int64
	stopCh     chan struct{}
	index      VectorIndex
	embeddingCache     *EmbeddingCache
	embeddingCacheHits int64
	embeddingCacheMisses int64
}

type EmbeddingCache struct {
	mu      sync.RWMutex
	cache   map[string][]float32
	maxSize int
	order   []string
}

func NewEmbeddingCache(maxSize int) *EmbeddingCache {
	return &EmbeddingCache{
		cache:   make(map[string][]float32),
		maxSize: maxSize,
	}
}

func (ec *EmbeddingCache) Get(key string) ([]float32, bool) {
	ec.mu.RLock()
	defer ec.mu.RUnlock()
	val, ok := ec.cache[key]
	return val, ok
}

func (ec *EmbeddingCache) Set(key string, embedding []float32) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	if _, exists := ec.cache[key]; exists {
		return
	}

	if len(ec.cache) >= ec.maxSize && len(ec.order) > 0 {
		oldest := ec.order[0]
		ec.order = ec.order[1:]
		delete(ec.cache, oldest)
	}

	ec.cache[key] = embedding
	ec.order = append(ec.order, key)
}

func (ec *EmbeddingCache) Len() int {
	ec.mu.RLock()
	defer ec.mu.RUnlock()
	return len(ec.cache)
}

func (ec *EmbeddingCache) Clear() {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.cache = make(map[string][]float32)
	ec.order = nil
}

type WarmupEntry struct {
	Model    string `json:"model"`
	Prompt   string `json:"prompt"`
	Response string `json:"response"`
}

func New(config CacheConfig, db *sql.DB) *SemanticCache {
	cache := &SemanticCache{
		entries:    make([]*CacheEntry, 0, config.MaxEntries),
		embeddings: make(map[string][]float32),
		config:     config,
		db:         db,
		stopCh:     make(chan struct{}),
		embeddingCache: NewEmbeddingCache(config.EmbeddingCacheSize),
	}

	switch config.IndexType {
	case "lsh":
		dim := 768
		cache.index = NewLSHIndex(dim)
	default:
		cache.index = NewFlatIndex()
	}

	if db != nil {
		if err := cache.migrate(); err != nil {
			log.Printf("[semantic-cache] Failed to migrate database: %v", err)
		}
		if err := cache.loadFromDB(); err != nil {
			log.Printf("[semantic-cache] Failed to load cache from database: %v", err)
		}
	}

	if config.WarmupFile != "" {
		if err := cache.WarmFromFile(config.WarmupFile); err != nil {
			log.Printf("[semantic-cache] Failed to warm cache from file: %v", err)
		}
	}

	go cache.evictionLoop()
	return cache
}

func (c *SemanticCache) migrate() error {
	query := `CREATE TABLE IF NOT EXISTS semantic_cache (
		id TEXT PRIMARY KEY,
		key TEXT NOT NULL,
		model TEXT NOT NULL,
		prompt TEXT NOT NULL,
		response TEXT NOT NULL,
		embedding BLOB,
		hit_count INTEGER NOT NULL DEFAULT 0,
		created_at REAL NOT NULL,
		last_hit_at REAL NOT NULL,
		ttl_seconds INTEGER NOT NULL DEFAULT 300,
		metadata TEXT DEFAULT ''
	);
	CREATE INDEX IF NOT EXISTS idx_sc_key ON semantic_cache(key);
	CREATE INDEX IF NOT EXISTS idx_sc_model ON semantic_cache(model);
	CREATE INDEX IF NOT EXISTS idx_sc_created ON semantic_cache(created_at);`

	_, err := c.db.ExecContext(context.Background(), query)
	return err
}

func (c *SemanticCache) loadFromDB() error {
	query := `SELECT id, key, model, prompt, response, embedding, hit_count, 
		created_at, last_hit_at, ttl_seconds, COALESCE(metadata, '')
		FROM semantic_cache ORDER BY created_at DESC`

	rows, err := c.db.QueryContext(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var entry CacheEntry
		var embeddingBytes []byte
		var createdAt, lastHitAt float64

		if err := rows.Scan(
			&entry.ID, &entry.Key, &entry.Model, &entry.Prompt, &entry.Response,
			&embeddingBytes, &entry.HitCount, &createdAt, &lastHitAt,
			&entry.TTLSeconds, &entry.Metadata,
		); err != nil {
			continue
		}

		entry.CreatedAt = time.Unix(0, int64(createdAt*1e9))
		entry.LastHitAt = time.Unix(0, int64(lastHitAt*1e9))

		if len(embeddingBytes) > 0 {
			embedding, err := deserializeEmbedding(embeddingBytes)
			if err == nil {
				entry.Embedding = embedding
				c.embeddings[entry.ID] = embedding
				c.index.Insert(entry.ID, entry.Model, embedding)
			}
		}

		c.entries = append(c.entries, &entry)
		count++
	}

	log.Printf("[semantic-cache] Loaded %d entries from database", count)
	return rows.Err()
}

func (c *SemanticCache) resolveEmbeddingModel(model string) string {
	if c.config.ModelEmbeddingMap != nil {
		if em, ok := c.config.ModelEmbeddingMap[model]; ok {
			return em
		}
	}
	return c.config.EmbeddingModel
}

func (c *SemanticCache) Get(ctx context.Context, model string, prompt string, messages []map[string]string) (*CacheEntry, bool) {
	if !c.config.Enabled {
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()

	for _, entry := range c.entries {
		if entry.Model != model {
			continue
		}

		if now.Sub(entry.CreatedAt) > time.Duration(entry.TTLSeconds)*time.Second {
			continue
		}

		if entry.Key == c.generateKey(model, prompt, messages) {
			entry.HitCount++
			entry.LastHitAt = now
			c.hits++
			return entry, true
		}
	}

	if c.config.SimilarityThreshold > 0 {
		queryEmbedding, err := c.getEmbedding(ctx, model, prompt)
		if err == nil && queryEmbedding != nil {
			results := c.index.Search(queryEmbedding, 1, model, func(id string) bool {
				for _, e := range c.entries {
					if e.ID == id {
						return now.Sub(e.CreatedAt) <= time.Duration(e.TTLSeconds)*time.Second
					}
				}
				return false
			})
			if len(results) > 0 && results[0].Score >= c.config.SimilarityThreshold {
				for _, e := range c.entries {
					if e.ID == results[0].ID {
						e.HitCount++
						e.LastHitAt = now
						c.hits++
						return e, true
					}
				}
			}
		}
	}

	c.misses++
	return nil, false
}

func (c *SemanticCache) Set(ctx context.Context, model string, prompt string, messages []map[string]string, response string, metadata string) {
	if !c.config.Enabled {
		return
	}

	entry := &CacheEntry{
		ID:         generateID(),
		Key:        c.generateKey(model, prompt, messages),
		Model:      model,
		Prompt:     prompt,
		Response:   response,
		HitCount:   0,
		CreatedAt:  time.Now(),
		LastHitAt:  time.Now(),
		TTLSeconds: c.config.TTLSeconds,
		Metadata:   metadata,
	}

	queryEmbedding, err := c.getEmbedding(ctx, model, prompt)
	if err == nil && queryEmbedding != nil {
		entry.Embedding = queryEmbedding
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) >= c.config.MaxEntries {
		c.evictOldest()
	}

	c.entries = append(c.entries, entry)
	if len(entry.Embedding) > 0 {
		c.embeddings[entry.ID] = entry.Embedding
		c.index.Insert(entry.ID, entry.Model, entry.Embedding)
	}

	if c.db != nil {
		go c.saveToDB(entry)
	}
}

func (c *SemanticCache) saveToDB(entry *CacheEntry) {
	var embeddingBytes []byte
	if len(entry.Embedding) > 0 {
		embeddingBytes = serializeEmbedding(entry.Embedding)
	}

	query := `INSERT OR REPLACE INTO semantic_cache 
		(id, key, model, prompt, response, embedding, hit_count, 
		 created_at, last_hit_at, ttl_seconds, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := c.db.ExecContext(context.Background(), query,
		entry.ID, entry.Key, entry.Model, entry.Prompt, entry.Response,
		embeddingBytes, entry.HitCount, entry.CreatedAt.Unix(),
		entry.LastHitAt.Unix(), entry.TTLSeconds, entry.Metadata)

	if err != nil {
		log.Printf("[semantic-cache] Failed to save entry: %v", err)
	}
}

func (c *SemanticCache) getEmbedding(ctx context.Context, model string, text string) ([]float32, error) {
	embeddingModel := c.resolveEmbeddingModel(model)
	cacheKey := fmt.Sprintf("%s:%s", embeddingModel, text)

	if cached, ok := c.embeddingCache.Get(cacheKey); ok {
		c.embeddingCacheHits++
		return cached, nil
	}
	c.embeddingCacheMisses++

	if c.config.OllamaURL == "" {
		return nil, fmt.Errorf("ollama URL not configured")
	}

	body := map[string]interface{}{
		"model":  embeddingModel,
		"prompt": text,
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.config.OllamaURL+"/api/embeddings", strings.NewReader(string(data)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if len(result.Embedding) > 0 {
		c.embeddingCache.Set(cacheKey, result.Embedding)
	}

	return result.Embedding, nil
}

func (c *SemanticCache) evictOldest() {
	if len(c.entries) == 0 {
		return
	}

	oldestIdx := 0
	oldestTime := c.entries[0].CreatedAt
	for i, entry := range c.entries {
		if entry.CreatedAt.Before(oldestTime) {
			oldestTime = entry.CreatedAt
			oldestIdx = i
		}
	}

	removed := c.entries[oldestIdx]
	delete(c.embeddings, removed.ID)
	c.index.Remove(removed.ID)
	c.entries = append(c.entries[:oldestIdx], c.entries[oldestIdx+1:]...)
}

func (c *SemanticCache) evictionLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			newEntries := make([]*CacheEntry, 0, len(c.entries))
			for _, entry := range c.entries {
				if now.Sub(entry.CreatedAt) <= time.Duration(entry.TTLSeconds)*time.Second {
					newEntries = append(newEntries, entry)
				} else {
					delete(c.embeddings, entry.ID)
					c.index.Remove(entry.ID)
				}
			}
			c.entries = newEntries
			c.mu.Unlock()
		}
	}
}

func (c *SemanticCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var hitRate float64
	if c.hits+c.misses > 0 {
		hitRate = float64(c.hits) / float64(c.hits+c.misses)
	}

	return CacheStats{
		Entries:              len(c.entries),
		MaxEntries:           c.config.MaxEntries,
		Hits:                 c.hits,
		Misses:               c.misses,
		HitRate:              hitRate,
		TTLSeconds:           c.config.TTLSeconds,
		SimilarityThreshold:  c.config.SimilarityThreshold,
		EmbeddingModel:       c.config.EmbeddingModel,
		TotalSavedTokens:     c.totalSavedTokens,
		IndexType:            c.config.IndexType,
		IndexSize:            c.index.Len(),
		EmbeddingCacheHits:   c.embeddingCacheHits,
		EmbeddingCacheMisses: c.embeddingCacheMisses,
	}
}

func (c *SemanticCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make([]*CacheEntry, 0, c.config.MaxEntries)
	c.embeddings = make(map[string][]float32)
	c.hits = 0
	c.misses = 0
	c.index = NewFlatIndex()
	c.embeddingCache.Clear()

	if c.db != nil {
		_, _ = c.db.ExecContext(context.Background(), "DELETE FROM semantic_cache")
	}
}

func (c *SemanticCache) ListEntries(limit int) []*CacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if limit <= 0 || limit > len(c.entries) {
		limit = len(c.entries)
	}

	sort.Slice(c.entries, func(i, j int) bool {
		return c.entries[i].HitCount > c.entries[j].HitCount
	})

	result := make([]*CacheEntry, limit)
	copy(result, c.entries[:limit])
	return result
}

func (c *SemanticCache) Invalidate(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, entry := range c.entries {
		if entry.ID == id {
			delete(c.embeddings, entry.ID)
			c.index.Remove(entry.ID)
			c.entries = append(c.entries[:i], c.entries[i+1:]...)
			if c.db != nil {
				_, _ = c.db.ExecContext(context.Background(), "DELETE FROM semantic_cache WHERE id = ?", id)
			}
			return true
		}
	}
	return false
}

func (c *SemanticCache) WarmFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open warmup file: %w", err)
	}
	defer file.Close()

	count := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var entry WarmupEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			log.Printf("[semantic-cache] Failed to parse warmup entry: %v", err)
			continue
		}

		cacheKey := c.generateKey(entry.Model, entry.Prompt, nil)
		exists := false
		for _, e := range c.entries {
			if e.Key == cacheKey && e.Model == entry.Model {
				exists = true
				break
			}
		}
		if exists {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		newEntry := &CacheEntry{
			ID:         generateID(),
			Key:        cacheKey,
			Model:      entry.Model,
			Prompt:     entry.Prompt,
			Response:   entry.Response,
			HitCount:   0,
			CreatedAt:  time.Now(),
			LastHitAt:  time.Now(),
			TTLSeconds: c.config.TTLSeconds,
		}

		queryEmbedding, err := c.getEmbedding(ctx, entry.Model, entry.Prompt)
		if err == nil && queryEmbedding != nil {
			newEntry.Embedding = queryEmbedding
		}

		c.mu.Lock()
		if len(c.entries) >= c.config.MaxEntries {
			c.evictOldest()
		}
		c.entries = append(c.entries, newEntry)
		if len(newEntry.Embedding) > 0 {
			c.embeddings[newEntry.ID] = newEntry.Embedding
			c.index.Insert(newEntry.ID, newEntry.Model, newEntry.Embedding)
		}
		c.mu.Unlock()

		if c.db != nil {
			go c.saveToDB(newEntry)
		}
		count++
	}

	log.Printf("[semantic-cache] Warmed %d entries from %s", count, filePath)
	return scanner.Err()
}

func (c *SemanticCache) Stop() {
	close(c.stopCh)
}

func (c *SemanticCache) generateKey(model string, prompt string, messages []map[string]string) string {
	var sb strings.Builder
	sb.WriteString(model)
	sb.WriteString(":")
	if prompt != "" {
		sb.WriteString(prompt)
	}
	for _, msg := range messages {
		sb.WriteString(msg["role"])
		sb.WriteString(msg["content"])
	}
	return fmt.Sprintf("%x", sha256Hash(sb.String()))
}

func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}

	var dotProduct, normA, normB float64
	for i := range a {
		dotProduct += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
}

func serializeEmbedding(embedding []float32) []byte {
	data, _ := json.Marshal(embedding)
	return data
}

func deserializeEmbedding(data []byte) ([]float32, error) {
	var embedding []float32
	err := json.Unmarshal(data, &embedding)
	return embedding, err
}

func generateID() string {
	return fmt.Sprintf("sc-%d-%d", time.Now().UnixNano(), randInt())
}

func randInt() int {
	return int(time.Now().UnixNano() % 100000)
}

func sha256Hash(s string) []byte {
	h := sha256.New()
	h.Write([]byte(s))
	return h.Sum(nil)
}
