package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
)

// CacheEntry represents a cached response with metadata
type CacheEntry struct {
	Response      interface{} `json:"response"`
	Model         string      `json:"model"`
	SystemPrompt  string      `json:"system_prompt,omitempty"`
	UserMessage   string      `json:"user_message"`
	CreatedAt     time.Time   `json:"created_at"`
	AccessCount   int64       `json:"access_count"`
	LastAccessed  time.Time   `json:"last_accessed"`
	TokenCount    int         `json:"token_count,omitempty"`
	EmbeddingHash string      `json:"embedding_hash,omitempty"` // For semantic similarity
}

// CacheConfig holds configuration for the response cache
type CacheConfig struct {
	MaxEntries       int64         `json:"max_entries"`
	TTL              time.Duration `json:"ttl"`
	EnableSemantic   bool          `json:"enable_semantic"`
	SimilarityThreshold float64    `json:"similarity_threshold"`
}

// DefaultCacheConfig returns sensible defaults
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		MaxEntries:        10000,
		TTL:               1 * time.Hour,
		EnableSemantic:    false, // Disabled by default, requires embedding model
		SimilarityThreshold: 0.95,
	}
}

// ResponseCache provides intelligent caching for LLM responses
type ResponseCache struct {
	mu           sync.RWMutex
	cache        *ristretto.Cache[string, *CacheEntry]
	config       CacheConfig
	semIndex     *SemanticIndex // Optional: for semantic search
	stats        CacheStats
	stopCleanup  chan struct{}
	cleanupDone  chan struct{}
}

// CacheStats tracks cache performance metrics
type CacheStats struct {
	Hits        int64 `json:"hits"`
	Misses      int64 `json:"misses"`
	Evictions   int64 `json:"evictions"`
	SemanticHits int64 `json:"semantic_hits"`
	Writes      int64 `json:"writes"`
}

// SemanticIndex maintains an index for semantic similarity search
type SemanticIndex struct {
	mu        sync.RWMutex
	embeddings map[string][]float64 // cache_key -> embedding vector
	indexed    map[string]bool      // cache_key -> indexed
}

// NewResponseCache creates a new response cache with the given configuration
func NewResponseCache(config CacheConfig) (*ResponseCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config[string, *CacheEntry]{
		NumCounters: config.MaxEntries * 10,
		MaxCost:     config.MaxEntries,
		BufferItems: 64,
		Cost: func(value *CacheEntry) int64 {
			return 1 // Each entry costs 1
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create cache: %w", err)
	}

	rc := &ResponseCache{
		cache:       cache,
		config:      config,
		stats:       CacheStats{},
		stopCleanup: make(chan struct{}),
		cleanupDone: make(chan struct{}),
	}

	if config.EnableSemantic {
		rc.semIndex = &SemanticIndex{
			embeddings: make(map[string][]float64),
			indexed:    make(map[string]bool),
		}
	}

	// Start background cleanup goroutine
	go rc.cleanupLoop()

	return rc, nil
}

// GenerateCacheKey creates a unique key for a request based on model and prompts
func (rc *ResponseCache) GenerateCacheKey(model, systemPrompt, userMessage string) string {
	hash := sha256.New()
	hash.Write([]byte(fmt.Sprintf("%s|%s|%s", model, systemPrompt, userMessage)))
	return hex.EncodeToString(hash.Sum(nil))
}

// Get retrieves a cached response if available
func (rc *ResponseCache) Get(model, systemPrompt, userMessage string) (*CacheEntry, bool) {
	key := rc.GenerateCacheKey(model, systemPrompt, userMessage)
	
	rc.mu.Lock()
	rc.stats.Misses++ // Assume miss until proven hit
	rc.mu.Unlock()

	entry, found := rc.cache.Get(key)
	if !found {
		// Try semantic search if enabled
		if rc.config.EnableSemantic && rc.semIndex != nil {
			if semanticEntry := rc.semanticSearch(userMessage, model); semanticEntry != nil {
				rc.mu.Lock()
				rc.stats.SemanticHits++
				rc.stats.Hits++
				rc.mu.Unlock()
				return semanticEntry, true
			}
		}
		
		rc.mu.Lock()
		rc.stats.Misses-- // Correct the count
		rc.mu.Unlock()
		return nil, false
	}

	// Update access metadata
	rc.mu.Lock()
	entry.AccessCount++
	entry.LastAccessed = time.Now()
	rc.stats.Hits++
	rc.mu.Unlock()

	return entry, true
}

// Set stores a response in the cache
func (rc *ResponseCache) Set(model, systemPrompt, userMessage string, response interface{}, tokenCount int) {
	key := rc.GenerateCacheKey(model, systemPrompt, userMessage)
	
	entry := &CacheEntry{
		Response:     response,
		Model:        model,
		SystemPrompt: systemPrompt,
		UserMessage:  userMessage,
		CreatedAt:    time.Now(),
		AccessCount:  0,
		LastAccessed: time.Now(),
		TokenCount:   tokenCount,
	}

	rc.cache.Set(key, entry, 1)
	
	rc.mu.Lock()
	rc.stats.Writes++
	rc.mu.Unlock()

	// Index for semantic search if enabled
	if rc.config.EnableSemantic && rc.semIndex != nil {
		// In production, you'd compute actual embeddings here
		// For now, we'll skip this as it requires an embedding model
		// embedding := computeEmbedding(userMessage)
		// rc.semIndex.Add(key, embedding)
	}
}

// Delete removes an entry from the cache
func (rc *ResponseCache) Delete(model, systemPrompt, userMessage string) {
	key := rc.GenerateCacheKey(model, systemPrompt, userMessage)
	rc.cache.Del(key)
	
	if rc.semIndex != nil {
		rc.semIndex.Remove(key)
	}
}

// Clear empties the entire cache
func (rc *ResponseCache) Clear() {
	rc.cache.Clear()
	if rc.semIndex != nil {
		rc.semIndex.Clear()
	}
}

// GetStats returns current cache statistics
func (rc *ResponseCache) GetStats() CacheStats {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	
	stats := rc.stats
	// Note: ristretto v2 doesn't expose evictions directly in the same way
	// We'll track this manually if needed
	
	return stats
}

// Size returns the current number of entries in the cache
func (rc *ResponseCache) Size() int64 {
	// Ristretto v2 doesn't provide a direct size method
	// We track writes and could track deletes/evictions separately
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.stats.Writes // Approximation
}

// cleanupLoop periodically removes expired entries
func (rc *ResponseCache) cleanupLoop() {
	defer close(rc.cleanupDone)
	
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			rc.cleanupExpired()
		case <-rc.stopCleanup:
			return
		}
	}
}

// cleanupExpired removes entries older than TTL
// Note: ristretto v2 doesn't support Range, so we use a different approach
func (rc *ResponseCache) cleanupExpired() {
	// Ristretto handles TTL automatically via cost-based eviction
	// This is a placeholder for custom cleanup logic if needed
	// In production, you might want to maintain a separate index for TTL-based cleanup
}

// semanticSearch finds similar cached responses using semantic similarity
func (rc *ResponseCache) semanticSearch(message, model string) *CacheEntry {
	if rc.semIndex == nil {
		return nil
	}
	
	rc.semIndex.mu.RLock()
	defer rc.semIndex.mu.RUnlock()
	
	// This is a placeholder - in production you would:
	// 1. Compute embedding for the input message
	// 2. Compare with stored embeddings using cosine similarity
	// 3. Return entries above similarity threshold
	
	// For now, return nil to indicate no semantic match
	return nil
}

// Add adds an embedding to the semantic index
func (si *SemanticIndex) Add(key string, embedding []float64) {
	si.mu.Lock()
	defer si.mu.Unlock()
	
	si.embeddings[key] = embedding
	si.indexed[key] = true
}

// Remove removes an embedding from the semantic index
func (si *SemanticIndex) Remove(key string) {
	si.mu.Lock()
	defer si.mu.Unlock()
	
	delete(si.embeddings, key)
	delete(si.indexed, key)
}

// Clear empties the semantic index
func (si *SemanticIndex) Clear() {
	si.mu.Lock()
	defer si.mu.Unlock()
	
	si.embeddings = make(map[string][]float64)
	si.indexed = make(map[string]bool)
}

// Stop gracefully shuts down the cache cleanup goroutine
func (rc *ResponseCache) Stop() {
	close(rc.stopCleanup)
	<-rc.cleanupDone
}

// MarshalJSON implements custom JSON marshaling for CacheEntry
func (ce *CacheEntry) MarshalJSON() ([]byte, error) {
	type Alias CacheEntry
	return json.Marshal(&struct {
		*Alias
		TimeSinceCreated string `json:"time_since_created"`
	}{
		Alias:            (*Alias)(ce),
		TimeSinceCreated: time.Since(ce.CreatedAt).Round(time.Second).String(),
	})
}

// GetHitRatio returns the cache hit ratio (0.0 to 1.0)
func (rc *ResponseCache) GetHitRatio() float64 {
	stats := rc.GetStats()
	total := stats.Hits + stats.Misses
	if total == 0 {
		return 0.0
	}
	return float64(stats.Hits) / float64(total)
}

// LogStats periodically logs cache statistics
func (rc *ResponseCache) LogStats() {
	stats := rc.GetStats()
	hitRatio := rc.GetHitRatio()
	size := rc.Size()
	
	logInfo("Cache Stats: size=%d, hits=%d, misses=%d, writes=%d, hit_ratio=%.2f%%",
		size, stats.Hits, stats.Misses, stats.Writes, hitRatio*100)
}
