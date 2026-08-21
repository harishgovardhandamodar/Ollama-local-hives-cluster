package main

import (
"crypto/sha256"
"encoding/hex"
"fmt"
"sync"
"time"

"github.com/dgraph-io/ristretto"
)

// CacheEntry represents a cached response with metadata
type CacheEntry struct {
Response     string
Model        string
Tokens       int
CreatedAt    time.Time
LastAccessed time.Time
AccessCount  int
}

// CacheConfig holds configuration for the response cache
type CacheConfig struct {
MaxEntries  int64
EnableSemantic bool
TTLDuration time.Duration
}

// CacheStats holds metrics for monitoring
type CacheStats struct {
Hits         int64 `json:"hits"`
Misses       int64 `json:"misses"`
SemanticHits int64 `json:"semantic_hits"`
Size         int64 `json:"size"`
Cost         int64 `json:"cost"`
}

// SemanticIndex provides simple semantic search capability
type SemanticIndex struct {
mu         sync.RWMutex
embeddings map[string][]float64
indexed    map[string]bool
}

// ResponseCache provides LLM response caching with TTL
type ResponseCache struct {
mu          sync.RWMutex
cache       *ristretto.Cache
config      CacheConfig
stats       CacheStats
semIndex    *SemanticIndex
stopCleanup chan struct{}
cleanupDone chan struct{}
items       map[string]*CacheEntry // Track items for cleanup
}

// NewResponseCache creates a new response cache
func NewResponseCache(config CacheConfig) (*ResponseCache, error) {
cache, err := ristretto.NewCache(&ristretto.Config{
NumCounters: config.MaxEntries * 10,
MaxCost:     config.MaxEntries,
BufferItems: 64,
Cost: func(value interface{}) int64 {
return 1
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
items:       make(map[string]*CacheEntry),
}

go rc.cleanupLoop()
return rc, nil
}

// GenerateCacheKey creates a unique key for a request
func (rc *ResponseCache) GenerateCacheKey(model, systemPrompt, userMessage string) string {
hash := sha256.New()
hash.Write([]byte(fmt.Sprintf("%s|%s|%s", model, systemPrompt, userMessage)))
return hex.EncodeToString(hash.Sum(nil))
}

// Get retrieves a cached response if available
func (rc *ResponseCache) Get(model, systemPrompt, userMessage string) (*CacheEntry, bool) {
key := rc.GenerateCacheKey(model, systemPrompt, userMessage)

rc.mu.Lock()
rc.stats.Misses++
rc.mu.Unlock()

value, found := rc.cache.Get(key)
if !found {
rc.mu.Lock()
rc.stats.Misses--
rc.mu.Unlock()
return nil, false
}

entry, ok := value.(*CacheEntry)
if !ok {
rc.mu.Lock()
rc.stats.Misses--
rc.mu.Unlock()
return nil, false
}

rc.mu.Lock()
entry.AccessCount++
entry.LastAccessed = time.Now()
rc.stats.Hits++
rc.mu.Unlock()

return entry, true
}

// Set stores a response in the cache
func (rc *ResponseCache) Set(model, systemPrompt, userMessage, response string, tokens int) {
key := rc.GenerateCacheKey(model, systemPrompt, userMessage)
entry := &CacheEntry{
Response:     response,
Model:        model,
Tokens:       tokens,
CreatedAt:    time.Now(),
LastAccessed: time.Now(),
AccessCount:  1,
}

rc.cache.Set(key, entry, 1)

rc.mu.Lock()
rc.items[key] = entry
rc.mu.Unlock()
}

// Delete removes an entry from the cache
func (rc *ResponseCache) Delete(model, systemPrompt, userMessage string) {
key := rc.GenerateCacheKey(model, systemPrompt, userMessage)
rc.cache.Del(key)

rc.mu.Lock()
delete(rc.items, key)
rc.mu.Unlock()
}

// Stats returns current cache statistics
func (rc *ResponseCache) Stats() CacheStats {
rc.mu.RLock()
defer rc.mu.RUnlock()

size := int64(len(rc.items))
rc.stats.Size = size
rc.stats.Cost = size

return rc.stats
}

// Clear removes all entries from the cache
func (rc *ResponseCache) Clear() {
rc.cache.Clear()
rc.mu.Lock()
rc.items = make(map[string]*CacheEntry)
rc.mu.Unlock()
}

// Close shuts down the cache gracefully
func (rc *ResponseCache) Close() {
close(rc.stopCleanup)
<-rc.cleanupDone
rc.cache.Close()
}

func (rc *ResponseCache) cleanupLoop() {
defer close(rc.cleanupDone)
ticker := time.NewTicker(1 * time.Minute)
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

func (rc *ResponseCache) cleanupExpired() {
now := time.Now()
rc.mu.Lock()
defer rc.mu.Unlock()

for key, entry := range rc.items {
if now.Sub(entry.CreatedAt) > rc.config.TTLDuration {
rc.cache.Del(key)
delete(rc.items, key)
}
}
}
