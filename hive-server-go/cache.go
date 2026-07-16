package main

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"time"
)

type CacheEntry struct {
	Key       string
	Value     interface{}
	CreatedAt time.Time
	HitCount  int
}

type ResponseCache struct {
	mu         sync.RWMutex
	entries    map[string]*CacheEntry
	maxEntries int
	ttl        time.Duration
	hits       int64
	misses     int64
}

func NewResponseCache(maxEntries int, ttlSeconds int) *ResponseCache {
	if maxEntries <= 0 {
		maxEntries = 1000
	}
	if ttlSeconds <= 0 {
		ttlSeconds = 300 // 5 minutes default
	}

	cache := &ResponseCache{
		entries:    make(map[string]*CacheEntry),
		maxEntries: maxEntries,
		ttl:        time.Duration(ttlSeconds) * time.Second,
	}
	go cache.evictLoop()
	return cache
}

func (c *ResponseCache) GenerateKey(model string, messages []map[string]string) string {
	// Create a deterministic key from model + messages
	h := sha256.New()
	h.Write([]byte(model))
	for _, m := range messages {
		h.Write([]byte(m["role"]))
		h.Write([]byte(m["content"]))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (c *ResponseCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[key]
	if !exists {
		c.misses++
		return nil, false
	}

	if time.Since(entry.CreatedAt) > c.ttl {
		c.misses++
		return nil, false
	}

	entry.HitCount++
	c.hits++
	return entry.Value, true
}

func (c *ResponseCache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict oldest if at capacity
	if len(c.entries) >= c.maxEntries {
		var oldestKey string
		var oldestTime time.Time
		for k, v := range c.entries {
			if oldestKey == "" || v.CreatedAt.Before(oldestTime) {
				oldestKey = k
				oldestTime = v.CreatedAt
			}
		}
		if oldestKey != "" {
			delete(c.entries, oldestKey)
		}
	}

	c.entries[key] = &CacheEntry{
		Key:       key,
		Value:     value,
		CreatedAt: time.Now(),
	}
}

func (c *ResponseCache) Stats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var hitRate float64
	if c.hits+c.misses > 0 {
		hitRate = float64(c.hits) / float64(c.hits+c.misses)
	}

	return map[string]interface{}{
		"entries":    len(c.entries),
		"max_entries": c.maxEntries,
		"hits":       c.hits,
		"misses":     c.misses,
		"hit_rate":   hitRate,
		"ttl_seconds": int(c.ttl.Seconds()),
	}
}

func (c *ResponseCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*CacheEntry)
	c.hits = 0
	c.misses = 0
}

func (c *ResponseCache) evictLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for key, entry := range c.entries {
			if now.Sub(entry.CreatedAt) > c.ttl {
				delete(c.entries, key)
			}
		}
		c.mu.Unlock()
	}
}

// CachedChatResponse wraps a chat response for caching
type CachedChatResponse struct {
	Content string
	Usage   *OpenAIUsage
}
