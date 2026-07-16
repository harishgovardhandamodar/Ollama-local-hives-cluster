package main

import (
	"net/http"
	"strings"
	"sync"
	"time"
)

type RateLimiter struct {
	mu       sync.Mutex
	clients  map[string]*rateBucket
	rate     int           // requests per window
	window   time.Duration // time window
}

type rateBucket struct {
	count    int
	resetAt  time.Time
}

func NewRateLimiter(rate int, window time.Duration) *RateLimiter {
	rl := &RateLimiter{
		clients: make(map[string]*rateBucket),
		rate:    rate,
		window:  window,
	}
	go rl.cleanup()
	return rl
}

func (rl *RateLimiter) Allow(clientID string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	bucket, exists := rl.clients[clientID]
	if !exists || time.Now().After(bucket.resetAt) {
		rl.clients[clientID] = &rateBucket{
			count:   1,
			resetAt: time.Now().Add(rl.window),
		}
		return true
	}

	if bucket.count >= rl.rate {
		return false
	}
	bucket.count++
	return true
}

func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		now := time.Now()
		for id, bucket := range rl.clients {
			if now.After(bucket.resetAt) {
				delete(rl.clients, id)
			}
		}
		rl.mu.Unlock()
	}
}

// AuthMiddleware validates API keys and applies rate limiting
func AuthMiddleware(apiKey string, rl *RateLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for dashboard, health, and CORS
		if r.URL.Path == "/" || r.URL.Path == "/v1/health" || r.Method == "OPTIONS" {
			next.ServeHTTP(w, r)
			return
		}

		// Rate limiting by IP
		clientIP := r.RemoteAddr
		if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
			clientIP = strings.Split(fwd, ",")[0]
		}
		if !rl.Allow(clientIP) {
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}

		// API key validation (if configured)
		if apiKey != "" {
			// Allow dashboard and internal endpoints without key
			if strings.HasPrefix(r.URL.Path, "/api/peers") ||
				strings.HasPrefix(r.URL.Path, "/api/logs") ||
				r.URL.Path == "/api/status" ||
				r.URL.Path == "/v1/health" {
				next.ServeHTTP(w, r)
				return
			}

			// Check Authorization header or query param
			key := r.Header.Get("Authorization")
			key = strings.TrimPrefix(key, "Bearer ")
			if key == "" {
				key = r.URL.Query().Get("api_key")
			}
			if key != apiKey {
				http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}
