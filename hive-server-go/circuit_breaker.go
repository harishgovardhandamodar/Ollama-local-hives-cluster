package main

import (
	"sync"
	"time"
)

// CircuitState represents the state of a circuit breaker
type CircuitState int

const (
	CircuitClosed   CircuitState = iota // Normal operation, requests allowed
	CircuitOpen                         // Failure threshold exceeded, requests blocked
	CircuitHalfOpen                     // Testing if service recovered
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerConfig holds configuration for circuit breakers
type CircuitBreakerConfig struct {
	FailureThreshold int           // Number of failures before opening circuit
	SuccessThreshold int           // Successes needed in half-open to close
	Timeout          time.Duration // How long circuit stays open before half-open
	Interval         time.Duration // Minimum interval between failure counts
}

// DefaultCircuitBreakerConfig returns sensible defaults
func DefaultCircuitBreakerConfig() *CircuitBreakerConfig {
	return &CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 3,
		Timeout:          30 * time.Second,
		Interval:         60 * time.Second,
	}
}

// CircuitBreaker implements the circuit breaker pattern for fault tolerance
type CircuitBreaker struct {
	mu              sync.RWMutex
	state           CircuitState
	failures        int
	successes       int
	lastFailure     time.Time
	lastStateChange time.Time
	config          *CircuitBreakerConfig
	name            string
}

// NewCircuitBreaker creates a new circuit breaker with the given config
func NewCircuitBreaker(name string, config *CircuitBreakerConfig) *CircuitBreaker {
	if config == nil {
		config = DefaultCircuitBreakerConfig()
	}
	return &CircuitBreaker{
		state:           CircuitClosed,
		config:          config,
		name:            name,
		lastStateChange: time.Now(),
	}
}

// Allow checks if a request should be allowed through
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		// Check if timeout has elapsed to transition to half-open
		if time.Since(cb.lastFailure) > cb.config.Timeout {
			cb.transitionTo(CircuitHalfOpen)
			return true
		}
		return false
	case CircuitHalfOpen:
		// Allow limited requests in half-open state
		return true
	}
	return false
}

// RecordSuccess records a successful request
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitHalfOpen:
		cb.successes++
		if cb.successes >= cb.config.SuccessThreshold {
			logInfo("Circuit breaker %s: closing circuit after %d successes", cb.name, cb.successes)
			cb.transitionTo(CircuitClosed)
		}
	case CircuitClosed:
		// Reset failure count on success
		cb.failures = 0
	}
}

// RecordFailure records a failed request
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()

	switch cb.state {
	case CircuitClosed:
		if cb.failures >= cb.config.FailureThreshold {
			logWarn("Circuit breaker %s: opening circuit after %d failures", cb.name, cb.failures)
			cb.transitionTo(CircuitOpen)
		}
	case CircuitHalfOpen:
		// Any failure in half-open immediately opens the circuit
		logWarn("Circuit breaker %s: reopening circuit after failure in half-open state", cb.name)
		cb.transitionTo(CircuitOpen)
	}
}

// State returns the current state of the circuit breaker
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Stats returns statistics about the circuit breaker
func (cb *CircuitBreaker) Stats() map[string]interface{} {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return map[string]interface{}{
		"name":            cb.name,
		"state":           cb.state.String(),
		"failures":        cb.failures,
		"successes":       cb.successes,
		"last_failure":    cb.lastFailure.Unix(),
		"last_state_change": cb.lastStateChange.Unix(),
		"failure_threshold": cb.config.FailureThreshold,
		"success_threshold": cb.config.SuccessThreshold,
		"timeout_seconds": cb.config.Timeout.Seconds(),
	}
}

// Reset manually resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transitionTo(CircuitClosed)
}

// transitionTo changes the state and updates metrics
func (cb *CircuitBreaker) transitionTo(newState CircuitState) {
	cb.state = newState
	cb.lastStateChange = time.Now()
	
	if newState == CircuitClosed {
		cb.failures = 0
		cb.successes = 0
	} else if newState == CircuitHalfOpen {
		cb.successes = 0
	}
}

// CircuitBreakerManager manages circuit breakers for multiple endpoints/services
type CircuitBreakerManager struct {
	mu           sync.RWMutex
	breakers     map[string]*CircuitBreaker
	defaultConfig *CircuitBreakerConfig
}

// NewCircuitBreakerManager creates a new manager
func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers:      make(map[string]*CircuitBreaker),
		defaultConfig: DefaultCircuitBreakerConfig(),
	}
}

// Get retrieves or creates a circuit breaker for the given name
func (cbm *CircuitBreakerManager) Get(name string) *CircuitBreaker {
	cbm.mu.RLock()
	if cb, exists := cbm.breakers[name]; exists {
		cbm.mu.RUnlock()
		return cb
	}
	cbm.mu.RUnlock()

	cbm.mu.Lock()
	defer cbm.mu.Unlock()
	
	// Double-check after acquiring write lock
	if cb, exists := cbm.breakers[name]; exists {
		return cb
	}
	
	cb := NewCircuitBreaker(name, cbm.defaultConfig)
	cbm.breakers[name] = cb
	return cb
}

// GetWithConfig retrieves or creates a circuit breaker with custom config
func (cbm *CircuitBreakerManager) GetWithConfig(name string, config *CircuitBreakerConfig) *CircuitBreaker {
	cbm.mu.RLock()
	if cb, exists := cbm.breakers[name]; exists {
		cbm.mu.RUnlock()
		return cb
	}
	cbm.mu.RUnlock()

	cbm.mu.Lock()
	defer cbm.mu.Unlock()
	
	// Double-check after acquiring write lock
	if cb, exists := cbm.breakers[name]; exists {
		return cb
	}
	
	cb := NewCircuitBreaker(name, config)
	cbm.breakers[name] = cb
	return cb
}

// GetAll returns all circuit breakers
func (cbm *CircuitBreakerManager) GetAll() map[string]*CircuitBreaker {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()
	
	result := make(map[string]*CircuitBreaker, len(cbm.breakers))
	for k, v := range cbm.breakers {
		result[k] = v
	}
	return result
}

// Remove removes a circuit breaker
func (cbm *CircuitBreakerManager) Remove(name string) {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()
	delete(cbm.breakers, name)
}

// Execute wraps a function execution with circuit breaker protection
func (cbm *CircuitBreakerManager) Execute(name string, fn func() error) error {
	cb := cbm.Get(name)
	
	if !cb.Allow() {
		return ErrCircuitOpen
	}
	
	err := fn()
	if err != nil {
		cb.RecordFailure()
	} else {
		cb.RecordSuccess()
	}
	return err
}

// ErrCircuitOpen is returned when circuit breaker is open
var ErrCircuitOpen = &CircuitError{Message: "circuit breaker is open"}

// CircuitError represents a circuit breaker error
type CircuitError struct {
	Message string
}

func (e *CircuitError) Error() string {
	return e.Message
}
