package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type LogEntry struct {
	Timestamp float64 `json:"epoch"`
	TimeStr   string  `json:"ts"`
	Level     string  `json:"level"`
	Message   string  `json:"message"`
	Logger    string  `json:"logger"`
}

type RingBuffer struct {
	entries []LogEntry
	maxSize int
	mu      sync.RWMutex
}

func NewRingBuffer(maxSize int) *RingBuffer {
	return &RingBuffer{
		entries: make([]LogEntry, 0, maxSize),
		maxSize: maxSize,
	}
}

func (rb *RingBuffer) Write(level, msg, loggerName string) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	entry := LogEntry{
		Timestamp: float64(time.Now().UnixMilli()) / 1000,
		TimeStr:   time.Now().Format("15:04:05"),
		Level:     level,
		Message:   msg,
		Logger:    loggerName,
	}
	if len(rb.entries) >= rb.maxSize {
		rb.entries = rb.entries[1:]
	}
	rb.entries = append(rb.entries, entry)
}

func (rb *RingBuffer) GetAll() []LogEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	out := make([]LogEntry, len(rb.entries))
	copy(out, rb.entries)
	return out
}

func (rb *RingBuffer) GetSince(since float64) []LogEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	var out []LogEntry
	for _, e := range rb.entries {
		if e.Timestamp >= since {
			out = append(out, e)
		}
	}
	return out
}

func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.entries = rb.entries[:0]
}

type logWriter struct {
	rb     *RingBuffer
	logger *log.Logger
	name   string
}

func (w *logWriter) Write(p []byte) (int, error) {
	msg := string(p)
	w.rb.Write("INFO", msg, w.name)
	return w.logger.Writer().Write(p)
}

var (
	globalRingBuffer = NewRingBuffer(500)
	infoLogger       = log.New(os.Stdout, "INFO ", log.Ltime)
	errorLogger      = log.New(os.Stderr, "ERROR ", log.Ltime)
	multiInfo        = io.MultiWriter(os.Stdout, &logWriter{rb: globalRingBuffer, logger: infoLogger, name: "hive-server"})
	multiError       = io.MultiWriter(os.Stderr, &logWriter{rb: globalRingBuffer, logger: errorLogger, name: "hive-server"})
	hivelog          = log.New(multiInfo, "", 0)
	hiveerror        = log.New(multiError, "", 0)
	jsonLogging      = false
)

func init() {
	hivelog.SetFlags(log.Ltime)
	// Enable JSON logging if HIVE_LOG_JSON=true
	if os.Getenv("HIVE_LOG_JSON") == "true" {
		jsonLogging = true
	}
}

func logInfo(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	if jsonLogging {
		entry := map[string]interface{}{
			"ts":      time.Now().Format(time.RFC3339),
			"level":   "INFO",
			"logger":  "hive-server",
			"message": msg,
		}
		data, _ := json.Marshal(entry)
		hivelog.Printf("%s", string(data))
	} else {
		hivelog.Printf("INFO [hive-server] %s", msg)
	}
}

func logError(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	globalRingBuffer.Write("ERROR", msg, "hive-server")
	if jsonLogging {
		entry := map[string]interface{}{
			"ts":      time.Now().Format(time.RFC3339),
			"level":   "ERROR",
			"logger":  "hive-server",
			"message": msg,
		}
		data, _ := json.Marshal(entry)
		errorLogger.Printf("%s", string(data))
	} else {
		errorLogger.Printf("[hive-server] %s", msg)
	}
}

func logWarn(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	globalRingBuffer.Write("WARN", msg, "hive-server")
	if jsonLogging {
		entry := map[string]interface{}{
			"ts":      time.Now().Format(time.RFC3339),
			"level":   "WARN",
			"logger":  "hive-server",
			"message": msg,
		}
		data, _ := json.Marshal(entry)
		hivelog.Printf("%s", string(data))
	} else {
		hivelog.Printf("WARN [hive-server] %s", msg)
	}
}

func getLogs(since float64) []LogEntry {
	if since > 0 {
		return globalRingBuffer.GetSince(since)
	}
	return globalRingBuffer.GetAll()
}

func clearLogs() {
	globalRingBuffer.Clear()
}
