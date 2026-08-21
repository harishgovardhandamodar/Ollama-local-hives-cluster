package main

import "time"

// maxInt returns the maximum of two integers
func maxInt(a, b int) int {
if a > b {
return a
}
return b
}

// minInt returns the minimum of two integers
func minInt(a, b int) int {
if a < b {
return a
}
return b
}

// estimateTokens estimates token count from text (rough approximation)
func estimateTokensHelper(text string) int {
return len(text) / 4
}

// calculateDeadline computes job deadline based on type and size
func calculateDeadline(jobType string, estimatedTokens int, maxTimeout time.Duration) time.Time {
baseTimeout := 30 * time.Second

switch jobType {
case "chat_stream", "agent":
baseTimeout = 120 * time.Second
case "generate":
baseTimeout = 60 * time.Second
case "embed":
baseTimeout = 30 * time.Second
}

tokenTime := time.Duration(estimatedTokens/20) * time.Second
total := baseTimeout + tokenTime

if total > maxTimeout {
total = maxTimeout
}

return time.Now().Add(total)
}
