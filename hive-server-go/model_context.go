package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ModelContextWindow holds detected or known context length for a model
type ModelContextWindow struct {
	Model         string `json:"model"`
	ContextLength int    `json:"context_length"`
	MaxOutput    int    `json:"max_output_tokens"`
	Source       string `json:"source"` // "api", "registry", "default"
}

// Well-known context windows for common models (tokens)
// Used as fallback when Ollama /api/show doesn't report context_length
var knownContextWindows = map[string]int{
	// Qwen 2.5 Coder family
	"qwen2.5-coder:0.5b":   32768,
	"qwen2.5-coder:1.5b":   32768,
	"qwen2.5-coder:3b":     32768,
	"qwen2.5-coder:7b":     32768,
	"qwen2.5-coder:14b":    131072,
	"qwen2.5-coder:32b":    131072,
	"qwen2.5-coder:72b":    131072,

	// Qwen 2.5 base
	"qwen2.5:0.5b":   32768,
	"qwen2.5:1.5b":   32768,
	"qwen2.5:3b":     32768,
	"qwen2.5:7b":     131072,
	"qwen2.5:14b":    131072,
	"qwen2.5:32b":    131072,
	"qwen2.5:72b":    131072,

	// Qwen 3
	"qwen3:0.6b":    32768,
	"qwen3:1.7b":    32768,
	"qwen3:4b":      32768,
	"qwen3:8b":      32768,
	"qwen3:14b":     131072,
	"qwen3:30b":     131072,
	"qwen3:32b":     131072,
	"qwen3:235b":    131072,

	// Qwen 3 Coder
	"qwen3-coder:0.6b":  32768,
	"qwen3-coder:1.7b":  32768,
	"qwen3-coder:4b":    32768,
	"qwen3-coder:8b":    32768,
	"qwen3-coder:14b":   131072,
	"qwen3-coder:30b":   131072,
	"qwen3-coder:32b":   131072,
	"qwen3-coder:235b":  131072,

	// DeepSeek Coder / V2
	"deepseek-coder:1.3b":    16384,
	"deepseek-coder:6.7b":    16384,
	"deepseek-coder:33b":     16384,
	"deepseek-coder-v2:16b":  128000,
	"deepseek-coder-v2:236b": 128000,

	// DeepSeek V2/V3/R1
	"deepseek-v2:16b":   128000,
	"deepseek-v2:236b":  128000,
	"deepseek-v3":       128000,
	"deepseek-r1:1.5b":  65536,
	"deepseek-r1:7b":    65536,
	"deepseek-r1:8b":    65536,
	"deepseek-r1:14b":   65536,
	"deepseek-r1:32b":   65536,
	"deepseek-r1:70b":   65536,

	// CodeLlama
	"codellama:7b":    16384,
	"codellama:13b":   16384,
	"codellama:34b":   16384,
	"codellama:70b":   16384,
	"codellama:7b-code":  16384,
	"codellama:13b-code": 16384,
	"codellama:34b-code": 16384,

	// Starcoder 2
	"starcoder2:3b":  16384,
	"starcoder2:7b":  16384,
	"starcoder2:15b": 16384,

	// Llama 3 / 3.1 / 3.2 / 3.3
	"llama3:8b":     8192,
	"llama3:70b":    8192,
	"llama3.1:8b":   131072,
	"llama3.1:70b":  131072,
	"llama3.1:405b": 131072,
	"llama3.2:1b":   131072,
	"llama3.2:3b":   131072,
	"llama3.3:70b":  131072,

	// Mistral
	"mistral:7b":      32768,
	"mistral-nemo:12b": 131072,
	"mixtral:8x7b":    32768,
	"mixtral:8x22b":   65536,

	// Gemma 2
	"gemma2:2b":  8192,
	"gemma2:9b":  8192,
	"gemma2:27b": 8192,

	// Gemma 3
	"gemma3:1b":   131072,
	"gemma3:4b":   131072,
	"gemma3:12b":  131072,
	"gemma3:27b":  131072,

	// Phi-3 / Phi-4
	"phi3:3.8b":  131072,
	"phi3:14b":   131072,
	"phi4:14b":   16384,

	// Yi
	"yi:6b":      204800,
	"yi:9b":      204800,
	"yi:34b":     204800,
	"yi-coder:1.5b": 4096,
	"yi-coder:6b":   204800,
	"yi-coder:9b":   204800,
	"yi-coder:34b":  204800,

	// GLM-4 family (ZhipuAI / Z AI)
	"glm-4":           131072,
	"glm-4-flash":     131072,
	"glm-4.7-flash":   200000,
	"glm-4.7-flash:bf16":  200000,
	"glm-4.7-flash:q4_0":  200000,
	"glm-4.7-flash:q8_0":  200000,
	"glm-4-plus":      131072,
	"glm-4-air":       131072,
	"glm-4-airx":      131072,
	"glm-4-long":      1048576,
	"glm-4v":          8192,
	"glm-4v-flash":    8192,
	"glm-4v-plus":     8192,

	// Command-R
	"command-r:35b":  131072,
	"command-r-plus:104b": 131072,

	// CodeGemma
	"codegemma:2b":   8192,
	"codegemma:7b":   8192,

	// CodeQwen
	"codeqwen:7b":    32768,

	// Magicoder
	"magicoder:7b":   8192,

	// MLPsydra
	"neural-chat:7b": 8192,
	"openhermes:7b":  8192,

	// Nous Hermes
	"nous-hermes:7b":     8192,
	"nous-hermes:13b":    8192,
	"nous-hermes2:7b":    8192,
	"nous-hermes2-mixtral:8x7b": 32768,
	"nous-hermes2-yi:34b": 204800,

	// Hermes (Teknium)
	"hermes-2-pro:7b":    4096,
	"hermes-2-pro:13b":   4096,
	"hermes-2-pro:70b":   4096,

	// OpenChat
	"openchat:7b":    8192,

	// Zephyr
	"zephyr:7b":      8192,

	// Vicuna
	"vicuna:7b":      4096,
	"vicuna:13b":     4096,
	"vicuna:33b":     4096,

	// WizardCoder
	"wizard-coder:7b":    8192,
	"wizard-coder:13b":   8192,
	"wizard-coder:34b":   8192,
	"wizard-coder:33b":   8192,
	"wizard-coder-python:7b":  8192,

	// MLX models (via ollama-mlx or compatible)
	"mlx-community/Qwen2.5-Coder-7B-Instruct-4bit":  32768,
	"mlx-community/Qwen2.5-Coder-14B-Instruct-4bit": 131072,
	"mlx-community/DeepSeek-Coder-V2-Instruct-16B":  128000,
	"mlx-community/Llama-3.1-8B-Instruct":           131072,
	"mlx-community/Mistral-7B-Instruct-v0.3":        32768,
}

// Pattern-based fallback: map model name substrings to context lengths
// Applied when exact match fails
var contextPatterns = []struct {
	Pattern string
	Tokens  int
}{
	{"qwen2.5-coder", 32768},
	{"qwen2.5", 131072},
	{"qwen3-coder", 32768},
	{"qwen3", 131072},
	{"qwen-coder", 32768},
	{"qwen", 32768},
	{"deepseek-coder-v2", 128000},
	{"deepseek-coder", 16384},
	{"deepseek-r1", 65536},
	{"deepseek", 128000},
	{"codellama", 16384},
	{"codegemma", 8192},
	{"codeqwen", 32768},
	{"magicoder", 8192},
	{"starcoder2", 16384},
	{"starcoder", 16384},
	{"llama3.1", 131072},
	{"llama3.2", 131072},
	{"llama3.3", 131072},
	{"llama3", 8192},
	{"mistral-nemo", 131072},
	{"mistral", 32768},
	{"mixtral", 32768},
	{"gemma3", 131072},
	{"gemma2", 8192},
	{"gemma", 8192},
	{"phi-4", 16384},
	{"phi4", 16384},
	{"phi-3", 131072},
	{"phi3", 131072},
	{"yi-coder", 204800},
	{"yi", 204800},
	{"hermes-2", 32768},
	{"hermes", 8192},
	{"command-r", 131072},
	{"glm-4.7", 200000},
	{"glm-4", 131072},
	{"glm", 131072},
	{"openchat", 8192},
	{"zephyr", 8192},
	{"vicuna", 4096},
	{"wizard-coder", 8192},
	{"wizard", 8192},
	{"neural-chat", 8192},
	{"openhermes", 8192},
	{"mlx", 32768},
}

// ModelContextDetector detects context window sizes for models
type ModelContextDetector struct {
	mu        sync.RWMutex
	cache     map[string]*ModelContextWindow
	ollamaURL string
	client    *http.Client
}

// NewModelContextDetector creates a new detector
func NewModelContextDetector(ollamaURL string) *ModelContextDetector {
	return &ModelContextDetector{
		cache:     make(map[string]*ModelContextWindow),
		ollamaURL: ollamaURL,
		client:    &http.Client{Timeout: 5 * time.Second},
	}
}

// DetectContextWindow determines the context length for a model using this priority:
// 1. Ollama /api/show (live API response)
// 2. Known model registry (hardcoded map)
// 3. Pattern matching on model name
// 4. Default (8192)
func (mcd *ModelContextDetector) DetectContextWindow(model string) *ModelContextWindow {
	if model == "" {
		model = "unknown"
	}

	// Check cache first
	mcd.mu.RLock()
	if cached, ok := mcd.cache[model]; ok {
		mcd.mu.RUnlock()
		return cached
	}
	mcd.mu.RUnlock()

	// Try Ollama /api/show
	if result := mcd.queryOllamaShow(model); result != nil {
		mcd.mu.Lock()
		mcd.cache[model] = result
		mcd.mu.Unlock()
		return result
	}

	// Try exact registry match
	if tokens, ok := knownContextWindows[model]; ok {
		result := &ModelContextWindow{
			Model:         model,
			ContextLength: tokens,
			Source:        "registry",
		}
		mcd.mu.Lock()
		mcd.cache[model] = result
		mcd.mu.Unlock()
		return result
	}

	// Try pattern matching
	lower := strings.ToLower(model)
	for _, p := range contextPatterns {
		if strings.Contains(lower, p.Pattern) {
			result := &ModelContextWindow{
				Model:         model,
				ContextLength: p.Tokens,
				Source:        "pattern",
			}
			mcd.mu.Lock()
			mcd.cache[model] = result
			mcd.mu.Unlock()
			return result
		}
	}

	// Default fallback
	result := &ModelContextWindow{
		Model:         model,
		ContextLength: DefaultTokenBudget,
		Source:        "default",
	}
	mcd.mu.Lock()
	mcd.cache[model] = result
	mcd.mu.Unlock()
	return result
}

// queryOllamaShow queries the Ollama /api/show endpoint for model metadata
func (mcd *ModelContextDetector) queryOllamaShow(model string) *ModelContextWindow {
	if mcd.ollamaURL == "" {
		return nil
	}

	body := map[string]interface{}{
		"name": model,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil
	}

	resp, err := mcd.client.Post(mcd.ollamaURL+"/api/show", "application/json", bytes.NewReader(data))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil
	}

	var result struct {
		ModelInfo struct {
			General struct {
				Architecture string `json:"architecture"`
				ContextLength int   `json:"context_length"`
			} `json:"general"`
		} `json:"model_info"`
		Parameters struct {
			NumCtx int `json:"num_ctx"`
		} `json:"parameters"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil
	}

	// Try context_length from model_info
	ctxLen := result.ModelInfo.General.ContextLength
	if ctxLen <= 0 {
		// Try num_ctx from parameters
		ctxLen = result.Parameters.NumCtx
	}

	if ctxLen > 0 {
		return &ModelContextWindow{
			Model:         model,
			ContextLength: ctxLen,
			Source:        "api",
		}
	}

	return nil
}

// GetContextBudgetForModel returns a recommended token budget for the model
// Applies a safety margin (80%) to avoid hitting the exact context limit
func (mcd *ModelContextDetector) GetContextBudgetForModel(model string, explicitBudget int) int {
	// If user explicitly set a budget, respect it (with bounds checking)
	if explicitBudget > 0 {
		if explicitBudget < MinTokenBudget {
			return MinTokenBudget
		}
		if explicitBudget > MaxTokenBudget {
			return MaxTokenBudget
		}
		return explicitBudget
	}

	// Auto-detect from model
	detected := mcd.DetectContextWindow(model)
	maxCtx := detected.ContextLength

	// Apply 80% safety margin to leave room for response generation
	budget := int(float64(maxCtx) * 0.80)

	// Clamp to allowed range
	if budget < MinTokenBudget {
		budget = MinTokenBudget
	}
	if budget > MaxTokenBudget {
		budget = MaxTokenBudget
	}

	return budget
}

// GetAllKnownModels returns all models from the registry with their context lengths
func GetAllKnownModels() []ModelContextWindow {
	var models []ModelContextWindow
	for name, tokens := range knownContextWindows {
		models = append(models, ModelContextWindow{
			Model:         name,
			ContextLength: tokens,
			Source:        "registry",
		})
	}
	return models
}

// NormalizeModelName strips common prefixes and returns the canonical model name
// e.g. "ollama:qwen2.5-coder:7b" → "qwen2.5-coder:7b"
//      "mlx-community/Qwen2.5-Coder-7B" → "mlx-community/qwen2.5-coder-7b"
func NormalizeModelName(model string) string {
	model = strings.TrimSpace(model)

	// Strip "ollama:" prefix
	model = strings.TrimPrefix(model, "ollama:")
	model = strings.TrimPrefix(model, "ollama/")

	// Lowercase the tag part but keep the registry prefix (like mlx-community)
	parts := strings.SplitN(model, "/", 2)
	if len(parts) == 2 {
		return parts[0] + "/" + strings.ToLower(parts[1])
	}

	return strings.ToLower(model)
}

// FormatContextWindow returns a human-readable context length string
func FormatContextWindow(tokens int) string {
	switch {
	case tokens >= 1000000:
		return fmt.Sprintf("%.1fM", float64(tokens)/1000000)
	case tokens >= 1000:
		return fmt.Sprintf("%dK", tokens/1000)
	default:
		return fmt.Sprintf("%d", tokens)
	}
}
