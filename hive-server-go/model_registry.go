package main

import (
	"sync"
	"time"
)

// ModelLoadState represents the current state of a model
type ModelLoadState string

const (
	ModelLoaded      ModelLoadState = "loaded"
	ModelLoading     ModelLoadState = "loading"
	ModelUnloading   ModelLoadState = "unloading"
	ModelNotLoaded   ModelLoadState = "not_loaded"
	ModelFailed      ModelLoadState = "failed"
)

// ModelInfo tracks the state and metadata of a loaded model
type LoadedModelInfo struct {
	Name        string         `json:"name"`
	Provider    string         `json:"provider"` // ollama, vllm, lm_studio, etc.
	NodeID      string         `json:"node_id"`
	Endpoint    string         `json:"endpoint"`
	State       ModelLoadState `json:"state"`
	LoadedAt    time.Time      `json:"loaded_at,omitempty"`
	LastUsed    time.Time      `json:"last_used"`
	RefCount    int            `json:"ref_count"` // Active requests using this model
	SizeBytes   int64          `json:"size_bytes,omitempty"`
	ContextLen  int            `json:"context_len,omitempty"`
	Family      string         `json:"family,omitempty"` // e.g., "llama3", "qwen2.5"
	Quantization string        `json:"quantization,omitempty"` // e.g., "q4_k_m", "fp16"
}

// ModelRegistry tracks which models are loaded where across the mesh
type ModelRegistry struct {
	mu sync.RWMutex
	// Local models: model_name -> LoadedModelInfo
	localModels map[string]*LoadedModelInfo
	// Peer models: node_id -> model_name -> LoadedModelInfo
	peerModels map[string]map[string]*LoadedModelInfo
	// Model family mappings for compatible substitution
	// e.g., "llama3:8b" -> ["llama3.1:8b", "llama3.2:8b"]
	modelFamilyGroups map[string][]string
	// Statistics
	totalLoads   int64
	totalUnloads int64
	loadFailures int64
}

// NewModelRegistry creates a new model registry
func NewModelRegistry() *ModelRegistry {
	return &ModelRegistry{
		localModels:       make(map[string]*LoadedModelInfo),
		peerModels:        make(map[string]map[string]*LoadedModelInfo),
		modelFamilyGroups: make(map[string][]string),
	}
}

// RegisterLocalModel registers a model as loaded on the local node
func (mr *ModelRegistry) RegisterLocalModel(name, provider, endpoint string, sizeBytes int64, contextLen int) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	now := time.Now()
	info := &LoadedModelInfo{
		Name:        name,
		Provider:    provider,
		NodeID:      getServerID(),
		Endpoint:    endpoint,
		State:       ModelLoaded,
		LoadedAt:    now,
		LastUsed:    now,
		RefCount:    0,
		SizeBytes:   sizeBytes,
		ContextLen:  contextLen,
		Family:      extractModelFamily(name),
		Quantization: extractQuantization(name),
	}

	if _, exists := mr.localModels[name]; !exists {
		mr.totalLoads++
	}
	mr.localModels[name] = info

	// Update family groups
	if info.Family != "" {
		key := info.Family
		if sizeBytes > 0 {
			key = info.Family + ":" + formatSize(sizeBytes)
		}
		mr.addToFamilyGroup(key, name)
	}
}

// UnregisterLocalModel removes a model from the local registry
func (mr *ModelRegistry) UnregisterLocalModel(name string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if _, exists := mr.localModels[name]; exists {
		mr.totalUnloads++
		delete(mr.localModels, name)
	}
}

// MarkModelUsed updates the last used timestamp for a model
func (mr *ModelRegistry) MarkModelUsed(name string, isLocal bool, nodeID string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	now := time.Now()
	if isLocal {
		if info, ok := mr.localModels[name]; ok {
			info.LastUsed = now
			info.RefCount++
		}
	} else if peerModels, ok := mr.peerModels[nodeID]; ok {
		if info, ok := peerModels[name]; ok {
			info.LastUsed = now
			info.RefCount++
		}
	}
}

// DecrementRefCount decrements the reference count when a request completes
func (mr *ModelRegistry) DecrementRefCount(name string, isLocal bool, nodeID string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if isLocal {
		if info, ok := mr.localModels[name]; ok {
			if info.RefCount > 0 {
				info.RefCount--
			}
		}
	} else if peerModels, ok := mr.peerModels[nodeID]; ok {
		if info, ok := peerModels[name]; ok {
			if info.RefCount > 0 {
				info.RefCount--
			}
		}
	}
}

// IsLoadedLocally checks if a model is currently loaded on the local node
func (mr *ModelRegistry) IsLoadedLocally(name string) bool {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	info, exists := mr.localModels[name]
	return exists && info.State == ModelLoaded
}

// GetLocalModel returns info about a locally loaded model
func (mr *ModelRegistry) GetLocalModel(name string) *LoadedModelInfo {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	info, exists := mr.localModels[name]
	if !exists || info.State != ModelLoaded {
		return nil
	}
	return info
}

// FindCompatibleLocalModel finds a compatible model already loaded locally
// Useful when exact model isn't loaded but similar one is (same family, different quant)
func (mr *ModelRegistry) FindCompatibleLocalModel(requestedModel string) *LoadedModelInfo {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	requestedFamily := extractModelFamily(requestedModel)
	if requestedFamily == "" {
		return nil
	}

	var bestMatch *LoadedModelInfo
	for _, info := range mr.localModels {
		if info.State != ModelLoaded {
			continue
		}
		if info.Family == requestedFamily {
			// Prefer same size, different quantization
			if bestMatch == nil {
				bestMatch = info
			} else if info.Quantization != "" && bestMatch.Quantization == "" {
				bestMatch = info
			} else if info.LastUsed.After(bestMatch.LastUsed) {
				// Prefer more recently used
				bestMatch = info
			}
		}
	}
	return bestMatch
}

// RegisterPeerModels registers all models loaded on a peer node
func (mr *ModelRegistry) RegisterPeerModels(nodeID string, models []LoadedModelInfo) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if _, exists := mr.peerModels[nodeID]; !exists {
		mr.peerModels[nodeID] = make(map[string]*LoadedModelInfo)
	}

	for i := range models {
		mr.peerModels[nodeID][models[i].Name] = &models[i]
	}
}

// RemovePeerModels removes all models for a peer (e.g., peer went offline)
func (mr *ModelRegistry) RemovePeerModels(nodeID string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	delete(mr.peerModels, nodeID)
}

// FindPeerWithLoadedModel finds a peer that has the requested model loaded
func (mr *ModelRegistry) FindPeerWithLoadedModel(modelName string) (nodeID string, info *LoadedModelInfo) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	for nodeID, models := range mr.peerModels {
		if info, exists := models[modelName]; exists && info.State == ModelLoaded {
			return nodeID, info
		}
	}
	return "", nil
}

// FindPeerWithCompatibleModel finds a peer with a compatible model
func (mr *ModelRegistry) FindPeerWithCompatibleModel(requestedModel string) (nodeID string, info *LoadedModelInfo) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	requestedFamily := extractModelFamily(requestedModel)
	if requestedFamily == "" {
		return "", nil
	}

	for nodeID, models := range mr.peerModels {
		for _, info := range models {
			if info.State != ModelLoaded {
				continue
			}
			if info.Family == requestedFamily {
				return nodeID, info
			}
		}
	}
	return "", nil
}

// GetAllLocalModels returns all locally loaded models
func (mr *ModelRegistry) GetAllLocalModels() []*LoadedModelInfo {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	result := make([]*LoadedModelInfo, 0, len(mr.localModels))
	for _, info := range mr.localModels {
		result = append(result, info)
	}
	return result
}

// GetStats returns registry statistics
func (mr *ModelRegistry) GetStats() map[string]interface{} {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	localCount := 0
	for _, info := range mr.localModels {
		if info.State == ModelLoaded {
			localCount++
		}
	}

	peerCount := 0
	for _, models := range mr.peerModels {
		peerCount += len(models)
	}

	return map[string]interface{}{
		"local_models_loaded": localCount,
		"peer_models_tracked": peerCount,
		"total_loads":         mr.totalLoads,
		"total_unloads":       mr.totalUnloads,
		"load_failures":       mr.loadFailures,
		"family_groups":       len(mr.modelFamilyGroups),
	}
}

// Helper functions

// extractModelFamily extracts the model family from a model name
// e.g., "llama3.1:8b-instruct-q4_k_m" -> "llama3"
func extractModelFamily(name string) string {
	// Common patterns
	families := []string{
		"llama3.2", "llama3.1", "llama3", "llama2",
		"qwen2.5", "qwen2", "qwen1.5", "qwen",
		"gemma2", "gemma",
		"mistral", "mixtral",
		"phi3", "phi2",
		"codellama", "codeqwen",
	}

	for _, f := range families {
		if len(name) >= len(f) && name[:len(f)] == f {
			return f
		}
	}

	// Fallback: use everything before first colon or dash
	for i, c := range name {
		if c == ':' || c == '-' {
			return name[:i]
		}
	}
	return name
}

// extractQuantization extracts quantization info from model name
func extractQuantization(name string) string {
	quantPatterns := []string{
		"q4_k_m", "q4_0", "q5_k_m", "q5_0",
		"q8_0", "fp16", "fp8", "bf16",
	}

	for _, q := range quantPatterns {
		if contains(name, q) {
			return q
		}
	}
	return ""
}

// formatSize formats bytes to a human-readable size string
func formatSize(bytes int64) string {
	const (
		KB = 1 << 10
		MB = 1 << 20
		GB = 1 << 30
	)

	switch {
	case bytes >= GB:
		return "large"
	case bytes >= MB:
		return "medium"
	default:
		return "small"
	}
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// addToFamilyGroup adds a model to its family group
func (mr *ModelRegistry) addToFamilyGroup(groupKey, modelName string) {
	group, exists := mr.modelFamilyGroups[groupKey]
	if !exists {
		mr.modelFamilyGroups[groupKey] = []string{modelName}
		return
	}

	// Check if already in group
	for _, m := range group {
		if m == modelName {
			return
		}
	}
	mr.modelFamilyGroups[groupKey] = append(group, modelName)
}
