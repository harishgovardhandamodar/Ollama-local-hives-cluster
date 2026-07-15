package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type ProviderType string

const (
	ProviderOllama  ProviderType = "ollama"
	ProviderLMStudio ProviderType = "lm_studio"
	ProviderVLLM    ProviderType = "vllm"
	ProviderOpenAI  ProviderType = "openai_compatible"
	ProviderCustom  ProviderType = "custom"
)

type ModelInfo struct {
	Name     string `json:"name"`
	Provider string `json:"provider"`
	NodeID   string `json:"node_id"`
	SizeGB   string `json:"size_gb,omitempty"`
}

type ProviderInfo struct {
	Type    ProviderType `json:"type"`
	BaseURL string       `json:"base_url"`
	Healthy bool         `json:"healthy"`
	Error   string       `json:"error,omitempty"`
	Models  []ModelInfo  `json:"models"`
}

type NodeInfo struct {
	ID        string         `json:"id"`
	Endpoint  string         `json:"endpoint"`
	IsSelf    bool           `json:"is_self"`
	Alive     bool           `json:"alive"`
	Load      float64        `json:"load,omitempty"`
	Clients   int            `json:"clients,omitempty"`
	Providers []ProviderInfo `json:"providers"`
}

type ProviderManager struct {
	mu          sync.RWMutex
	selfID      string
	selfPort    int
	ollamaURL   string
	customURLs  []string
	nodesCache  []NodeInfo
	lastRefresh time.Time
	interval    time.Duration
	client      *http.Client
}

func NewProviderManager(selfID string, selfPort int, ollamaURL string, customURLs []string) *ProviderManager {
	return &ProviderManager{
		selfID:     selfID,
		selfPort:   selfPort,
		ollamaURL:  ollamaURL,
		customURLs: customURLs,
		interval:   30 * time.Second,
		client:     &http.Client{Timeout: 5 * time.Second},
	}
}

func (pm *ProviderManager) GetNodes(peers []*PeerInfo) []NodeInfo {
	pm.mu.RLock()
	age := time.Since(pm.lastRefresh)
	pm.mu.RUnlock()

	if age > pm.interval {
		pm.refresh(peers)
	}

	pm.mu.RLock()
	defer pm.mu.RUnlock()
	out := make([]NodeInfo, len(pm.nodesCache))
	copy(out, pm.nodesCache)
	return out
}

func (pm *ProviderManager) refresh(peers []*PeerInfo) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.lastRefresh = time.Now()

	var nodes []NodeInfo

	selfProviders := pm.detectLocalProviders()
	selfNode := NodeInfo{
		ID:        pm.selfID,
		Endpoint:  fmt.Sprintf("http://localhost:%d", pm.selfPort),
		IsSelf:    true,
		Alive:     true,
		Providers: selfProviders,
	}
	nodes = append(nodes, selfNode)

	for _, p := range peers {
		providers := pm.detectPeerProviders(p)
		alive := time.Since(time.Unix(int64(p.LastSeen), 0)) < 30*time.Second
		nodes = append(nodes, NodeInfo{
			ID:        p.ServerID,
			Endpoint:  p.Endpoint,
			IsSelf:    false,
			Alive:     alive,
			Load:      p.Load,
			Clients:   p.Clients,
			Providers: providers,
		})
	}

	pm.nodesCache = nodes
}

func (pm *ProviderManager) detectLocalProviders() []ProviderInfo {
	var providers []ProviderInfo

	if p := pm.probeOllama(pm.ollamaURL, pm.selfID); p != nil {
		providers = append(providers, *p)
	}
	if p := pm.probeLMStudio(pm.selfID); p != nil {
		providers = append(providers, *p)
	}
	if p := pm.probeVLLM(pm.selfID); p != nil {
		providers = append(providers, *p)
	}
	for _, cu := range pm.customURLs {
		if p := pm.probeCustom(cu, pm.selfID); p != nil {
			providers = append(providers, *p)
		}
	}

	return providers
}

func (pm *ProviderManager) detectPeerProviders(peer *PeerInfo) []ProviderInfo {
	var providers []ProviderInfo

	parsed, err := url.Parse(peer.Endpoint)
	if err != nil {
		return providers
	}
	host := parsed.Hostname()

	if p := pm.probeOllama(fmt.Sprintf("http://%s:11434", host), peer.ServerID); p != nil {
		providers = append(providers, *p)
	}
	if p := pm.probeLMStudioAt(fmt.Sprintf("http://%s:1234", host), peer.ServerID); p != nil {
		providers = append(providers, *p)
	}
	if p := pm.probeVLLMAt(fmt.Sprintf("http://%s:8000", host), peer.ServerID); p != nil {
		providers = append(providers, *p)
	}

	return providers
}

func (pm *ProviderManager) probeOllama(baseURL, nodeID string) *ProviderInfo {
	resp, err := pm.client.Get(baseURL + "/api/tags")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var result struct {
		Models []struct {
			Name string `json:"name"`
			Size int64  `json:"size"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil
	}

	models := make([]ModelInfo, 0, len(result.Models))
	for _, m := range result.Models {
		sizeGB := ""
		if m.Size > 0 {
			sizeGB = fmt.Sprintf("%.1f", float64(m.Size)/1e9)
		}
		models = append(models, ModelInfo{
			Name:     m.Name,
			Provider: string(ProviderOllama),
			NodeID:   nodeID,
			SizeGB:   sizeGB,
		})
	}

	return &ProviderInfo{
		Type:    ProviderOllama,
		BaseURL: baseURL,
		Healthy: true,
		Models:  models,
	}
}

func (pm *ProviderManager) probeLMStudio(nodeID string) *ProviderInfo {
	return pm.probeLMStudioAt("http://localhost:1234", nodeID)
}

func (pm *ProviderManager) probeLMStudioAt(baseURL, nodeID string) *ProviderInfo {
	models, err := pm.probeOpenAICompatible(baseURL)
	if err != nil {
		return nil
	}
	for i := range models {
		models[i].Provider = string(ProviderLMStudio)
	}
	return &ProviderInfo{
		Type:    ProviderLMStudio,
		BaseURL: baseURL,
		Healthy: true,
		Models:  models,
	}
}

func (pm *ProviderManager) probeVLLM(nodeID string) *ProviderInfo {
	return pm.probeVLLMAt("http://localhost:8000", nodeID)
}

func (pm *ProviderManager) probeVLLMAt(baseURL, nodeID string) *ProviderInfo {
	models, err := pm.probeOpenAICompatible(baseURL)
	if err != nil {
		return nil
	}
	for i := range models {
		models[i].Provider = string(ProviderVLLM)
	}
	return &ProviderInfo{
		Type:    ProviderVLLM,
		BaseURL: baseURL,
		Healthy: true,
		Models:  models,
	}
}

func (pm *ProviderManager) probeCustom(baseURL, nodeID string) *ProviderInfo {
	models, err := pm.probeOpenAICompatible(baseURL)
	if err != nil {
		models = nil
	}
	providerType := ProviderCustom
	if models == nil {
		models = []ModelInfo{}
	}
	for i := range models {
		models[i].Provider = string(providerType)
	}
	return &ProviderInfo{
		Type:    providerType,
		BaseURL: baseURL,
		Healthy: true,
		Models:  models,
	}
}

func (pm *ProviderManager) probeOpenAICompatible(baseURL string) ([]ModelInfo, error) {
	resp, err := pm.client.Get(baseURL + "/v1/models")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
		Object string `json:"object"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	models := make([]ModelInfo, 0, len(result.Data))
	for _, m := range result.Data {
		models = append(models, ModelInfo{
			Name: m.ID,
		})
	}
	return models, nil
}

func (pm *ProviderManager) GetAggregatedModels() []ModelInfo {
	nodes := pm.GetNodes(nil)
	seen := make(map[string]bool)
	var all []ModelInfo
	for _, n := range nodes {
		for _, p := range n.Providers {
			for _, m := range p.Models {
				key := m.Provider + ":" + m.Name
				if !seen[key] {
					seen[key] = true
					all = append(all, m)
				}
			}
		}
	}
	return all
}

func (pm *ProviderManager) GetProviderTypes() []ProviderType {
	seen := make(map[ProviderType]bool)
	nodes := pm.GetNodes(nil)
	for _, n := range nodes {
		for _, p := range n.Providers {
			seen[p.Type] = true
		}
	}
	var types []ProviderType
	for t := range seen {
		types = append(types, t)
	}
	return types
}
