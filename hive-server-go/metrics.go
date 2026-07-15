package main

import (
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type GPUInfo struct {
	Model     string  `json:"model"`
	MemoryGB  int     `json:"memory_gb"`
	VRAMUsed  float64 `json:"vram_used_gb"`
	VRAMTotal float64 `json:"vram_total_gb"`
	DriverVer string  `json:"driver_version"`
}

type SystemMetrics struct {
	mu           sync.RWMutex
	gpuInfo      []GPUInfo
	hasNvidia    bool
	lastCheck    time.Time
	checkInterval time.Duration
}

func NewSystemMetrics() *SystemMetrics {
	return &SystemMetrics{
		checkInterval: 15 * time.Second,
	}
}

func (sm *SystemMetrics) GetGPUInfo() []GPUInfo {
	sm.mu.RLock()
	age := time.Since(sm.lastCheck)
	sm.mu.RUnlock()
	if age > sm.checkInterval {
		sm.refresh()
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	out := make([]GPUInfo, len(sm.gpuInfo))
	copy(out, sm.gpuInfo)
	return out
}

func (sm *SystemMetrics) HasNvidia() bool {
	sm.mu.RLock()
	age := time.Since(sm.lastCheck)
	sm.mu.RUnlock()
	if age > sm.checkInterval {
		sm.refresh()
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.hasNvidia
}

func (sm *SystemMetrics) refresh() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.lastCheck = time.Now()

	gpus := detectNvidiaGPU()
	sm.gpuInfo = gpus
	sm.hasNvidia = len(gpus) > 0
}

func detectNvidiaGPU() []GPUInfo {
	out, err := exec.Command("nvidia-smi",
		"--query-gpu=index,name,memory.total,memory.used,driver_version",
		"--format=csv,noheader,nounits").Output()
	if err != nil {
		return nil
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var gpus []GPUInfo
	for _, line := range lines {
		parts := strings.Split(line, ", ")
		if len(parts) < 5 {
			continue
		}
		memTotalMB, _ := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
		memUsedMB, _ := strconv.ParseFloat(strings.TrimSpace(parts[3]), 64)
		gpus = append(gpus, GPUInfo{
			Model:     strings.TrimSpace(parts[1]),
			MemoryGB:  int(memTotalMB / 1024),
			VRAMTotal: memTotalMB / 1024,
			VRAMUsed:  memUsedMB / 1024,
			DriverVer: strings.TrimSpace(parts[4]),
		})
	}
	return gpus
}

type HardwareInfo struct {
	Platform     string    `json:"platform"`
	Architecture string    `json:"architecture"`
	Hostname     string    `json:"hostname"`
	GPUs         []GPUInfo `json:"gpus,omitempty"`
}

func getHardwareInfo() HardwareInfo {
	sm := NewSystemMetrics()
	gpus := sm.GetGPUInfo()
	return HardwareInfo{
		Platform:     runtime.GOOS,
		Architecture: runtime.GOARCH,
		GPUs:         gpus,
	}
}

func getOllamaVersion() string {
	out, err := exec.Command("ollama", "--version").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}
