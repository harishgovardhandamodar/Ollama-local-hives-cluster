package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HiveAddr   string
	OllamaAddr string
	NodeName   string
	Hardware   string
	GPUModel   string
	GPUMemGB   int
	Capacity   int
}

type Registration struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Address       string   `json:"address"`
	Port          int      `json:"port"`
	Hardware      string   `json:"hardware"`
	GPUModel      string   `json:"gpu_model"`
	GPUMemory     int      `json:"gpu_memory_gb"`
	Capacity      int      `json:"capacity"`
	Models        []string `json:"models"`
	OllamaVersion string   `json:"ollama_version"`
}

type Heartbeat struct {
	NodeID      string   `json:"node_id"`
	CPUUsage    float64  `json:"cpu_usage"`
	MemoryUsed  float64  `json:"memory_used_gb"`
	MemoryTotal float64  `json:"memory_total_gb"`
	VRAMUsed    float64  `json:"vram_used_gb"`
	VRAMTotal   float64  `json:"vram_total_gb"`
	ActiveConns int      `json:"active_connections"`
	Models      []string `json:"models"`
}

func main() {
	cfg := Config{
		HiveAddr:   envOr("HIVE_ADDR", "http://localhost:8080"),
		OllamaAddr: envOr("OLLAMA_ADDR", "http://localhost:11434"),
		NodeName:   envOr("NODE_NAME", ""),
		Hardware:   envOr("HARDWARE", "unknown"),
		GPUModel:   envOr("GPU_MODEL", ""),
		Capacity:   envIntOr("CAPACITY", 5),
	}

	if cfg.NodeName == "" {
		hostname, _ := os.Hostname()
		cfg.NodeName = hostname
	}

	nodeID := envOr("NODE_ID", "")
	if nodeID == "" {
		nodeID = fmt.Sprintf("%s-node", cfg.NodeName)
	}

	if cfg.Hardware == "unknown" {
		cfg.Hardware = detectHardware()
	}

	gpuMem := cfg.GPUMemGB
	if gpuMem == 0 {
		gpuMem = detectGPUMemory()
	}

	log.Printf("=== Hive Node Agent ===")
	log.Printf("Name:      %s", cfg.NodeName)
	log.Printf("Hardware:  %s", cfg.Hardware)
	log.Printf("GPU Model: %s", cfg.GPUModel)
	log.Printf("GPU Mem:   %d GB", gpuMem)
	log.Printf("Capacity:  %d", cfg.Capacity)
	log.Printf("Hive:      %s", cfg.HiveAddr)
	log.Printf("Ollama:    %s", cfg.OllamaAddr)

	ollamaVer := getOllamaVersion()
	models := getOllamaModels()

	reg := Registration{
		ID:            nodeID,
		Name:          cfg.NodeName,
		Address:       getLocalIP(),
		Port:          parsePort(cfg.OllamaAddr),
		Hardware:      cfg.Hardware,
		GPUModel:      cfg.GPUModel,
		GPUMemory:     gpuMem,
		Capacity:      cfg.Capacity,
		Models:        models,
		OllamaVersion: ollamaVer,
	}

	for i := 0; i < 5; i++ {
		if err := register(cfg.HiveAddr, &reg); err != nil {
			log.Printf("Registration attempt %d failed: %v", i+1, err)
			time.Sleep(2 * time.Second)
		} else {
			log.Println("Registered with cluster!")
			break
		}
	}

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		hb := &Heartbeat{
			NodeID:      nodeID,
			CPUUsage:    getCPUUsage(),
			MemoryUsed:  getMemoryUsedGB(),
			MemoryTotal: getMemoryTotalGB(),
			VRAMUsed:    getVRAMUsed(),
			VRAMTotal:   float64(gpuMem),
			Models:      getOllamaModels(),
		}
		if err := sendHeartbeat(cfg.HiveAddr, hb); err != nil {
			log.Printf("Heartbeat failed: %v", err)
			register(cfg.HiveAddr, &reg)
		}
	}
}

func register(addr string, reg *Registration) error {
	data, _ := json.Marshal(reg)
	resp, err := http.Post(addr+"/register", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func sendHeartbeat(addr string, hb *Heartbeat) error {
	data, _ := json.Marshal(hb)
	resp, err := http.Post(addr+"/heartbeat", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func detectHardware() string {
	switch runtime.GOOS {
	case "darwin":
		out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output()
		if err == nil {
			s := string(out)
			if strings.Contains(s, "Apple") {
				return "apple-silicon"
			}
		}
	case "linux":
		out, err := exec.Command("nvidia-smi", "--query-gpu=name", "--format=csv,noheader").Output()
		if err == nil {
			s := strings.TrimSpace(string(out))
			if s != "" {
				if strings.Contains(strings.ToLower(s), "dgx") {
					return "nvidia-dgx"
				}
				return "nvidia-gpu"
			}
		}
	}
	return "cpu"
}

func detectGPUMemory() int {
	out, err := exec.Command("nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader,nounits").Output()
	if err == nil {
		s := strings.TrimSpace(string(out))
		if mb, err := strconv.Atoi(s); err == nil {
			return mb / 1024
		}
	}
	if runtime.GOOS == "darwin" {
		out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
		if err == nil {
			s := strings.TrimSpace(string(out))
			if bytes, err := strconv.ParseInt(s, 10, 64); err == nil {
				return int(bytes/1024/1024/1024) / 3
			}
		}
	}
	return 8
}

func getOllamaVersion() string {
	resp, err := http.Get("http://127.0.0.1:11434/api/version")
	if err != nil {
		return "unknown"
	}
	defer resp.Body.Close()
	var v struct {
		Version string `json:"version"`
	}
	json.NewDecoder(resp.Body).Decode(&v)
	return v.Version
}

func getOllamaModels() []string {
	resp, err := http.Get("http://127.0.0.1:11434/api/tags")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var result struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	models := make([]string, 0)
	for _, m := range result.Models {
		models = append(models, m.Name)
	}
	return models
}

func getCPUUsage() float64 {
	if runtime.GOOS == "linux" {
		out, err := exec.Command("bash", "-c", "top -bn1 | grep 'Cpu(s)' | awk '{print $2}'").Output()
		if err == nil {
			s := strings.TrimSpace(string(out))
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}
	}
	return 0
}

func getMemoryUsedGB() float64 {
	if runtime.GOOS == "linux" {
		out, err := exec.Command("bash", "-c", "free -g | awk '/Mem:/ {print $3}'").Output()
		if err == nil {
			s := strings.TrimSpace(string(out))
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / 1024 / 1024 / 1024
}

func getMemoryTotalGB() float64 {
	if runtime.GOOS == "linux" {
		out, err := exec.Command("bash", "-c", "free -g | awk '/Mem:/ {print $2}'").Output()
		if err == nil {
			s := strings.TrimSpace(string(out))
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}
	}
	return 16
}

func getVRAMUsed() float64 {
	out, err := exec.Command("nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits").Output()
	if err == nil {
		s := strings.TrimSpace(string(out))
		if mb, err := strconv.ParseFloat(s, 64); err == nil {
			return mb / 1024
		}
	}
	return 0
}

func getLocalIP() string {
	addrs, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}
	for _, a := range addrs {
		if a.Flags&net.FlagUp == 0 || a.Flags&net.FlagLoopback != 0 {
			continue
		}
		ifAddrs, _ := a.Addrs()
		for _, addr := range ifAddrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					return ipnet.IP.String()
				}
			}
		}
	}
	return "127.0.0.1"
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func parsePort(addr string) int {
	if u, err := url.Parse(addr); err == nil && u.Port() != "" {
		if p, err := strconv.Atoi(u.Port()); err == nil {
			return p
		}
	}
	return 11434
}
