package main

import (
	"os"
	"runtime"
)

func getHardwareInfo() map[string]interface{} {
	info := map[string]interface{}{
		"platform":     runtime.GOOS,
		"architecture": runtime.GOARCH,
		"hostname":     getHostname(),
	}

	// Detect GPUs
	gpus := detectGPUs()
	if len(gpus) > 0 {
		info["gpus"] = gpus
	}

	return info
}

func getHostname() string {
	hostname, _ := os.Hostname()
	return hostname
}

func detectGPUs() []map[string]interface{} {
	var gpus []map[string]interface{}

	// Check for NVIDIA GPU via nvidia-smi
	if nvidiaGPU := detectNvidiaGPU(); nvidiaGPU != "" {
		gpus = append(gpus, map[string]interface{}{
			"model": nvidiaGPU,
			"type":  "nvidia",
		})
	}

	// Check for AMD/Intel via /dev/dri
	if _, err := os.Stat("/dev/dri/renderD128"); err == nil {
		gpus = append(gpus, map[string]interface{}{
			"model": "DRI GPU",
			"type":  "dri",
		})
	}

	return gpus
}

func detectNvidiaGPU() string {
	// Simple detection - in production you'd parse nvidia-smi output
	// For now, just check if nvidia-smi exists
	if _, err := os.Stat("/usr/bin/nvidia-smi"); err == nil {
		return "NVIDIA GPU (detected)"
	}
	return ""
}

// getAcceleratorType returns the accelerator type for the current platform
func getAcceleratorType() string {
	if runtime.GOOS == "linux" {
		// Check for GPU
		if nvidiaGPU := detectNvidiaGPU(); nvidiaGPU != "" {
			return "GPU"
		}
		if _, err := os.Stat("/dev/dri/renderD128"); err == nil {
			return "GPU"
		}
	}
	return "CPU"
}
