package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type ConfigFile struct {
	Server struct {
		Port            int    `yaml:"port"`
		OllamaURL       string `yaml:"ollama_url"`
		OllamaModel     string `yaml:"ollama_model"`
		MaxConcurrent   int    `yaml:"max_concurrent"`
		MaxClients      int    `yaml:"max_clients"`
		ReadTimeout     int    `yaml:"read_timeout"`
		WriteTimeout    int    `yaml:"write_timeout"`
		IdleTimeout     int    `yaml:"idle_timeout"`
		APIKey          string `yaml:"api_key"`
	} `yaml:"server"`
	Mesh struct {
		Enabled         bool   `yaml:"enabled"`
		DiscoveryPort   int    `yaml:"discovery_port"`
		AnnounceAddress string `yaml:"announce_address"`
		SeedPeers       string `yaml:"seed_peers"`
		ModelMap        string `yaml:"model_map"`
	} `yaml:"mesh"`
	Database struct {
		Path string `yaml:"path"`
	} `yaml:"database"`
	Cache struct {
		Enabled     bool `yaml:"enabled"`
		MaxEntries  int  `yaml:"max_entries"`
		TTLSeconds  int  `yaml:"ttl_seconds"`
	} `yaml:"cache"`
	Logging struct {
		JSON bool `yaml:"json"`
	} `yaml:"logging"`
	CustomProviders []string `yaml:"custom_providers"`
}

func LoadConfigFile(path string) (*ConfigFile, error) {
	if path == "" {
		path = os.Getenv("HIVE_CONFIG")
	}
	if path == "" {
		path = "hive.yaml"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No config file, use env vars
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg ConfigFile
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return &cfg, nil
}

// envOrDefault returns env value or default, used to merge file config with env overrides
func envOrDefault(key string, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envOrDefaultInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return defaultVal
}
