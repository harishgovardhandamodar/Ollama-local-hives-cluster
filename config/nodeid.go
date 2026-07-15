package config

import (
	"crypto/rand"
	"fmt"
	"os"
)

func generateNodeID() string {
	b := make([]byte, 8)
	rand.Read(b)
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%x", hostname, b)
}
