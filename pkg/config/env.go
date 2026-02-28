// Package config provides environment variable helpers for SDL agent configuration.
//
// These helpers eliminate the copy-paste of getEnv/getIntEnv/getDurationEnv across agents.
//
// Usage:
//
//	redisURL := config.GetEnv("REDIS_URL", "redis://localhost:6379")
//	timeout := config.GetDurationEnv("CLONE_TIMEOUT", 30*time.Second)
//	retries := config.GetIntEnv("MAX_RETRIES", 3)
//	redisURL := config.BuildRedisURL()
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// GetEnv retrieves an environment variable or returns a default value.
func GetEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// GetIntEnv retrieves an integer environment variable or returns a default value.
func GetIntEnv(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

// GetDurationEnv retrieves a duration environment variable or returns a default value.
// It first tries to parse as a Go duration string (e.g., "30s", "5m"),
// then falls back to interpreting as seconds (e.g., "30" → 30s).
func GetDurationEnv(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	if d, err := time.ParseDuration(value); err == nil {
		return d
	}
	// Fallback: parse as integer seconds
	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(seconds) * time.Second
	}
	return defaultValue
}

// GetBoolEnv retrieves a boolean environment variable or returns a default value.
func GetBoolEnv(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return defaultValue
}

// GetFloatEnv retrieves a float64 environment variable or returns a default value.
func GetFloatEnv(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
	}
	return defaultValue
}

// BuildRedisURL constructs a Redis URL from environment variables.
// Priority: REDIS_URL (direct) > REDIS_HOST/REDIS_PORT/REDIS_PASSWORD/REDIS_DB (Kubernetes pattern).
func BuildRedisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}

	host := GetEnv("REDIS_HOST", "localhost")
	port := GetEnv("REDIS_PORT", "6379")
	db := GetEnv("REDIS_DB", "0")

	if password := os.Getenv("REDIS_PASSWORD"); password != "" {
		return fmt.Sprintf("redis://:%s@%s:%s/%s", password, host, port, db)
	}
	return fmt.Sprintf("redis://%s:%s/%s", host, port, db)
}

// GenerateConsumerName creates a stable hostname-based consumer name.
// Using a stable name prevents dead consumer accumulation on pod restarts.
func GenerateConsumerName(prefix string) string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s-%s", prefix, hostname)
}
