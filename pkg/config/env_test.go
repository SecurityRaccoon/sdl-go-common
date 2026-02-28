package config

import (
	"testing"
	"time"
)

func TestGetEnv(t *testing.T) {
	t.Run("returns env value when set", func(t *testing.T) {
		t.Setenv("TEST_VAR", "hello")
		if got := GetEnv("TEST_VAR", "default"); got != "hello" {
			t.Errorf("GetEnv() = %q, want %q", got, "hello")
		}
	})

	t.Run("returns default when not set", func(t *testing.T) {
		if got := GetEnv("NONEXISTENT_VAR_XYZ", "fallback"); got != "fallback" {
			t.Errorf("GetEnv() = %q, want %q", got, "fallback")
		}
	})

	t.Run("returns default when empty", func(t *testing.T) {
		t.Setenv("EMPTY_VAR", "")
		if got := GetEnv("EMPTY_VAR", "default"); got != "default" {
			t.Errorf("GetEnv() = %q, want %q", got, "default")
		}
	})
}

func TestGetIntEnv(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		setEnv     bool
		defaultVal int
		want       int
	}{
		{"valid int", "42", true, 0, 42},
		{"negative int", "-5", true, 0, -5},
		{"not set", "", false, 10, 10},
		{"invalid string", "abc", true, 10, 10},
		{"float string", "3.14", true, 10, 10},
		{"empty string", "", true, 10, 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setEnv {
				t.Setenv("TEST_INT", tc.envValue)
			}
			got := GetIntEnv("TEST_INT", tc.defaultVal)
			if got != tc.want {
				t.Errorf("GetIntEnv() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestGetDurationEnv(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		setEnv     bool
		defaultVal time.Duration
		want       time.Duration
	}{
		{"go duration string", "30s", true, time.Second, 30 * time.Second},
		{"minutes", "5m", true, time.Second, 5 * time.Minute},
		{"integer seconds fallback", "45", true, time.Second, 45 * time.Second},
		{"not set", "", false, 10 * time.Second, 10 * time.Second},
		{"empty string", "", true, 10 * time.Second, 10 * time.Second},
		{"invalid string", "abc", true, 10 * time.Second, 10 * time.Second},
		{"zero seconds", "0", true, 5 * time.Second, 0},
		{"complex duration", "1h30m", true, time.Second, 90 * time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setEnv {
				t.Setenv("TEST_DUR", tc.envValue)
			}
			got := GetDurationEnv("TEST_DUR", tc.defaultVal)
			if got != tc.want {
				t.Errorf("GetDurationEnv() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGetBoolEnv(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		setEnv     bool
		defaultVal bool
		want       bool
	}{
		{"true", "true", true, false, true},
		{"false", "false", true, true, false},
		{"1", "1", true, false, true},
		{"0", "0", true, true, false},
		{"TRUE", "TRUE", true, false, true},
		{"not set", "", false, true, true},
		{"invalid", "maybe", true, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setEnv {
				t.Setenv("TEST_BOOL", tc.envValue)
			}
			got := GetBoolEnv("TEST_BOOL", tc.defaultVal)
			if got != tc.want {
				t.Errorf("GetBoolEnv() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGetFloatEnv(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		setEnv     bool
		defaultVal float64
		want       float64
	}{
		{"valid float", "3.14", true, 0, 3.14},
		{"integer", "42", true, 0, 42.0},
		{"negative", "-1.5", true, 0, -1.5},
		{"not set", "", false, 9.99, 9.99},
		{"invalid", "abc", true, 9.99, 9.99},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setEnv {
				t.Setenv("TEST_FLOAT", tc.envValue)
			}
			got := GetFloatEnv("TEST_FLOAT", tc.defaultVal)
			if got != tc.want {
				t.Errorf("GetFloatEnv() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBuildRedisURL(t *testing.T) {
	t.Run("uses REDIS_URL directly", func(t *testing.T) {
		t.Setenv("REDIS_URL", "redis://custom:6380/2")
		got := BuildRedisURL()
		if got != "redis://custom:6380/2" {
			t.Errorf("BuildRedisURL() = %q, want %q", got, "redis://custom:6380/2")
		}
	})

	t.Run("builds from components without password", func(t *testing.T) {
		t.Setenv("REDIS_URL", "")
		t.Setenv("REDIS_HOST", "myhost")
		t.Setenv("REDIS_PORT", "6380")
		t.Setenv("REDIS_DB", "3")
		t.Setenv("REDIS_PASSWORD", "")
		got := BuildRedisURL()
		if got != "redis://myhost:6380/3" {
			t.Errorf("BuildRedisURL() = %q, want %q", got, "redis://myhost:6380/3")
		}
	})

	t.Run("builds from components with password", func(t *testing.T) {
		t.Setenv("REDIS_URL", "")
		t.Setenv("REDIS_HOST", "myhost")
		t.Setenv("REDIS_PORT", "6380")
		t.Setenv("REDIS_DB", "1")
		t.Setenv("REDIS_PASSWORD", "s3cret")
		got := BuildRedisURL()
		if got != "redis://:s3cret@myhost:6380/1" {
			t.Errorf("BuildRedisURL() = %q, want %q", got, "redis://:s3cret@myhost:6380/1")
		}
	})

	t.Run("uses defaults when no env set", func(t *testing.T) {
		t.Setenv("REDIS_URL", "")
		t.Setenv("REDIS_HOST", "")
		t.Setenv("REDIS_PORT", "")
		t.Setenv("REDIS_DB", "")
		t.Setenv("REDIS_PASSWORD", "")
		got := BuildRedisURL()
		if got != "redis://localhost:6379/0" {
			t.Errorf("BuildRedisURL() = %q, want %q", got, "redis://localhost:6379/0")
		}
	})
}

func TestGenerateConsumerName(t *testing.T) {
	t.Run("generates stable name with prefix", func(t *testing.T) {
		name := GenerateConsumerName("scan-aggregate")
		if name == "" {
			t.Fatal("GenerateConsumerName() returned empty string")
		}
		// Should start with prefix
		if len(name) <= len("scan-aggregate-") {
			t.Errorf("name %q too short", name)
		}
		if name[:len("scan-aggregate-")] != "scan-aggregate-" {
			t.Errorf("name %q should start with %q", name, "scan-aggregate-")
		}
	})

	t.Run("is stable across calls", func(t *testing.T) {
		a := GenerateConsumerName("test")
		b := GenerateConsumerName("test")
		if a != b {
			t.Errorf("expected stable name, got %q and %q", a, b)
		}
	})
}
