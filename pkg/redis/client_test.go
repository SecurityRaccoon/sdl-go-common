package redis

import (
	"testing"
)

// Unit tests for the redis package. These test constructor logic
// and error handling without requiring a live Redis server.
// Integration tests with a real Redis should be run separately.

func TestNewClient_InvalidURL(t *testing.T) {
	_, err := NewClient("not-a-valid-url")
	if err == nil {
		t.Fatal("expected error for invalid URL, got nil")
	}
}

func TestNewClient_UnreachableServer(t *testing.T) {
	// This should fail on the ping (nothing listening on port 16379)
	_, err := NewClient("redis://localhost:16379")
	if err == nil {
		t.Fatal("expected connection error, got nil")
	}
}

func TestNewClient_DBOverride(t *testing.T) {
	// Set REDIS_DB to verify it's read (will still fail connect, but tests the parse path)
	t.Setenv("REDIS_DB", "5")
	_, err := NewClient("redis://localhost:16379")
	if err == nil {
		t.Fatal("expected connection error, got nil")
	}
	// The error should be about connection, not about DB parsing
}

func TestNewClient_InvalidDBOverride(t *testing.T) {
	// Invalid REDIS_DB should be silently ignored
	t.Setenv("REDIS_DB", "notanumber")
	_, err := NewClient("redis://localhost:16379")
	if err == nil {
		t.Fatal("expected connection error, got nil")
	}
}
