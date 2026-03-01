package publisher

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	pub := New(nil)
	if pub == nil {
		t.Fatal("expected non-nil publisher")
	}
	if pub.logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestCalculateDelay_Linear(t *testing.T) {
	pub := New(nil)
	cfg := RetryConfig{
		BaseDelay: time.Second,
		Strategy:  Linear,
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 3 * time.Second},
	}

	for _, tt := range tests {
		result := pub.calculateDelay(cfg, tt.attempt)
		if result != tt.expected {
			t.Errorf("Linear attempt %d: got %v, want %v", tt.attempt, result, tt.expected)
		}
	}
}

func TestCalculateDelay_Exponential(t *testing.T) {
	pub := New(nil)
	cfg := RetryConfig{
		BaseDelay: time.Second,
		Strategy:  Exponential,
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 1 * time.Second}, // 1s * 2^0
		{2, 2 * time.Second}, // 1s * 2^1
		{3, 4 * time.Second}, // 1s * 2^2
		{4, 8 * time.Second}, // 1s * 2^3
	}

	for _, tt := range tests {
		result := pub.calculateDelay(cfg, tt.attempt)
		if result != tt.expected {
			t.Errorf("Exponential attempt %d: got %v, want %v", tt.attempt, result, tt.expected)
		}
	}
}

func TestBackoffStrategy_Constants(t *testing.T) {
	// Ensure constants are distinct
	if Linear == Exponential {
		t.Error("Linear and Exponential should be different values")
	}
}
