package publisher

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"

	sharedredis "github.com/sdl-platform/sdl-go-common/pkg/redis"
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

func TestPublishAndHelpers(t *testing.T) {
	ctx := context.Background()
	pub, store := newTestPublisher(t)

	id, err := pub.Publish(ctx, "stream:scan.completed", map[string]interface{}{
		"scan_id": "scan-1",
		"status":  "completed",
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if id == "" {
		t.Fatal("Publish() returned empty ID")
	}

	jsonID, err := pub.PublishJSON(ctx, "stream:scan.completed", "data", map[string]string{"hello": "world"}, map[string]interface{}{
		"scan_id": "scan-2",
	})
	if err != nil {
		t.Fatalf("PublishJSON() error = %v", err)
	}
	if jsonID == "" {
		t.Fatal("PublishJSON() returned empty ID")
	}

	length, err := pub.GetStreamLength(ctx, "stream:scan.completed")
	if err != nil {
		t.Fatalf("GetStreamLength() error = %v", err)
	}
	if length != 2 {
		t.Fatalf("GetStreamLength() = %d, want 2", length)
	}

	entries, err := store.GetClient().XRange(ctx, "stream:scan.completed", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange() error = %v", err)
	}
	if got := entries[1].Values["scan_id"]; got != "scan-2" {
		t.Fatalf("PublishJSON() scan_id = %v, want scan-2", got)
	}
	if got := entries[1].Values["data"]; got != `{"hello":"world"}` {
		t.Fatalf("PublishJSON() data = %v, want JSON payload", got)
	}

	if err := pub.PublishToPubSub(ctx, "channel:scan", "done"); err != nil {
		t.Fatalf("PublishToPubSub() error = %v", err)
	}
}

func TestPublishJSONWithRetry(t *testing.T) {
	ctx := context.Background()
	pub, store := newTestPublisher(t)

	id, err := pub.PublishJSONWithRetry(ctx, "stream:scan.completed", "data", map[string]string{"hello": "retry"}, nil, RetryConfig{
		MaxRetries: 1,
		BaseDelay:  time.Millisecond,
		Strategy:   Linear,
	})
	if err != nil {
		t.Fatalf("PublishJSONWithRetry() error = %v", err)
	}
	if id == "" {
		t.Fatal("PublishJSONWithRetry() returned empty ID")
	}

	length, err := store.XLen(ctx, "stream:scan.completed")
	if err != nil {
		t.Fatalf("XLen() error = %v", err)
	}
	if length != 1 {
		t.Fatalf("XLen() = %d, want 1", length)
	}
}

func TestPublishJSONMarshalError(t *testing.T) {
	pub := New(nil)

	if _, err := pub.PublishJSON(context.Background(), "stream:scan.completed", "data", func() {}, nil); err == nil {
		t.Fatal("PublishJSON() expected marshal error")
	}

	if _, err := pub.PublishJSONWithRetry(context.Background(), "stream:scan.completed", "data", func() {}, nil, RetryConfig{}); err == nil {
		t.Fatal("PublishJSONWithRetry() expected marshal error")
	}
}

func TestPublishWithRetryCancelledContext(t *testing.T) {
	pub, store := newTestPublisher(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := pub.PublishWithRetry(ctx, "stream:scan.completed", map[string]interface{}{"scan_id": "scan-1"}, RetryConfig{
		MaxRetries: 2,
		BaseDelay:  time.Second,
		Strategy:   Exponential,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("PublishWithRetry() error = %v, want context.Canceled", err)
	}
}

func newTestPublisher(t *testing.T) (*StreamPublisher, *sharedredis.Client) {
	t.Helper()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(server.Close)

	client, err := sharedredis.NewClient("redis://" + server.Addr())
	if err != nil {
		t.Fatalf("redis.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil && !strings.Contains(err.Error(), "client is closed") {
			t.Fatalf("redis.Client.Close() error = %v", err)
		}
	})

	return New(client), client
}
