package consumer

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func TestNew_Defaults(t *testing.T) {
	// New should set reasonable defaults when zero values are passed
	handler := func(ctx context.Context, msg goredis.XMessage) error {
		return nil
	}

	sc := New(nil, Config{
		Stream:   "test-stream",
		Group:    "test-group",
		Consumer: "test-consumer",
		Handler:  handler,
	})

	if sc.config.BlockTimeout != 5*time.Second {
		t.Errorf("expected BlockTimeout 5s, got %v", sc.config.BlockTimeout)
	}
	if sc.config.Count != 1 {
		t.Errorf("expected Count 1, got %d", sc.config.Count)
	}
	if sc.config.ErrorDelay != 1*time.Second {
		t.Errorf("expected ErrorDelay 1s, got %v", sc.config.ErrorDelay)
	}
	if sc.config.ClaimMinIdle != 5*time.Minute {
		t.Errorf("expected ClaimMinIdle 5m, got %v", sc.config.ClaimMinIdle)
	}
	if sc.config.ClaimBatch != 100 {
		t.Errorf("expected ClaimBatch 100, got %d", sc.config.ClaimBatch)
	}
}

func TestNew_CustomValues(t *testing.T) {
	handler := func(ctx context.Context, msg goredis.XMessage) error {
		return nil
	}

	sc := New(nil, Config{
		Stream:       "test-stream",
		Group:        "test-group",
		Consumer:     "test-consumer",
		Handler:      handler,
		BlockTimeout: 10 * time.Second,
		Count:        5,
		ErrorDelay:   2 * time.Second,
		ClaimMinIdle: 10 * time.Minute,
		ClaimBatch:   50,
	})

	if sc.config.BlockTimeout != 10*time.Second {
		t.Errorf("expected BlockTimeout 10s, got %v", sc.config.BlockTimeout)
	}
	if sc.config.Count != 5 {
		t.Errorf("expected Count 5, got %d", sc.config.Count)
	}
	if sc.config.ErrorDelay != 2*time.Second {
		t.Errorf("expected ErrorDelay 2s, got %v", sc.config.ErrorDelay)
	}
	if sc.config.ClaimMinIdle != 10*time.Minute {
		t.Errorf("expected ClaimMinIdle 10m, got %v", sc.config.ClaimMinIdle)
	}
	if sc.config.ClaimBatch != 50 {
		t.Errorf("expected ClaimBatch 50, got %d", sc.config.ClaimBatch)
	}
}

func TestParseEventData_DataField(t *testing.T) {
	msg := goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"data": `{"scan_id":"123"}`,
		},
	}

	data, err := ParseEventData(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != `{"scan_id":"123"}` {
		t.Errorf("expected data field content, got %s", string(data))
	}
}

func TestParseEventData_EventField(t *testing.T) {
	msg := goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"event": `{"type":"ai.context.enriched"}`,
		},
	}

	data, err := ParseEventData(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != `{"type":"ai.context.enriched"}` {
		t.Errorf("expected event field content, got %s", string(data))
	}
}

func TestParseEventData_FallbackMarshal(t *testing.T) {
	msg := goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"scan_id": "abc",
			"status":  "completed",
		},
	}

	data, err := ParseEventData(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should be valid JSON containing both fields
	if len(data) == 0 {
		t.Error("expected non-empty data from fallback marshal")
	}
}

func TestParseEventData_DataFieldPriority(t *testing.T) {
	// When both "data" and "event" are present, "data" takes priority
	msg := goredis.XMessage{
		ID: "1-0",
		Values: map[string]interface{}{
			"data":  `{"source":"data"}`,
			"event": `{"source":"event"}`,
		},
	}

	data, err := ParseEventData(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != `{"source":"data"}` {
		t.Errorf("expected data field to take priority, got %s", string(data))
	}
}
