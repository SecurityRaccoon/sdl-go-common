package consumer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/sdl-platform/sdl-go-common/pkg/redis"
)

func newTestClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	clock = time.Now()
	mr.SetTime(clock)
	c, err := redis.NewClient("redis://" + mr.Addr())
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return mr, c
}

// seedPending adds n messages and delivers them to consumer "old-pod" without
// ACK, so they sit in the group PEL like messages from a failed handler.
func seedPending(t *testing.T, c *redis.Client, stream, group, consumer string, n int) []string {
	t.Helper()
	ctx := context.Background()
	if err := c.EnsureConsumerGroup(ctx, stream, group); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; i < n; i++ {
		id, err := c.XAdd(ctx, stream, map[string]interface{}{"data": `{"scan_id":"s1"}`})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if _, err := c.XReadGroup(ctx, group, consumer, stream, int64(n), 0); err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestNew_RedeliveryDefaults(t *testing.T) {
	sc := New(nil, Config{Stream: "stream:x", Group: "g", Consumer: "c", Handler: func(context.Context, goredis.XMessage) error { return nil }})
	if sc.config.ClaimInterval != 60*time.Second {
		t.Errorf("ClaimInterval = %v", sc.config.ClaimInterval)
	}
	if sc.config.MaxDeliveries != 5 {
		t.Errorf("MaxDeliveries = %d", sc.config.MaxDeliveries)
	}
	if sc.config.DLQStream != "stream:x.dlq" {
		t.Errorf("DLQStream = %q", sc.config.DLQStream)
	}
	if sc.config.DLQMaxLen != 10000 {
		t.Errorf("DLQMaxLen = %d", sc.config.DLQMaxLen)
	}
}

func TestReclaim_RetriesOwnAndOthersPending(t *testing.T) {
	mr, c := newTestClient(t)
	ctx := context.Background()
	seedPending(t, c, "stream:a", "g", "old-pod", 2)
	seedPending(t, c, "stream:a", "g", "me", 1) // own pending (previously skipped)
	advance(mr, 10*time.Minute)

	var handled int32
	sc := New(c, Config{Stream: "stream:a", Group: "g", Consumer: "me", ClaimMinIdle: time.Minute,
		Handler: func(context.Context, goredis.XMessage) error { atomic.AddInt32(&handled, 1); return nil }})
	if err := sc.claimOldPendingMessages(ctx); err != nil {
		t.Fatal(err)
	}
	if handled != 3 {
		t.Fatalf("handled = %d, want 3", handled)
	}
	n, _ := sc.GetPendingCount(ctx)
	if n != 0 {
		t.Fatalf("pending = %d, want 0", n)
	}
}

func TestReclaim_SkipsMessagesNotIdleYet(t *testing.T) {
	_, c := newTestClient(t)
	ctx := context.Background()
	seedPending(t, c, "stream:b", "g", "old-pod", 1)
	var handled int32
	sc := New(c, Config{Stream: "stream:b", Group: "g", Consumer: "me", ClaimMinIdle: time.Hour,
		Handler: func(context.Context, goredis.XMessage) error { atomic.AddInt32(&handled, 1); return nil }})
	if err := sc.claimOldPendingMessages(ctx); err != nil {
		t.Fatal(err)
	}
	if handled != 0 {
		t.Fatalf("handled = %d, want 0", handled)
	}
}

func TestReclaim_DeadLettersAfterMaxDeliveries(t *testing.T) {
	mr, c := newTestClient(t)
	ctx := context.Background()
	seedPending(t, c, "stream:c", "g", "old-pod", 1) // delivery 1
	var handled int32
	sc := New(c, Config{Stream: "stream:c", Group: "g", Consumer: "me", ClaimMinIdle: time.Minute, MaxDeliveries: 3,
		Handler: func(context.Context, goredis.XMessage) error {
			atomic.AddInt32(&handled, 1)
			return errors.New("backend down")
		}})
	for i := 0; i < 4; i++ {
		advance(mr, 2*time.Minute)
		if err := sc.claimOldPendingMessages(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// deliveries 2 and 3 run the handler; the 4th claim exceeds max=3.
	if handled != 2 {
		t.Fatalf("handled = %d, want 2", handled)
	}
	n, _ := sc.GetPendingCount(ctx)
	if n != 0 {
		t.Fatalf("pending = %d, want 0 after dead-lettering", n)
	}
	dlq, err := c.GetClient().XRange(ctx, "stream:c.dlq", "-", "+").Result()
	if err != nil || len(dlq) != 1 {
		t.Fatalf("dlq entries = %d err=%v", len(dlq), err)
	}
	v := dlq[0].Values
	if v["error_type"] != "MaxDeliveriesExceeded" || v["original_stream"] != "stream:c" || v["consumer_group"] != "g" {
		t.Fatalf("unexpected dlq entry: %v", v)
	}
	if v["data"] != `{"data":"{\"scan_id\":\"s1\"}"}` {
		t.Fatalf("data = %v", v["data"])
	}
}

func TestReclaim_DisabledMaxDeliveriesKeepsRetrying(t *testing.T) {
	mr, c := newTestClient(t)
	ctx := context.Background()
	seedPending(t, c, "stream:d", "g", "old-pod", 1)
	var handled int32
	sc := New(c, Config{Stream: "stream:d", Group: "g", Consumer: "me", ClaimMinIdle: time.Minute, MaxDeliveries: -1,
		Handler: func(context.Context, goredis.XMessage) error { atomic.AddInt32(&handled, 1); return errors.New("x") }})
	for i := 0; i < 8; i++ {
		advance(mr, 2*time.Minute)
		_ = sc.claimOldPendingMessages(ctx)
	}
	if handled != 8 {
		t.Fatalf("handled = %d, want 8", handled)
	}
}

func TestRecoverPending_DeadLettersOverLimit(t *testing.T) {
	_, c := newTestClient(t)
	ctx := context.Background()
	seedPending(t, c, "stream:e", "g", "me", 1)
	// Simulate 5 previous deliveries to this consumer.
	for i := 0; i < 4; i++ {
		if _, err := c.XClaim(ctx, &goredis.XClaimArgs{Stream: "stream:e", Group: "g", Consumer: "me", Messages: []string{mustFirstPending(t, c, "stream:e", "g")}}); err != nil {
			t.Fatal(err)
		}
	}
	var handled int32
	sc := New(c, Config{Stream: "stream:e", Group: "g", Consumer: "me",
		Handler: func(context.Context, goredis.XMessage) error { atomic.AddInt32(&handled, 1); return nil }})
	if err := sc.processPendingMessages(ctx); err != nil {
		t.Fatal(err)
	}
	if handled != 0 {
		t.Fatalf("handled = %d, want 0 (over limit)", handled)
	}
	if n, _ := c.XLen(ctx, "stream:e.dlq"); n != 1 {
		t.Fatalf("dlq len = %d", n)
	}
}

func mustFirstPending(t *testing.T, c *redis.Client, stream, group string) string {
	t.Helper()
	p, err := c.XPendingExt(context.Background(), &goredis.XPendingExtArgs{Stream: stream, Group: group, Start: "-", End: "+", Count: 1})
	if err != nil || len(p) == 0 {
		t.Fatalf("no pending: %v", err)
	}
	return p[0].ID
}

func TestDLQData_SummarisesLargePayload(t *testing.T) {
	big := make([]byte, 100000)
	for i := range big {
		big[i] = 'x'
	}
	out, truncated, size := dlqData(map[string]interface{}{"data": string(big), "scan_id": "s9"}, 1024)
	if !truncated || size < 100000 || len(out) > 1024 {
		t.Fatalf("truncated=%v size=%d len=%d", truncated, size, len(out))
	}
}

// advance moves miniredis' clock (stream idle times use it; FastForward only affects TTLs).
func advance(mr *miniredis.Miniredis, d time.Duration) {
	clock = clock.Add(d)
	mr.SetTime(clock)
}

var clock = time.Now()
