// Package consumer provides a shared Redis Streams consumer for Argus agents.
//
// It implements the common consumption loop pattern used across all Go agents:
// create consumer group, read messages via XReadGroup, process via callback,
// acknowledge on success.
//
// Redelivery is on by default: every ClaimInterval (60s) the consumer runs
// XAUTOCLAIM over the whole group PEL (its own and other consumers' messages
// idle longer than ClaimMinIdle) and retries them. A message delivered more
// than MaxDeliveries times is copied to a size-capped dead-letter stream
// ({stream}.dlq, approx MAXLEN DLQMaxLen) and acknowledged, so one failed call
// can no longer leave a message pending until the pod restarts.
//
// Usage:
//
//	c := consumer.New(redisClient, consumer.Config{
//	    Stream:        "stream:scan.completed",
//	    Group:         "scan-aggregate-workers",
//	    Consumer:      "scan-aggregate-pod-abc",
//	    Handler:       func(ctx context.Context, msg redis.XMessage) error { ... },
//	})
//	err := c.Start(ctx)
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/sdl-platform/sdl-go-common/pkg/redis"
)

// MessageHandler processes a single Redis Stream message.
// Return nil to acknowledge the message; return an error to skip acknowledgement
// (message stays pending for retry or DLQ handling).
type MessageHandler func(ctx context.Context, msg goredis.XMessage) error

// DLQHandler is called when a message should be sent to the dead letter queue.
// originalErr is the processing error that caused the DLQ send.
type DLQHandler func(ctx context.Context, msg goredis.XMessage, originalErr error)

// Config configures a StreamConsumer.
type Config struct {
	// Required fields
	Stream   string         // Redis stream name
	Group    string         // Consumer group name
	Consumer string         // Consumer name (should be stable per pod)
	Handler  MessageHandler // Callback for each message

	// Optional: tuning
	BlockTimeout time.Duration // How long to block on XReadGroup (default: 5s)
	Count        int64         // Messages per XReadGroup call (default: 1)
	ErrorDelay   time.Duration // Sleep duration after read errors (default: 1s)

	// Optional: per-message timeout
	MessageTimeout time.Duration // If >0, each message gets context.WithTimeout

	// Optional: DLQ
	DLQHandler DLQHandler // If set, enables DLQ handling

	// Optional: pending recovery on startup
	RecoverPending bool // If true, processes pending messages on startup via XPENDING+XCLAIM

	// Periodic redelivery (XAUTOCLAIM of own and other consumers' idle messages).
	ClaimInterval time.Duration // Reclaim cycle (default 60s; negative disables)
	ClaimMinIdle  time.Duration // Minimum idle time before a pending message is retried (default: 5m)
	ClaimBatch    int64         // Max messages to claim per cycle (default: 100)

	// Max deliveries before dead-lettering (default 5; negative disables).
	MaxDeliveries int64
	// Dead-letter stream for messages over MaxDeliveries (default "{Stream}.dlq").
	DLQStream string
	// Approximate MAXLEN for the dead-letter stream (default 10000).
	DLQMaxLen int64
	// Payloads larger than this are stored as a summary in the DLQ (default 64KiB).
	DLQMaxInlineBytes int

	// Optional: metrics/observability hooks
	OnProcessed func(stream, status string, duration time.Duration) // Called after each message
	OnError     func(operation, detail string)                      // Called on errors
}

// Defaults for redelivery and dead-lettering.
const (
	DefaultClaimInterval     = 60 * time.Second
	DefaultMaxDeliveries     = 5
	DefaultDLQMaxLen         = 10000
	DefaultDLQMaxInlineBytes = 64 * 1024
)

// StreamConsumer consumes messages from a Redis Stream using consumer groups.
type StreamConsumer struct {
	client *redis.Client
	config Config
	logger *slog.Logger
}

// New creates a new StreamConsumer with the given configuration.
func New(client *redis.Client, cfg Config) *StreamConsumer {
	// Apply defaults
	if cfg.BlockTimeout == 0 {
		cfg.BlockTimeout = 5 * time.Second
	}
	if cfg.Count == 0 {
		cfg.Count = 1
	}
	if cfg.ErrorDelay == 0 {
		cfg.ErrorDelay = 1 * time.Second
	}
	if cfg.ClaimMinIdle == 0 {
		cfg.ClaimMinIdle = 5 * time.Minute
	}
	if cfg.ClaimBatch == 0 {
		cfg.ClaimBatch = 100
	}
	if cfg.ClaimInterval == 0 {
		cfg.ClaimInterval = DefaultClaimInterval
	}
	if cfg.MaxDeliveries == 0 {
		cfg.MaxDeliveries = DefaultMaxDeliveries
	}
	if cfg.DLQStream == "" {
		cfg.DLQStream = cfg.Stream + ".dlq"
	}
	if cfg.DLQMaxLen == 0 {
		cfg.DLQMaxLen = DefaultDLQMaxLen
	}
	if cfg.DLQMaxInlineBytes == 0 {
		cfg.DLQMaxInlineBytes = DefaultDLQMaxInlineBytes
	}

	return &StreamConsumer{
		client: client,
		config: cfg,
		logger: slog.Default(),
	}
}

// Start runs the consumer loop. It blocks until ctx is cancelled.
func (sc *StreamConsumer) Start(ctx context.Context) error {
	// Create consumer group
	if err := sc.client.EnsureConsumerGroup(ctx, sc.config.Stream, sc.config.Group); err != nil {
		sc.logger.Error("Failed to create consumer group",
			slog.String("component", "consumer"),
			slog.String("stream", sc.config.Stream),
			slog.String("group", sc.config.Group),
			slog.Any("error", err))
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	// Recover pending messages from previous session
	if sc.config.RecoverPending {
		if err := sc.processPendingMessages(ctx); err != nil {
			sc.logger.Warn("Failed to recover pending messages on startup",
				slog.String("component", "consumer"),
				slog.String("consumer", sc.config.Consumer),
				slog.Any("error", err))
			// Don't fail startup
		}
	}

	sc.logger.Info("Consumer started",
		slog.String("component", "consumer"),
		slog.String("consumer", sc.config.Consumer),
		slog.String("stream", sc.config.Stream),
		slog.String("group", sc.config.Group))

	// Periodic redelivery of idle pending messages (own and other consumers').
	if sc.config.ClaimInterval > 0 {
		if err := sc.claimOldPendingMessages(ctx); err != nil {
			sc.logger.Warn("Failed to reclaim pending messages",
				slog.String("component", "consumer"),
				slog.Any("error", err))
		}
		claimTicker := time.NewTicker(sc.config.ClaimInterval)
		defer claimTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				sc.logger.Info("Consumer shutting down", slog.String("component", "consumer"))
				return ctx.Err()
			case <-claimTicker.C:
				if err := sc.claimOldPendingMessages(ctx); err != nil {
					sc.logger.Warn("Failed to claim old pending messages",
						slog.String("component", "consumer"),
						slog.Any("error", err))
				}
			default:
				if err := sc.readAndProcess(ctx); err != nil {
					sc.logger.Error("Error in consume loop",
						slog.String("component", "consumer"),
						slog.Any("error", err))
					time.Sleep(sc.config.ErrorDelay)
				}
			}
		}
	}

	// Simple loop (no claim ticker)
	for {
		select {
		case <-ctx.Done():
			sc.logger.Info("Consumer shutting down", slog.String("component", "consumer"))
			return ctx.Err()
		default:
			if err := sc.readAndProcess(ctx); err != nil {
				sc.logger.Error("Error in consume loop",
					slog.String("component", "consumer"),
					slog.Any("error", err))
				time.Sleep(sc.config.ErrorDelay)
			}
		}
	}
}

// readAndProcess reads messages via XReadGroup and processes them.
func (sc *StreamConsumer) readAndProcess(ctx context.Context) error {
	streams, err := sc.client.XReadGroup(
		ctx,
		sc.config.Group,
		sc.config.Consumer,
		sc.config.Stream,
		sc.config.Count,
		sc.config.BlockTimeout,
	)
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil // No messages, normal timeout
		}
		return fmt.Errorf("failed to read from stream: %w", err)
	}

	for _, stream := range streams {
		for _, msg := range stream.Messages {
			sc.handleMessage(ctx, msg)
		}
	}
	return nil
}

// handleMessage processes a single message with optional timeout, ACK, and DLQ.
func (sc *StreamConsumer) handleMessage(ctx context.Context, msg goredis.XMessage) {
	startTime := time.Now()

	// Optional per-message timeout
	msgCtx := ctx
	if sc.config.MessageTimeout > 0 {
		var cancel context.CancelFunc
		msgCtx, cancel = context.WithTimeout(ctx, sc.config.MessageTimeout)
		defer cancel()
	}

	err := sc.config.Handler(msgCtx, msg)
	duration := time.Since(startTime)

	if err != nil {
		sc.logger.Error("Failed to process message",
			slog.String("component", "consumer"),
			slog.String("message_id", msg.ID),
			slog.Any("error", err))

		if sc.config.OnError != nil {
			sc.config.OnError("process_message", err.Error())
		}
		if sc.config.OnProcessed != nil {
			sc.config.OnProcessed(sc.config.Stream, "error", duration)
		}

		// DLQ: acknowledge the poison message and send to DLQ
		if sc.config.DLQHandler != nil {
			sc.config.DLQHandler(ctx, msg, err)
			if ackErr := sc.client.XAck(ctx, sc.config.Stream, sc.config.Group, msg.ID); ackErr != nil {
				sc.logger.Warn("Failed to ACK message after DLQ",
					slog.String("component", "consumer"),
					slog.String("message_id", msg.ID),
					slog.Any("error", ackErr))
			}
		}
		// If no DLQ handler, message stays pending for retry
		return
	}

	// ACK on success
	if err := sc.client.XAck(ctx, sc.config.Stream, sc.config.Group, msg.ID); err != nil {
		sc.logger.Warn("Failed to acknowledge message",
			slog.String("component", "consumer"),
			slog.String("message_id", msg.ID),
			slog.Any("error", err))
	}

	if sc.config.OnProcessed != nil {
		sc.config.OnProcessed(sc.config.Stream, "success", duration)
	}
}

// processPendingMessages recovers messages pending for this consumer on startup.
func (sc *StreamConsumer) processPendingMessages(ctx context.Context) error {
	pending, err := sc.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream:   sc.config.Stream,
		Group:    sc.config.Group,
		Consumer: sc.config.Consumer,
		Start:    "-",
		End:      "+",
		Count:    sc.config.ClaimBatch,
	})
	if err != nil {
		return fmt.Errorf("failed to check pending messages: %w", err)
	}

	if len(pending) == 0 {
		return nil
	}

	sc.logger.Info("Recovering pending messages from previous session",
		slog.String("component", "consumer"),
		slog.String("consumer", sc.config.Consumer),
		slog.Int("count", len(pending)))

	for _, p := range pending {
		if sc.overDeliveryLimit(p.RetryCount + 1) {
			sc.deadLetter(ctx, p.ID, p.RetryCount)
			continue
		}
		messages, err := sc.client.XClaim(ctx, &goredis.XClaimArgs{
			Stream:   sc.config.Stream,
			Group:    sc.config.Group,
			Consumer: sc.config.Consumer,
			MinIdle:  0,
			Messages: []string{p.ID},
		})
		if err != nil {
			sc.logger.Error("Failed to claim pending message",
				slog.String("component", "consumer"),
				slog.String("message_id", p.ID),
				slog.Any("error", err))
			continue
		}

		for _, msg := range messages {
			sc.handleMessage(ctx, msg)
		}
	}

	return nil
}

// claimOldPendingMessages retries pending messages idle longer than
// ClaimMinIdle, from any consumer in the group including this one (a message
// whose handler failed stays in this consumer's PEL and was previously only
// retried on restart). It walks the PEL with XAUTOCLAIM, one batch per cycle.
// Messages delivered more than MaxDeliveries times are dead-lettered and ACKed.
func (sc *StreamConsumer) claimOldPendingMessages(ctx context.Context) error {
	rdb := sc.client.GetClient()
	start := "0-0"
	claimed := 0
	for {
		messages, next, err := rdb.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
			Stream:   sc.config.Stream,
			Group:    sc.config.Group,
			Consumer: sc.config.Consumer,
			MinIdle:  sc.config.ClaimMinIdle,
			Start:    start,
			Count:    sc.config.ClaimBatch,
		}).Result()
		if err != nil {
			return fmt.Errorf("xautoclaim: %w", err)
		}
		for _, msg := range messages {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			claimed++
			if msg.Values == nil {
				// Entry was trimmed from the stream; nothing to retry.
				_ = sc.client.XAck(ctx, sc.config.Stream, sc.config.Group, msg.ID)
				continue
			}
			deliveries := sc.deliveryCount(ctx, msg.ID)
			if sc.overDeliveryLimit(deliveries) {
				sc.deadLetterMessage(ctx, msg, deliveries-1)
				continue
			}
			sc.logger.Info("Retrying pending message",
				slog.String("component", "consumer"),
				slog.String("stream", sc.config.Stream),
				slog.String("message_id", msg.ID),
				slog.Int64("delivery", deliveries))
			sc.handleMessage(ctx, msg)
		}
		// One full batch per cycle keeps the read loop responsive; the cursor
		// restarts at 0-0 next cycle, already-retried messages are not idle yet.
		if next == "0-0" || next == "" || int64(claimed) >= sc.config.ClaimBatch {
			break
		}
		start = next
	}
	if claimed > 0 {
		sc.logger.Info("Reclaimed pending messages",
			slog.String("component", "consumer"),
			slog.String("stream", sc.config.Stream),
			slog.String("group", sc.config.Group),
			slog.Int("count", claimed))
	}
	return nil
}

// deliveryCount returns the delivery count of a pending message (after the
// claim that just happened). Returns 0 if it cannot be determined.
func (sc *StreamConsumer) deliveryCount(ctx context.Context, id string) int64 {
	p, err := sc.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: sc.config.Stream,
		Group:  sc.config.Group,
		Start:  id,
		End:    id,
		Count:  1,
	})
	if err != nil || len(p) == 0 {
		return 0
	}
	return p[0].RetryCount
}

func (sc *StreamConsumer) overDeliveryLimit(deliveries int64) bool {
	return sc.config.MaxDeliveries > 0 && deliveries > sc.config.MaxDeliveries
}

// deadLetter loads a pending message by id and dead-letters it.
func (sc *StreamConsumer) deadLetter(ctx context.Context, id string, attempts int64) {
	msgs, err := sc.client.GetClient().XRangeN(ctx, sc.config.Stream, id, id, 1).Result()
	if err != nil || len(msgs) == 0 {
		// Trimmed or unreadable: drop it from the PEL so it stops counting.
		_ = sc.client.XAck(ctx, sc.config.Stream, sc.config.Group, id)
		return
	}
	sc.deadLetterMessage(ctx, msgs[0], attempts)
}

// deadLetterMessage copies a message to the capped DLQ stream and ACKs it.
// If a DLQHandler is configured it is called instead of the built-in writer.
func (sc *StreamConsumer) deadLetterMessage(ctx context.Context, msg goredis.XMessage, attempts int64) {
	reason := fmt.Errorf("max deliveries exceeded: delivered %d times (max=%d)", attempts, sc.config.MaxDeliveries)
	if sc.config.DLQHandler != nil {
		sc.config.DLQHandler(ctx, msg, reason)
	} else if err := sc.writeDLQ(ctx, msg, reason, attempts); err != nil {
		// Keep the message pending rather than lose it if the DLQ write fails.
		sc.logger.Error("Failed to dead-letter message",
			slog.String("component", "consumer"),
			slog.String("stream", sc.config.Stream),
			slog.String("message_id", msg.ID),
			slog.Any("error", err))
		if sc.config.OnError != nil {
			sc.config.OnError("dead_letter", err.Error())
		}
		return
	}
	if err := sc.client.XAck(ctx, sc.config.Stream, sc.config.Group, msg.ID); err != nil {
		sc.logger.Warn("Failed to ACK dead-lettered message",
			slog.String("component", "consumer"),
			slog.String("message_id", msg.ID),
			slog.Any("error", err))
	}
	sc.logger.Warn("Message dead-lettered",
		slog.String("component", "consumer"),
		slog.String("stream", sc.config.Stream),
		slog.String("group", sc.config.Group),
		slog.String("dlq_stream", sc.config.DLQStream),
		slog.String("message_id", msg.ID),
		slog.Int64("deliveries", attempts))
	if sc.config.OnProcessed != nil {
		sc.config.OnProcessed(sc.config.Stream, "dead_lettered", 0)
	}
}

// writeDLQ appends a capped DLQ entry. Large bodies are replaced by per-field
// sizes; the original stays addressable by original_stream/original_id.
func (sc *StreamConsumer) writeDLQ(ctx context.Context, msg goredis.XMessage, reason error, attempts int64) error {
	data, truncated, size := dlqData(msg.Values, sc.config.DLQMaxInlineBytes)
	values := map[string]interface{}{
		"original_stream": sc.config.Stream,
		"original_id":     msg.ID,
		"consumer_group":  sc.config.Group,
		"consumer":        sc.config.Consumer,
		"error_type":      "MaxDeliveriesExceeded",
		"error_message":   reason.Error(),
		"retry_count":     fmt.Sprintf("%d", attempts),
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
		"data":            data,
	}
	if truncated {
		values["data_truncated"] = "true"
		values["data_bytes"] = fmt.Sprintf("%d", size)
	}
	return sc.client.GetClient().XAdd(ctx, &goredis.XAddArgs{
		Stream: sc.config.DLQStream,
		MaxLen: sc.config.DLQMaxLen,
		Approx: true,
		Values: values,
	}).Err()
}

func dlqData(values map[string]interface{}, maxInline int) (string, bool, int) {
	raw, err := json.Marshal(values)
	if err != nil {
		return "{}", true, 0
	}
	if len(raw) <= maxInline {
		return string(raw), false, len(raw)
	}
	summary := map[string]interface{}{"_truncated": true}
	sizes := map[string]int{}
	for k, v := range values {
		str := fmt.Sprint(v)
		sizes[k] = len(str)
		if len(str) <= 256 {
			summary[k] = str
		}
	}
	summary["_field_bytes"] = sizes
	out, _ := json.Marshal(summary)
	return string(out), true, len(raw)
}

// GetPendingCount returns the number of pending messages for this consumer group.
func (sc *StreamConsumer) GetPendingCount(ctx context.Context) (int64, error) {
	pending, err := sc.client.XPending(ctx, sc.config.Stream, sc.config.Group)
	if err != nil {
		return 0, fmt.Errorf("xpending: %w", err)
	}
	return pending.Count, nil
}

// ParseEventData extracts event data from a Redis message using the standard
// 3-way fallback: "data" field -> "event" field -> marshal all values.
func ParseEventData(msg goredis.XMessage) ([]byte, error) {
	if data, ok := msg.Values["data"].(string); ok {
		return []byte(data), nil
	}
	if data, ok := msg.Values["event"].(string); ok {
		return []byte(data), nil
	}
	// Fallback: marshal all values
	data, err := json.Marshal(msg.Values)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message values: %w", err)
	}
	return data, nil
}
