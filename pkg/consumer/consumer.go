// Package consumer provides a shared Redis Streams consumer for Argus agents.
//
// It implements the common consumption loop pattern used across all Go agents:
// create consumer group, read messages via XReadGroup, process via callback,
// acknowledge on success. Advanced features (DLQ, pending recovery, dead
// consumer claiming) are opt-in via Config fields.
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

	// Optional: dead consumer claiming
	ClaimInterval time.Duration // If >0, periodically claims old pending messages from dead consumers
	ClaimMinIdle  time.Duration // Minimum idle time before claiming (default: 5m)
	ClaimBatch    int64         // Max messages to claim per cycle (default: 100)

	// Optional: metrics/observability hooks
	OnProcessed func(stream, status string, duration time.Duration) // Called after each message
	OnError     func(operation, detail string)                      // Called on errors
}

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

	// Optional: periodic dead consumer claiming
	if sc.config.ClaimInterval > 0 {
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

// claimOldPendingMessages claims messages from dead/idle consumers.
func (sc *StreamConsumer) claimOldPendingMessages(ctx context.Context) error {
	pending, err := sc.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: sc.config.Stream,
		Group:  sc.config.Group,
		Start:  "-",
		End:    "+",
		Count:  sc.config.ClaimBatch,
		Idle:   sc.config.ClaimMinIdle,
	})
	if err != nil {
		return fmt.Errorf("failed to check pending messages: %w", err)
	}

	if len(pending) == 0 {
		return nil
	}

	sc.logger.Info("Found old pending messages from dead consumers",
		slog.String("component", "consumer"),
		slog.Int("count", len(pending)))

	for _, p := range pending {
		if p.Consumer == sc.config.Consumer {
			continue // Skip own pending messages
		}

		sc.logger.Info("Claiming message from dead consumer",
			slog.String("component", "consumer"),
			slog.String("message_id", p.ID),
			slog.String("original_consumer", p.Consumer),
			slog.Duration("idle_time", p.Idle))

		messages, err := sc.client.XClaim(ctx, &goredis.XClaimArgs{
			Stream:   sc.config.Stream,
			Group:    sc.config.Group,
			Consumer: sc.config.Consumer,
			MinIdle:  sc.config.ClaimMinIdle,
			Messages: []string{p.ID},
		})
		if err != nil {
			sc.logger.Error("Failed to claim message from dead consumer",
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
