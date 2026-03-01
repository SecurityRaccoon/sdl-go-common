// Package publisher provides a shared Redis Streams publisher for SDL agents.
//
// It wraps XAdd with optional retry logic (linear or exponential backoff)
// and context-aware sleep. All agents publish JSON data to streams; this
// package standardizes that pattern.
//
// Usage:
//
//	pub := publisher.New(redisClient)
//	err := pub.Publish(ctx, "stream:scan.completed", map[string]interface{}{
//	    "data": jsonString,
//	    "scan_id": "123",
//	})
//
//	// With retry:
//	err := pub.PublishWithRetry(ctx, "stream:output", values, publisher.RetryConfig{
//	    MaxRetries: 3,
//	    BaseDelay:  time.Second,
//	    Strategy:   publisher.Exponential,
//	})
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/sdl-platform/sdl-go-common/pkg/redis"
)

// BackoffStrategy controls how retry delays grow.
type BackoffStrategy int

const (
	// Linear backoff: delay = baseDelay * attempt.
	Linear BackoffStrategy = iota
	// Exponential backoff: delay = baseDelay * 2^(attempt-1).
	Exponential
)

// RetryConfig controls publish retry behavior.
type RetryConfig struct {
	MaxRetries int             // Maximum number of retries (0 = no retries)
	BaseDelay  time.Duration   // Base delay between retries
	Strategy   BackoffStrategy // Linear or Exponential
}

// StreamPublisher publishes messages to Redis Streams.
type StreamPublisher struct {
	client *redis.Client
	logger *slog.Logger
}

// New creates a new StreamPublisher.
func New(client *redis.Client) *StreamPublisher {
	return &StreamPublisher{
		client: client,
		logger: slog.Default(),
	}
}

// Publish publishes a message to a stream (no retries).
func (p *StreamPublisher) Publish(ctx context.Context, stream string, values map[string]interface{}) (string, error) {
	id, err := p.client.XAdd(ctx, stream, values)
	if err != nil {
		return "", fmt.Errorf("failed to publish to %s: %w", stream, err)
	}
	return id, nil
}

// PublishWithRetry publishes a message with configurable retry logic.
// Retries use context-aware sleep (respects cancellation).
func (p *StreamPublisher) PublishWithRetry(ctx context.Context, stream string, values map[string]interface{}, cfg RetryConfig) (string, error) {
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := p.calculateDelay(cfg, attempt)
			p.logger.Debug("Retrying publish",
				slog.String("component", "publisher"),
				slog.String("stream", stream),
				slog.Int("attempt", attempt),
				slog.Duration("delay", delay))

			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}
		}

		id, err := p.client.XAdd(ctx, stream, values)
		if err == nil {
			return id, nil
		}
		lastErr = err
	}

	return "", fmt.Errorf("failed to publish to %s after %d retries: %w", stream, cfg.MaxRetries, lastErr)
}

// PublishJSON marshals payload to JSON, puts it in the given dataKey field,
// merges extraFields, and publishes to the stream.
func (p *StreamPublisher) PublishJSON(ctx context.Context, stream, dataKey string, payload interface{}, extraFields map[string]interface{}) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	values := make(map[string]interface{}, len(extraFields)+1)
	for k, v := range extraFields {
		values[k] = v
	}
	values[dataKey] = string(data)

	return p.Publish(ctx, stream, values)
}

// PublishJSONWithRetry is like PublishJSON but with retry logic.
func (p *StreamPublisher) PublishJSONWithRetry(ctx context.Context, stream, dataKey string, payload interface{}, extraFields map[string]interface{}, cfg RetryConfig) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	values := make(map[string]interface{}, len(extraFields)+1)
	for k, v := range extraFields {
		values[k] = v
	}
	values[dataKey] = string(data)

	return p.PublishWithRetry(ctx, stream, values, cfg)
}

// GetStreamLength returns the number of entries in a stream.
func (p *StreamPublisher) GetStreamLength(ctx context.Context, stream string) (int64, error) {
	return p.client.XLen(ctx, stream)
}

// PublishToPubSub publishes a message to a Redis Pub/Sub channel (fire-and-forget).
func (p *StreamPublisher) PublishToPubSub(ctx context.Context, channel string, message interface{}) error {
	return p.client.Publish(ctx, channel, message)
}

// calculateDelay computes the retry delay for a given attempt.
func (p *StreamPublisher) calculateDelay(cfg RetryConfig, attempt int) time.Duration {
	switch cfg.Strategy {
	case Exponential:
		return cfg.BaseDelay * time.Duration(1<<uint(attempt-1))
	default: // Linear
		return cfg.BaseDelay * time.Duration(attempt)
	}
}
