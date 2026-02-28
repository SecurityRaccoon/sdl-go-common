// Package redis provides a Redis client wrapper for SDL agents.
//
// It wraps github.com/redis/go-redis/v9 with convenience methods for Streams,
// consumer groups, and common key-value operations used across the platform.
//
// Usage:
//
//	client, err := redis.NewClient("redis://localhost:6379")
//	defer client.Close()
//	err = client.EnsureConsumerGroup(ctx, "stream:scan.completed", "my-workers")
//	streams, err := client.XReadGroup(ctx, "my-workers", "consumer-1", "stream:scan.completed", 10, 5*time.Second)
package redis

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client wraps the Redis client with helper methods for Streams and key-value operations.
type Client struct {
	client *redis.Client
}

// NewClient creates a new Redis client from a URL string.
// It parses the URL, optionally overrides the DB from REDIS_DB env var,
// and verifies connectivity with a 5-second ping.
func NewClient(redisURL string) (*Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	// Override DB if REDIS_DB env var is set
	if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
		if db, err := strconv.Atoi(dbStr); err == nil {
			opts.DB = db
		}
	}

	client := redis.NewClient(opts)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &Client{client: client}, nil
}

// Close closes the Redis connection.
func (c *Client) Close() error {
	return c.client.Close()
}

// GetClient returns the underlying go-redis client for advanced operations.
func (c *Client) GetClient() *redis.Client {
	return c.client
}

// Ping checks if Redis is reachable.
func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// --- Stream Operations ---

// EnsureConsumerGroup creates a consumer group if it doesn't exist.
// It handles the BUSYGROUP error gracefully when the group already exists.
func (c *Client) EnsureConsumerGroup(ctx context.Context, stream, group string) error {
	err := c.client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("failed to create consumer group %q on stream %q: %w", group, stream, err)
	}
	return nil
}

// XAdd adds an entry to a stream.
func (c *Client) XAdd(ctx context.Context, stream string, values map[string]interface{}) (string, error) {
	return c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}).Result()
}

// XReadGroup reads from a stream using a consumer group.
func (c *Client) XReadGroup(ctx context.Context, group, consumer, stream string, count int64, block time.Duration) ([]redis.XStream, error) {
	return c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
}

// XAck acknowledges one or more messages in a stream.
func (c *Client) XAck(ctx context.Context, stream, group string, ids ...string) error {
	return c.client.XAck(ctx, stream, group, ids...).Err()
}

// --- Key-Value Operations ---

// Set stores a key-value pair with optional expiration.
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return c.client.Set(ctx, key, value, expiration).Err()
}

// Get retrieves a value by key.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.client.Get(ctx, key).Result()
}

// Del deletes one or more keys.
func (c *Client) Del(ctx context.Context, keys ...string) error {
	return c.client.Del(ctx, keys...).Err()
}

// Expire sets a TTL on a key.
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return c.client.Expire(ctx, key, expiration).Err()
}

// Keys returns all keys matching a pattern.
// Prefer SCAN in production for large key spaces.
func (c *Client) Keys(ctx context.Context, pattern string) ([]string, error) {
	return c.client.Keys(ctx, pattern).Result()
}

// --- Hash Operations ---

// HSet stores a field-value pair in a hash.
func (c *Client) HSet(ctx context.Context, key, field string, value interface{}) error {
	return c.client.HSet(ctx, key, field, value).Err()
}

// HGet retrieves a field value from a hash.
func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	return c.client.HGet(ctx, key, field).Result()
}

// HGetAll retrieves all field-value pairs from a hash.
func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.client.HGetAll(ctx, key).Result()
}

// HLen returns the number of fields in a hash.
func (c *Client) HLen(ctx context.Context, key string) (int64, error) {
	return c.client.HLen(ctx, key).Result()
}

// --- Pub/Sub Operations ---

// Publish publishes a message to a Pub/Sub channel.
func (c *Client) Publish(ctx context.Context, channel string, message interface{}) error {
	return c.client.Publish(ctx, channel, message).Err()
}
