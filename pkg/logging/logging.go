// Package logging provides structured JSON logging with Kubernetes context enrichment for SDL agents.
//
// Usage:
//
//	logger := logging.New()
//	logging.SetDefault(logger)
//	logger.Info("Starting service", slog.String("component", "consumer"))
//
// The logger enriches all log records with Kubernetes context (pod name, namespace, pod IP,
// node name, service name) read from standard downward API environment variables.
//
// Environment variables:
//   - LOG_LEVEL: DEBUG, INFO, WARN, ERROR (default: INFO)
//   - LOG_FORMAT: json, text (default: json)
//   - POD_NAME / HOSTNAME: Pod identifier
//   - POD_IP: Pod IP address
//   - NAMESPACE: Kubernetes namespace
//   - NODE_NAME: Node the pod is running on
//   - SERVICE_NAME: Service name label
package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
)

var (
	initOnce   sync.Once
	handler    slog.Handler
	k8sContext *K8sContext
)

// K8sContext holds Kubernetes context information for logging.
type K8sContext struct {
	PodName     string
	PodIP       string
	Namespace   string
	NodeName    string
	ServiceName string
}

// initK8sContext initializes Kubernetes context from environment variables.
func initK8sContext() {
	k8sContext = &K8sContext{
		PodName:     os.Getenv("POD_NAME"),
		PodIP:       os.Getenv("POD_IP"),
		Namespace:   os.Getenv("NAMESPACE"),
		NodeName:    os.Getenv("NODE_NAME"),
		ServiceName: os.Getenv("SERVICE_NAME"),
	}
	// Fallback to HOSTNAME for pod name if not set
	if k8sContext.PodName == "" {
		k8sContext.PodName = os.Getenv("HOSTNAME")
	}
}

// K8sContextHandler wraps an slog.Handler and enriches log records with Kubernetes context.
type K8sContextHandler struct {
	handler    slog.Handler
	k8sContext *K8sContext
}

// Enabled implements slog.Handler.
func (h *K8sContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

// Handle implements slog.Handler and enriches records with K8s context.
func (h *K8sContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.k8sContext != nil {
		if h.k8sContext.PodName != "" {
			r.AddAttrs(slog.String("pod", h.k8sContext.PodName))
		}
		if h.k8sContext.Namespace != "" {
			r.AddAttrs(slog.String("namespace", h.k8sContext.Namespace))
		}
		if h.k8sContext.PodIP != "" {
			r.AddAttrs(slog.String("pod_ip", h.k8sContext.PodIP))
		}
		if h.k8sContext.NodeName != "" {
			r.AddAttrs(slog.String("node", h.k8sContext.NodeName))
		}
		if h.k8sContext.ServiceName != "" {
			r.AddAttrs(slog.String("service", h.k8sContext.ServiceName))
		}
	}
	return h.handler.Handle(ctx, r)
}

// WithAttrs implements slog.Handler.
func (h *K8sContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &K8sContextHandler{
		handler:    h.handler.WithAttrs(attrs),
		k8sContext: h.k8sContext,
	}
}

// WithGroup implements slog.Handler.
func (h *K8sContextHandler) WithGroup(name string) slog.Handler {
	return &K8sContextHandler{
		handler:    h.handler.WithGroup(name),
		k8sContext: h.k8sContext,
	}
}

// getLevelFromEnv returns the log level from LOG_LEVEL environment variable.
func getLevelFromEnv() slog.Level {
	level := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	switch level {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO", "":
		return slog.LevelInfo
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// New returns a structured logger with Kubernetes context enrichment.
// The handler is initialized once and reused for all subsequent calls.
// Format (json/text) is controlled by the LOG_FORMAT environment variable.
func New() *slog.Logger {
	initOnce.Do(func() {
		initK8sContext()
		level := getLevelFromEnv()

		logFormat := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_FORMAT")))
		opts := &slog.HandlerOptions{Level: level}

		var baseHandler slog.Handler
		switch logFormat {
		case "text":
			baseHandler = slog.NewTextHandler(os.Stdout, opts)
		default:
			baseHandler = slog.NewJSONHandler(os.Stdout, opts)
		}

		handler = &K8sContextHandler{
			handler:    baseHandler,
			k8sContext: k8sContext,
		}
	})

	return slog.New(handler)
}

// SetDefault configures the global slog default logger.
func SetDefault(logger *slog.Logger) {
	if logger != nil {
		slog.SetDefault(logger)
	}
}
