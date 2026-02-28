package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestK8sContextHandler_EnrichesRecords(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := &K8sContextHandler{
		handler: base,
		k8sContext: &K8sContext{
			PodName:     "my-pod-abc123",
			PodIP:       "10.0.0.5",
			Namespace:   "default",
			NodeName:    "node-1",
			ServiceName: "scan-agent",
		},
	}

	logger := slog.New(h)
	logger.Info("test message", slog.String("extra", "value"))

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse log output: %v\nbuf: %s", err, buf.String())
	}

	checks := map[string]string{
		"pod":       "my-pod-abc123",
		"pod_ip":    "10.0.0.5",
		"namespace": "default",
		"node":      "node-1",
		"service":   "scan-agent",
		"extra":     "value",
		"msg":       "test message",
	}
	for key, want := range checks {
		got, ok := entry[key].(string)
		if !ok {
			t.Errorf("missing key %q in log output", key)
			continue
		}
		if got != want {
			t.Errorf("key %q = %q, want %q", key, got, want)
		}
	}
}

func TestK8sContextHandler_SkipsEmptyFields(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	h := &K8sContextHandler{
		handler: base,
		k8sContext: &K8sContext{
			PodName: "only-pod",
			// All others empty
		},
	}

	logger := slog.New(h)
	logger.Info("sparse context")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse log output: %v", err)
	}

	if _, ok := entry["pod"]; !ok {
		t.Error("expected 'pod' in output")
	}
	for _, key := range []string{"pod_ip", "namespace", "node", "service"} {
		if _, ok := entry[key]; ok {
			t.Errorf("unexpected key %q in output for empty value", key)
		}
	}
}

func TestK8sContextHandler_NilContext(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	h := &K8sContextHandler{
		handler:    base,
		k8sContext: nil,
	}

	logger := slog.New(h)
	logger.Info("nil context test")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse log output: %v", err)
	}
	if entry["msg"] != "nil context test" {
		t.Errorf("unexpected msg: %v", entry["msg"])
	}
}

func TestK8sContextHandler_Enabled(t *testing.T) {
	base := slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelWarn})
	h := &K8sContextHandler{handler: base, k8sContext: &K8sContext{}}

	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("DEBUG should not be enabled at WARN level")
	}
	if !h.Enabled(context.Background(), slog.LevelError) {
		t.Error("ERROR should be enabled at WARN level")
	}
}

func TestK8sContextHandler_WithAttrs(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	h := &K8sContextHandler{
		handler:    base,
		k8sContext: &K8sContext{PodName: "p1"},
	}

	h2 := h.WithAttrs([]slog.Attr{slog.String("component", "worker")})
	logger := slog.New(h2)
	logger.Info("with-attrs test")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse: %v", err)
	}
	if entry["component"] != "worker" {
		t.Errorf("expected component=worker, got %v", entry["component"])
	}
	if entry["pod"] != "p1" {
		t.Errorf("expected pod=p1, got %v", entry["pod"])
	}
}

func TestK8sContextHandler_WithGroup(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	h := &K8sContextHandler{
		handler:    base,
		k8sContext: &K8sContext{PodName: "p1"},
	}

	h2 := h.WithGroup("mygroup")
	logger := slog.New(h2)
	logger.Info("grouped", slog.String("key", "val"))

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse: %v", err)
	}
	// K8s context attrs are injected via Handle, so they end up inside the group
	// when the underlying handler has a group set. The important thing is that
	// the handler still works and doesn't panic.
	if entry["msg"] != "grouped" {
		t.Errorf("expected msg=grouped, got %v", entry["msg"])
	}
	// Verify the group contains the user attr
	group, ok := entry["mygroup"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected mygroup to be a map, got %T", entry["mygroup"])
	}
	if group["key"] != "val" {
		t.Errorf("expected mygroup.key=val, got %v", group["key"])
	}
}

func TestGetLevelFromEnv(t *testing.T) {
	tests := []struct {
		envValue string
		want     slog.Level
	}{
		{"", slog.LevelInfo},
		{"DEBUG", slog.LevelDebug},
		{"debug", slog.LevelDebug},
		{"INFO", slog.LevelInfo},
		{"WARN", slog.LevelWarn},
		{"WARNING", slog.LevelWarn},
		{"ERROR", slog.LevelError},
		{"unknown", slog.LevelInfo},
	}
	for _, tc := range tests {
		t.Run("LOG_LEVEL="+tc.envValue, func(t *testing.T) {
			t.Setenv("LOG_LEVEL", tc.envValue)
			got := getLevelFromEnv()
			if got != tc.want {
				t.Errorf("getLevelFromEnv() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestInitK8sContext(t *testing.T) {
	t.Setenv("POD_NAME", "test-pod")
	t.Setenv("POD_IP", "10.0.0.1")
	t.Setenv("NAMESPACE", "staging")
	t.Setenv("NODE_NAME", "node-2")
	t.Setenv("SERVICE_NAME", "my-svc")

	// Reset global for test
	k8sContext = nil
	initK8sContext()

	if k8sContext.PodName != "test-pod" {
		t.Errorf("PodName = %q, want %q", k8sContext.PodName, "test-pod")
	}
	if k8sContext.PodIP != "10.0.0.1" {
		t.Errorf("PodIP = %q, want %q", k8sContext.PodIP, "10.0.0.1")
	}
	if k8sContext.Namespace != "staging" {
		t.Errorf("Namespace = %q, want %q", k8sContext.Namespace, "staging")
	}
	if k8sContext.NodeName != "node-2" {
		t.Errorf("NodeName = %q, want %q", k8sContext.NodeName, "node-2")
	}
	if k8sContext.ServiceName != "my-svc" {
		t.Errorf("ServiceName = %q, want %q", k8sContext.ServiceName, "my-svc")
	}
}

func TestInitK8sContext_HostnameFallback(t *testing.T) {
	t.Setenv("POD_NAME", "")
	t.Setenv("HOSTNAME", "fallback-host")

	k8sContext = nil
	initK8sContext()

	if k8sContext.PodName != "fallback-host" {
		t.Errorf("PodName = %q, want %q (HOSTNAME fallback)", k8sContext.PodName, "fallback-host")
	}
}

func TestSetDefault_NilSafe(t *testing.T) {
	// Should not panic
	SetDefault(nil)
}
