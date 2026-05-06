package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestValidateAgainstSchema_WhenCanonicalPayloads_ThenSucceeds(t *testing.T) {
	now := time.Date(2026, 5, 4, 18, 0, 0, 0, time.UTC)
	finding := Finding{
		ID:          "finding-1",
		RuleID:      "rule-1",
		Severity:    SeverityHigh,
		File:        "internal/worker.go",
		Line:        12,
		EndLine:     14,
		Message:     "Potential SQL injection",
		Tool:        "semgrep",
		Fingerprint: "fp-1",
	}

	tests := []struct {
		name       string
		schemaName string
		payload    any
	}{
		{
			name:       "finding",
			schemaName: SchemaFinding,
			payload:    finding,
		},
		{
			name:       "scan completed event",
			schemaName: SchemaScanCompletedEvent,
			payload: ScanCompletedEvent{
				JobID:         "job-1",
				ScanID:        "scan-1",
				CorrelationID: "corr-1",
				ScannerType:   "semgrep",
				Target:        "https://github.com/sdl-platform/sdl",
				Status:        StatusCompleted,
				Findings:      []Finding{finding},
				CompletedAt:   now,
			},
		},
		{
			name:       "scan prep completed payload",
			schemaName: SchemaScanPrepCompletedEvent,
			payload: ScanPrepCompletedEvent{
				ScanID:          "scan-1",
				OrganizationID:  "org_123",
				CorrelationID:   "corr-1",
				SuccessfulCount: 1,
				PrepCompletedAt: now,
				SuccessfulRepositories: []RepositoryInfo{{
					URL:       "https://github.com/sdl-platform/sdl",
					Ref:       "main",
					ProjectID: "proj-1",
					JobID:     "job-1",
				}},
			},
		},
		{
			name:       "aggregated result",
			schemaName: SchemaAggregatedResult,
			payload: AggregatedResult{
				ScanID:                "scan-1",
				OrganizationID:        "org_123",
				CorrelationID:         "corr-1",
				Status:                StatusCompleted,
				TotalRepositories:     1,
				CompletedRepositories: 1,
				TotalFindings:         1,
				BySeverity:            map[string]int{SeverityHigh: 1},
				Findings:              []Finding{finding},
				Repositories:          []string{"https://github.com/sdl-platform/sdl"},
				StartedAt:             now.Add(-5 * time.Minute),
				CompletedAt:           now,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateAgainstSchema(tt.schemaName, tt.payload); err != nil {
				t.Fatalf("ValidateAgainstSchema() error = %v", err)
			}
		})
	}
}

func TestValidateJSONAgainstSchema_WhenRequiredFieldMissing_ThenFails(t *testing.T) {
	payload := []byte(`{"scanner_type":"semgrep","status":"completed","findings":[],"completed_at":"2026-05-04T18:00:00Z","target":"https://github.com/sdl-platform/sdl"}`)

	err := ValidateJSONAgainstSchema(SchemaScanCompletedEvent, payload)
	if err == nil {
		t.Fatal("ValidateJSONAgainstSchema() error = nil, want validation failure")
	}

	if !strings.Contains(err.Error(), "scan_id") {
		t.Fatalf("expected missing scan_id in error, got %v", err)
	}
}

func TestValidateJSONAgainstSchema_WhenCorrelationIDMissing_ThenFails(t *testing.T) {
	payload := []byte(`{"scan_id":"scan-1","scanner_type":"semgrep","status":"completed","findings":[],"completed_at":"2026-05-04T18:00:00Z","target":"https://github.com/sdl-platform/sdl"}`)

	err := ValidateJSONAgainstSchema(SchemaScanCompletedEvent, payload)
	if err == nil {
		t.Fatal("ValidateJSONAgainstSchema() error = nil, want validation failure")
	}

	if !strings.Contains(err.Error(), "correlation_id") {
		t.Fatalf("expected missing correlation_id in error, got %v", err)
	}
}

func TestValidateAgainstSchema_WhenPayloadCannotMarshal_ThenFails(t *testing.T) {
	err := ValidateAgainstSchema(SchemaFinding, map[string]any{"bad": func() {}})
	if err == nil {
		t.Fatal("ValidateAgainstSchema() error = nil, want marshal failure")
	}
}

func TestValidateJSONAgainstSchema_WhenUnknownSchema_ThenFails(t *testing.T) {
	payload, err := json.Marshal(map[string]string{"ok": "true"})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	err = ValidateJSONAgainstSchema("does-not-exist", payload)
	if err == nil {
		t.Fatal("ValidateJSONAgainstSchema() error = nil, want unknown schema failure")
	}
}
