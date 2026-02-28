package events

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSeverityRank(t *testing.T) {
	tests := []struct {
		severity string
		expected int
	}{
		{SeverityCritical, 0},
		{SeverityHigh, 1},
		{SeverityMedium, 2},
		{SeverityLow, 3},
		{SeverityInfo, 4},
		{"UNKNOWN", 999},
		{"", 999},
	}

	for _, tt := range tests {
		t.Run(tt.severity, func(t *testing.T) {
			got := SeverityRank(tt.severity)
			if got != tt.expected {
				t.Errorf("SeverityRank(%q) = %d, want %d", tt.severity, got, tt.expected)
			}
		})
	}
}

func TestSeverityConstants(t *testing.T) {
	if SeverityCritical != "CRITICAL" {
		t.Errorf("SeverityCritical = %q, want CRITICAL", SeverityCritical)
	}
	if SeverityHigh != "HIGH" {
		t.Errorf("SeverityHigh = %q, want HIGH", SeverityHigh)
	}
	if SeverityMedium != "MEDIUM" {
		t.Errorf("SeverityMedium = %q, want MEDIUM", SeverityMedium)
	}
	if SeverityLow != "LOW" {
		t.Errorf("SeverityLow = %q, want LOW", SeverityLow)
	}
	if SeverityInfo != "INFO" {
		t.Errorf("SeverityInfo = %q, want INFO", SeverityInfo)
	}
}

func TestStatusConstants(t *testing.T) {
	if StatusCompleted != "completed" {
		t.Errorf("StatusCompleted = %q, want completed", StatusCompleted)
	}
	if StatusPartialComplete != "partial_complete" {
		t.Errorf("StatusPartialComplete = %q, want partial_complete", StatusPartialComplete)
	}
	if StatusFailed != "failed" {
		t.Errorf("StatusFailed = %q, want failed", StatusFailed)
	}
}

func TestFindingJSONTags(t *testing.T) {
	// Verify canonical JSON tags are correct by round-tripping
	finding := Finding{
		ID:          "abc123",
		RuleID:      "sql-injection",
		Severity:    SeverityHigh,
		Category:    "SQL Injection",
		File:        "src/db.go",
		Line:        42,
		EndLine:     45,
		Column:      10,
		Message:     "Possible SQL injection",
		Snippet:     "db.Query(q)",
		Tool:        "semgrep",
		Confidence:  "HIGH",
		Metadata:    map[string]interface{}{"cwe": "CWE-89"},
		References:  []string{"https://cwe.mitre.org/data/definitions/89.html"},
		Repository:  "https://github.com/org/repo",
		Branch:      "main",
		CommitHash:  "deadbeef",
		Fingerprint: "fp-001",
	}

	data, err := json.Marshal(finding)
	if err != nil {
		t.Fatalf("Failed to marshal Finding: %v", err)
	}

	// Verify specific JSON keys exist
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Failed to unmarshal to map: %v", err)
	}

	requiredKeys := []string{
		"id", "rule_id", "severity", "category",
		"file", "line", "end_line", "column",
		"message", "snippet", "tool", "confidence",
		"metadata", "references",
		"repository", "branch", "commit_hash", "fingerprint",
	}

	for _, key := range requiredKeys {
		if _, ok := raw[key]; !ok {
			t.Errorf("Missing JSON key %q in Finding", key)
		}
	}

	// Verify WRONG tags are NOT present
	wrongKeys := []string{"file_path", "line_number", "line_start", "line_end", "description"}
	for _, key := range wrongKeys {
		if _, ok := raw[key]; ok {
			t.Errorf("Found wrong JSON key %q in Finding — should use canonical tag", key)
		}
	}

	// Round-trip
	var decoded Finding
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal Finding: %v", err)
	}
	if decoded.File != finding.File {
		t.Errorf("File = %q, want %q", decoded.File, finding.File)
	}
	if decoded.EndLine != finding.EndLine {
		t.Errorf("EndLine = %d, want %d", decoded.EndLine, finding.EndLine)
	}
	if decoded.Message != finding.Message {
		t.Errorf("Message = %q, want %q", decoded.Message, finding.Message)
	}
}

func TestFindingOmitEmpty(t *testing.T) {
	// Minimal finding — verify omitempty fields are absent
	finding := Finding{
		ID:       "abc",
		RuleID:   "rule",
		Severity: SeverityLow,
		File:     "f.go",
		Line:     1,
		Message:  "msg",
		Tool:     "semgrep",
	}

	data, err := json.Marshal(finding)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	// These should be omitted when empty
	omittedKeys := []string{
		"category", "column", "snippet", "confidence",
		"metadata", "references",
		"repository", "branch", "commit_hash", "fingerprint",
	}
	for _, key := range omittedKeys {
		if _, ok := raw[key]; ok {
			t.Errorf("Expected omitted key %q to be absent in minimal Finding", key)
		}
	}
}

func TestScanCompletedEventJSON(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	event := ScanCompletedEvent{
		JobID:         "job-1",
		ScanID:        "scan-1",
		CorrelationID: "corr-1",
		ScannerType:   "semgrep",
		Repository:    Repository{URL: "https://github.com/org/repo", Ref: "main"},
		Status:        StatusCompleted,
		Findings: []Finding{
			{ID: "f1", RuleID: "r1", Severity: SeverityHigh, File: "a.go", Line: 10, EndLine: 10, Message: "issue", Tool: "semgrep"},
		},
		Metrics:     Metrics{DurationSeconds: 30, FilesScanned: 100, LinesScanned: 5000},
		CompletedAt: now,
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded ScanCompletedEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.ScanID != "scan-1" {
		t.Errorf("ScanID = %q, want scan-1", decoded.ScanID)
	}
	if decoded.Repository.URL != "https://github.com/org/repo" {
		t.Errorf("Repository.URL = %q", decoded.Repository.URL)
	}
	if len(decoded.Findings) != 1 {
		t.Errorf("Findings count = %d, want 1", len(decoded.Findings))
	}
	if decoded.Metrics.DurationSeconds != 30 {
		t.Errorf("Metrics.DurationSeconds = %d, want 30", decoded.Metrics.DurationSeconds)
	}
}

func TestAggregatedResultJSON(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	result := AggregatedResult{
		ScanID:               "scan-1",
		OrganizationID:       "org-1",
		CorrelationID:        "corr-1",
		Status:               StatusCompleted,
		TotalRepositories:    2,
		TotalFindings:        3,
		BySeverity:           map[string]int{SeverityHigh: 2, SeverityLow: 1},
		TotalDurationSeconds: 60,
		TotalFilesScanned:    200,
		TotalLinesScanned:    10000,
		Findings: []Finding{
			{ID: "f1", RuleID: "r1", Severity: SeverityHigh, File: "a.go", Line: 1, Message: "msg", Tool: "semgrep"},
		},
		Repositories: []string{"https://github.com/org/repo1", "https://github.com/org/repo2"},
		StartedAt:    now.Add(-time.Minute),
		CompletedAt:  now,
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded AggregatedResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", decoded.Status, StatusCompleted)
	}
	if decoded.TotalFindings != 3 {
		t.Errorf("TotalFindings = %d, want 3", decoded.TotalFindings)
	}
	if len(decoded.Repositories) != 2 {
		t.Errorf("Repositories count = %d, want 2", len(decoded.Repositories))
	}
}

func TestScanPrepCompletedEventJSON(t *testing.T) {
	event := ScanPrepCompletedEvent{
		ScanID:          "scan-1",
		OrganizationID:  "org-1",
		CorrelationID:   "corr-1",
		SuccessfulCount: 2,
		SuccessfulRepositories: []RepositoryInfo{
			{URL: "https://github.com/org/repo1", Ref: "main", ProjectID: "proj-1", JobID: "job-1"},
			{URL: "https://github.com/org/repo2", Ref: "develop", ProjectID: "proj-2", JobID: "job-2"},
		},
		PrepCompletedAt: "2025-02-28T12:00:00Z",
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded ScanPrepCompletedEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.ScanID != "scan-1" {
		t.Errorf("ScanID = %q", decoded.ScanID)
	}
	if decoded.SuccessfulCount != 2 {
		t.Errorf("SuccessfulCount = %d, want 2", decoded.SuccessfulCount)
	}
	if len(decoded.SuccessfulRepositories) != 2 {
		t.Errorf("SuccessfulRepositories count = %d, want 2", len(decoded.SuccessfulRepositories))
	}
	if decoded.SuccessfulRepositories[0].ProjectID != "proj-1" {
		t.Errorf("SuccessfulRepositories[0].ProjectID = %q", decoded.SuccessfulRepositories[0].ProjectID)
	}
}

func TestScanResultJSON(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	result := ScanResult{
		ScannerType: "trivy",
		Repository:  "https://github.com/org/repo",
		Branch:      "main",
		CommitHash:  "abc123",
		Findings: []Finding{
			{ID: "f1", RuleID: "CVE-2024-1234", Severity: SeverityCritical, File: "go.mod", Line: 0, Message: "vuln", Tool: "trivy"},
		},
		Metrics:     Metrics{DurationSeconds: 15, FilesScanned: 50, LinesScanned: 2000},
		CompletedAt: now,
	}

	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded ScanResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.ScannerType != "trivy" {
		t.Errorf("ScannerType = %q, want trivy", decoded.ScannerType)
	}
	if decoded.Branch != "main" {
		t.Errorf("Branch = %q, want main", decoded.Branch)
	}
	if len(decoded.Findings) != 1 {
		t.Errorf("Findings count = %d, want 1", len(decoded.Findings))
	}
}

func TestScanMetadataJSON(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	metadata := ScanMetadata{
		ScanID:            "scan-1",
		OrganizationID:    "org-1",
		CorrelationID:     "corr-1",
		TotalRepositories: 3,
		Repositories:      []string{"repo1", "repo2", "repo3"},
		StartedAt:         now,
		TimeoutAt:         now.Add(time.Hour),
	}

	data, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded ScanMetadata
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.TotalRepositories != 3 {
		t.Errorf("TotalRepositories = %d, want 3", decoded.TotalRepositories)
	}
	if len(decoded.Repositories) != 3 {
		t.Errorf("Repositories count = %d, want 3", len(decoded.Repositories))
	}
}

// TestWireCompatibility_ScannerFinding verifies that a Finding produced by scanners
// (using file/line/end_line/message tags) deserializes correctly.
func TestWireCompatibility_ScannerFinding(t *testing.T) {
	// Simulate scanner JSON output (e.g., from semgrep or trivy)
	scannerJSON := `{
		"id": "abc123",
		"rule_id": "java.lang.security.audit.sqli",
		"severity": "HIGH",
		"file": "src/main/java/App.java",
		"line": 42,
		"end_line": 45,
		"message": "Possible SQL injection",
		"tool": "semgrep",
		"snippet": "stmt.execute(query)"
	}`

	var finding Finding
	if err := json.Unmarshal([]byte(scannerJSON), &finding); err != nil {
		t.Fatalf("Failed to unmarshal scanner JSON: %v", err)
	}

	if finding.File != "src/main/java/App.java" {
		t.Errorf("File = %q", finding.File)
	}
	if finding.Line != 42 {
		t.Errorf("Line = %d, want 42", finding.Line)
	}
	if finding.EndLine != 45 {
		t.Errorf("EndLine = %d, want 45", finding.EndLine)
	}
	if finding.Message != "Possible SQL injection" {
		t.Errorf("Message = %q", finding.Message)
	}
}

// TestWireCompatibility_OldLineEnd verifies that the old "line_end" tag
// does NOT populate EndLine (since the canonical tag is "end_line").
// This documents the breaking fix for the scan-enrich bug.
func TestWireCompatibility_OldLineEnd(t *testing.T) {
	oldJSON := `{
		"id": "abc123",
		"rule_id": "r1",
		"severity": "HIGH",
		"file": "a.go",
		"line": 10,
		"line_end": 15,
		"message": "msg",
		"tool": "semgrep"
	}`

	var finding Finding
	if err := json.Unmarshal([]byte(oldJSON), &finding); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	// line_end should NOT map to EndLine (canonical is end_line)
	if finding.EndLine != 0 {
		t.Errorf("EndLine = %d, want 0 (line_end should not map to EndLine)", finding.EndLine)
	}
}

// TestWireCompatibility_PrepEventNested verifies that the scan-aggregate
// prep consumer can parse the nested data payload.
func TestWireCompatibility_PrepEventNested(t *testing.T) {
	// scan-prep publishes with envelope wrapper; scan-aggregate extracts data
	envelopeJSON := `{
		"event_id": "evt-1",
		"correlation_id": "corr-1",
		"source": "scan-prep",
		"event_type": "scan.prep.completed",
		"data": {
			"scan_id": "scan-1",
			"organization_id": "org-1",
			"successful_count": 2,
			"successful_repositories": [
				{"url": "https://github.com/org/repo1", "ref": "main", "project_id": "p1", "job_id": "j1"}
			],
			"prep_completed_at": "2025-02-28T12:00:00Z"
		}
	}`

	// Parse as wrapper and extract data (simulates prep_consumer.go logic)
	var wrapper struct {
		Data ScanPrepCompletedEvent `json:"data"`
	}
	if err := json.Unmarshal([]byte(envelopeJSON), &wrapper); err != nil {
		t.Fatalf("Failed to unmarshal wrapper: %v", err)
	}

	if wrapper.Data.ScanID != "scan-1" {
		t.Errorf("ScanID = %q", wrapper.Data.ScanID)
	}
	if wrapper.Data.SuccessfulCount != 2 {
		t.Errorf("SuccessfulCount = %d", wrapper.Data.SuccessfulCount)
	}
}

func TestBackendUpdateRequestJSON(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	req := BackendUpdateRequest{
		Status:        StatusCompleted,
		FindingsCount: 42,
		CompletedAt:   now,
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded BackendUpdateRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.FindingsCount != 42 {
		t.Errorf("FindingsCount = %d, want 42", decoded.FindingsCount)
	}
}
