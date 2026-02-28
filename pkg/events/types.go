// Package events provides canonical event types for the SDL scan pipeline.
//
// These types define the wire format for events flowing through Redis Streams
// across the scan pipeline: scanners -> scan-aggregate -> scan-enrich.
//
// JSON tags are the canonical contract. All pipeline services MUST use these
// exact tags to ensure wire compatibility. Any change to JSON tags is a
// breaking change that requires coordinated deployment.
package events

import "time"

// Severity levels used across the scan pipeline.
const (
	SeverityCritical = "CRITICAL"
	SeverityHigh     = "HIGH"
	SeverityMedium   = "MEDIUM"
	SeverityLow      = "LOW"
	SeverityInfo     = "INFO"
)

// Status values for aggregated results.
const (
	StatusCompleted       = "completed"
	StatusPartialComplete = "partial_complete"
	StatusFailed          = "failed"
)

// SeverityRank returns a numeric rank for severity levels (lower is more severe).
// Unknown severities return 999.
func SeverityRank(severity string) int {
	switch severity {
	case SeverityCritical:
		return 0
	case SeverityHigh:
		return 1
	case SeverityMedium:
		return 2
	case SeverityLow:
		return 3
	case SeverityInfo:
		return 4
	default:
		return 999
	}
}

// Finding represents a security finding flowing through the scan pipeline.
//
// This is the superset of all fields used by scanners, scan-aggregate, and scan-enrich.
// Scanners populate a subset; scan-aggregate adds Repository/Branch/CommitHash/Fingerprint
// during aggregation; scan-enrich reads what it needs for enrichment.
//
// JSON tags are the canonical wire format:
//   - file (not file_path)
//   - line (not line_number or line_start)
//   - end_line (not line_end)
//   - message (not description)
type Finding struct {
	// Core identification
	ID       string `json:"id"`                 // Unique identifier (SHA1 hash of key fields)
	RuleID   string `json:"rule_id"`            // Scanner-specific rule/check ID
	Severity string `json:"severity"`           // Normalized: CRITICAL, HIGH, MEDIUM, LOW, INFO
	Category string `json:"category,omitempty"` // Finding category (e.g., "SQL Injection")

	// Location
	File    string `json:"file"`             // Relative path from repo root
	Line    int    `json:"line"`             // Start line number (0 if N/A)
	EndLine int    `json:"end_line"`         // End line number (same as Line if single-line)
	Column  int    `json:"column,omitempty"` // Column number

	// Description
	Message string `json:"message"`           // Human-readable description
	Snippet string `json:"snippet,omitempty"` // Code snippet context

	// Scanner metadata
	Tool       string                 `json:"tool"`                 // Scanner name: semgrep, trivy, gitleaks, osv, checkov
	Confidence string                 `json:"confidence,omitempty"` // HIGH, MEDIUM, LOW
	Metadata   map[string]interface{} `json:"metadata,omitempty"`   // Scanner-specific data
	References []string               `json:"references,omitempty"` // URLs to documentation, CVE details, etc.

	// Aggregation fields (populated by scan-aggregate, not scanners)
	Repository  string `json:"repository,omitempty"`  // Repository URL, added during aggregation
	Branch      string `json:"branch,omitempty"`      // Git branch or ref
	CommitHash  string `json:"commit_hash,omitempty"` // Git commit hash
	Fingerprint string `json:"fingerprint,omitempty"` // Unique fingerprint for deduplication
}

// Repository contains repository information in pipeline events.
type Repository struct {
	URL string `json:"url"`
	Ref string `json:"ref"`
}

// Metrics contains scan performance metrics.
type Metrics struct {
	DurationSeconds int `json:"duration_seconds"`
	FilesScanned    int `json:"files_scanned"`
	LinesScanned    int `json:"lines_scanned"`
}

// ScanCompletedEvent represents a scan completion event from a single scanner+repository.
// Published by scanners (semgrep, trivy) to stream:scan.completed.
// Consumed by scan-aggregate.
//
// Note: Scanners publish ScanResult (flat format with target/ref), which is
// unmarshalled into this struct. The Repository field may be empty when coming
// from scanners; scan-aggregate falls back to the Target field.
type ScanCompletedEvent struct {
	JobID         string     `json:"job_id"`
	ScanID        string     `json:"scan_id"`
	CorrelationID string     `json:"correlation_id"`
	ScannerType   string     `json:"scanner_type"`
	Repository    Repository `json:"repository"`
	Target        string     `json:"target,omitempty"` // Fallback for repository URL (used by scanners)
	Status        string     `json:"status"`
	Findings      []Finding  `json:"findings"`
	Metrics       Metrics    `json:"metrics"`
	CompletedAt   time.Time  `json:"completed_at"`
}

// ScanResult stores the result from a single scanner+repository combination.
// Used internally by scan-aggregate to track per-scanner results in Redis.
type ScanResult struct {
	ScannerType string    `json:"scanner_type"`
	Repository  string    `json:"repository"`
	Branch      string    `json:"branch,omitempty"`
	CommitHash  string    `json:"commit_hash,omitempty"`
	Findings    []Finding `json:"findings"`
	Metrics     Metrics   `json:"metrics"`
	CompletedAt time.Time `json:"completed_at"`
}

// ScanMetadata contains metadata about an in-progress scan aggregation.
type ScanMetadata struct {
	ScanID            string    `json:"scan_id"`
	OrganizationID    string    `json:"organization_id"`
	CorrelationID     string    `json:"correlation_id"`
	TotalRepositories int       `json:"total_repositories"`
	Repositories      []string  `json:"repositories"`
	StartedAt         time.Time `json:"started_at"`
	TimeoutAt         time.Time `json:"timeout_at"`
}

// AggregatedResult represents the aggregated results from all repositories.
// Published by scan-aggregate to stream:scan.completed.aggregated.
// Consumed by scan-enrich.
type AggregatedResult struct {
	ScanID                string         `json:"scan_id"`
	OrganizationID        string         `json:"organization_id"`
	CorrelationID         string         `json:"correlation_id"`
	Status                string         `json:"status"` // "completed" or "partial_complete"
	TotalRepositories     int            `json:"total_repositories"`
	CompletedRepositories int            `json:"completed_repositories,omitempty"`
	TotalFindings         int            `json:"total_findings"`
	BySeverity            map[string]int `json:"by_severity"`
	TotalDurationSeconds  int            `json:"total_duration_seconds"`
	TotalFilesScanned     int            `json:"total_files_scanned"`
	TotalLinesScanned     int            `json:"total_lines_scanned"`
	Findings              []Finding      `json:"findings"`
	Repositories          []string       `json:"repositories"`
	MissingRepositories   []string       `json:"missing_repositories,omitempty"`
	StartedAt             time.Time      `json:"started_at"`
	CompletedAt           time.Time      `json:"completed_at"`
}

// ScanPrepCompletedEvent represents the scan-prep completion event as consumed
// by scan-aggregate. This is the "inner" data payload — scan-prep wraps it in
// an envelope with event_id/source/event_type/timestamp/version/data, but
// scan-aggregate's parseEvent extracts the data portion into this struct.
type ScanPrepCompletedEvent struct {
	ScanID                 string           `json:"scan_id"`
	OrganizationID         string           `json:"organization_id"`
	CorrelationID          string           `json:"correlation_id"`
	SuccessfulCount        int              `json:"successful_count"`
	SuccessfulRepositories []RepositoryInfo `json:"successful_repositories"`
	PrepCompletedAt        string           `json:"prep_completed_at"`
}

// RepositoryInfo contains information about a successfully prepared repository.
type RepositoryInfo struct {
	URL       string `json:"url"`
	Ref       string `json:"ref"`
	ProjectID string `json:"project_id"`
	JobID     string `json:"job_id"`
}

// BackendUpdateRequest represents the request to update scan status in the backend.
type BackendUpdateRequest struct {
	Status        string    `json:"status"`
	FindingsCount int       `json:"findings_count"`
	CompletedAt   time.Time `json:"completed_at"`
}
