package events

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/xeipuuv/gojsonschema"

	rootschemas "github.com/sdl-platform/sdl-go-common/schemas"
)

// Shared payload schema names.
const (
	SchemaFinding                = "finding"
	SchemaScanCompletedEvent     = "scan-completed-event"
	SchemaScanPrepCompletedEvent = "scan-prep-completed-event"
	SchemaAggregatedResult       = "aggregated-result"
)

var schemaFiles = map[string]string{
	SchemaFinding:                "finding.schema.json",
	SchemaScanCompletedEvent:     "scan-completed-event.schema.json",
	SchemaScanPrepCompletedEvent: "scan-prep-completed-event.schema.json",
	SchemaAggregatedResult:       "aggregated-result.schema.json",
}

// ValidateAgainstSchema marshals payload and checks it against a shared JSON Schema.
func ValidateAgainstSchema(schemaName string, payload any) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload for %s schema: %w", schemaName, err)
	}

	return ValidateJSONAgainstSchema(schemaName, payloadJSON)
}

// ValidateJSONAgainstSchema checks a JSON document against a shared JSON Schema.
func ValidateJSONAgainstSchema(schemaName string, payloadJSON []byte) error {
	schemaFile, ok := schemaFiles[schemaName]
	if !ok {
		return fmt.Errorf("unknown event schema %q", schemaName)
	}

	schemaJSON, err := rootschemas.Files.ReadFile(schemaFile)
	if err != nil {
		return fmt.Errorf("read %s schema: %w", schemaName, err)
	}

	result, err := gojsonschema.Validate(
		gojsonschema.NewBytesLoader(schemaJSON),
		gojsonschema.NewBytesLoader(payloadJSON),
	)
	if err != nil {
		return fmt.Errorf("validate %s schema: %w", schemaName, err)
	}

	if result.Valid() {
		return nil
	}

	errors := make([]string, 0, len(result.Errors()))
	for _, validationErr := range result.Errors() {
		errors = append(errors, validationErr.String())
	}
	sort.Strings(errors)

	return fmt.Errorf("payload failed %s schema validation: %s", schemaName, strings.Join(errors, "; "))
}
