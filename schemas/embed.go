package schemas

import "embed"

// Files embeds the JSON Schema documents for shared Redis event payloads.
//
//go:embed *.schema.json
var Files embed.FS
