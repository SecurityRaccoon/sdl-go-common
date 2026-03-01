# AGENTS.md — sdl-go-common

Shared Go library for SDL platform agents. Eliminates duplicated code across Go agents.

## Packages

| Package | Purpose |
|---------|---------|
| `pkg/logging` | Structured JSON logging with K8s context enrichment (pod, namespace, node, service) |
| `pkg/config` | Environment variable helpers (`GetEnv`, `GetIntEnv`, `GetDurationEnv`, `GetBoolEnv`, `GetFloatEnv`, `BuildRedisURL`, `GenerateConsumerName`) |
| `pkg/redis` | Redis client wrapper with Stream, key-value, hash, and pub/sub operations |
| `pkg/consumer` | Generic Redis Streams consumer framework with DLQ, pending recovery, and dead consumer claiming |
| `pkg/publisher` | Redis Streams publisher with retry and exponential backoff |
| `pkg/events` | Canonical event type definitions (`Finding`, `Repository`, `ScanEvent`, `ScanPrepCompletedEvent`) |

## Build / Test / Lint

```bash
make test      # All tests with race detector + coverage
make lint      # go fmt + go vet
make fmt       # Format only
go test -run TestName ./pkg/config/... -v -count=1  # Single test
```

## Usage from Agents

Agents reference this library via a `replace` directive pointing to the local path:

```go
// go.mod
require github.com/sdl-platform/sdl-go-common v0.0.0

replace github.com/sdl-platform/sdl-go-common => ../sdl-go-common
```

```go
import (
    "github.com/sdl-platform/sdl-go-common/pkg/config"
    "github.com/sdl-platform/sdl-go-common/pkg/logging"
    "github.com/sdl-platform/sdl-go-common/pkg/redis"
)
```

## Code Style

- Same conventions as all Go agents (see root `AGENTS.md`)
- Packages are intentionally thin wrappers — no business logic
- All public functions are documented with godoc comments
- Table-driven tests with `t.Setenv` for env var testing
