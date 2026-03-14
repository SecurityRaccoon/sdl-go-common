# AGENTS.md — sdl-go-common

Shared Go library for SDL platform agents. Provides Redis client, consumer/publisher framework, event types, config helpers, and structured logging. **Not a deployable service** — consumed by Go agents via `replace` directive.

## 1. Orientation

- **Stack**: Go 1.24. Pure library — no `main` package, no container image.
- **Consumed by**: All Go agents (`sdl-agent-scan-*`, `sdl-agent-ai-*`, `sdl-agent-repository-*`, etc.).
- **Toolchain**: Uses Devbox for reproducible builds (`devbox shell` or prefix with `devbox run --`).

## 2. Packages

| Package | Purpose |
|---------|---------|
| `pkg/config` | Env var helpers (`GetEnv`, `GetIntEnv`, `GetDurationEnv`, `GetBoolEnv`, `GetFloatEnv`, `BuildRedisURL`, `GenerateConsumerName`) |
| `pkg/consumer` | Generic Redis Streams consumer with DLQ, pending recovery, dead consumer claiming |
| `pkg/events` | Canonical event types (`Finding`, `Repository`, `ScanEvent`, `ScanPrepCompletedEvent`) |
| `pkg/logging` | Structured JSON logging with K8s context enrichment (pod, namespace, node, service) |
| `pkg/publisher` | Redis Streams publisher with retry and exponential backoff |
| `pkg/redis` | Redis client wrapper (Streams, key-value, hash, pub/sub) |

## 3. Command Reference

| Purpose | Command | Notes |
|---|---|---|
| Run all tests | `make test` | `go test ./... -v -race -cover` |
| Lint (strict) | `make lint` | `gofmt -l` (fails on drift) + `go vet` |
| Auto-fix format | `make lint-fix` | `gofmt -w -s` + `go mod tidy` |
| Format only | `make fmt` | `go fmt ./...` |
| Clean | `make clean` | Remove test artifacts |
| Git hooks | `make install-hooks` | Pre-commit hook |
| Single test | `go test -run TestName ./pkg/config/... -v -count=1` | |

## 4. Usage from Agents

Agents reference this library via a local `replace` directive:

```go
// go.mod
require github.com/sdl-platform/sdl-go-common v0.0.0
replace github.com/sdl-platform/sdl-go-common => ../sdl-go-common
```

## 5. Code Style

- Same conventions as all Go agents (see root `AGENTS.md`).
- Packages are thin wrappers — no business logic.
- All public functions documented with godoc comments.
- Table-driven tests with `t.Setenv` for env var testing.

## 6. Commit Conventions

- Conventional commits: `feat:`, `fix:`, `refactor:`, `test:`, `chore:`.
- Before PR: `make lint && make test`.
