# sdl-go-common

Shared Go library for platform agents: Redis Streams consumer/publisher and config helpers.

Part of the Security Raccoon platform.

## Layout
- `pkg/`: config consumer events logging publisher redis 

## Build and test
```sh
go build ./...
go test ./...
```

## Deploy
Images are built by `.github/workflows` and pinned for production in `sdl-infra` (`k8s/environments/do-prod`).
