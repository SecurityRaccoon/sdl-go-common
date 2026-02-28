.PHONY: help test lint fmt clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

test: ## Run all tests with coverage
	go test ./... -v -race -cover

lint: fmt ## Run linters
	go vet ./...

fmt: ## Format code
	go fmt ./...

clean: ## Clean test artifacts
	rm -f *.out coverage.html
