.PHONY: help test lint lint-fix fmt clean install-hooks

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

test: ## Run all tests with coverage
	go test ./... -v -race -cover

lint: ## Run linters (strict formatting check)
	@test -z "$$(gofmt -l .)" || (echo "Files need formatting:"; gofmt -l .; exit 1)
	go vet ./...

lint-fix: ## Auto-fix formatting issues
	gofmt -w -s .
	go mod tidy

fmt: ## Format code
	go fmt ./...

clean: ## Clean test artifacts
	rm -f *.out coverage.html

install-hooks: ## Install git hooks
	@mkdir -p .git/hooks
	@for hook in scripts/git-hooks/*; do \
		if [ -f "$$hook" ]; then \
			cp "$$hook" ".git/hooks/$$(basename "$$hook")"; \
			chmod +x ".git/hooks/$$(basename "$$hook")"; \
			printf 'Installed %s hook\n' "$$(basename "$$hook")"; \
		fi; \
	done

setup-hooks: install-hooks ## Install local verification hooks
