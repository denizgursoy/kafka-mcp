PROJECT    := kafka-mcp
MAIN_FILE := cmd/server/main.go

BUILD_DATE := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
BUILD_COMMIT := $(shell git rev-parse --short HEAD)
VERSION := $(or $(IMAGE_TAG),$(shell git describe --tags --first-parent --match "v*" 2> /dev/null || echo v0.0.0))
PKG := $(shell go list -m | head -n 1)

.DEFAULT_GOAL := help

COMPOSE := docker compose --project-name=$(PROJECT)

.PHONY: up
up: ## Start environment with docker-compose (Kafka/Redpanda)
	$(COMPOSE) up -d

.PHONY: down
down: ## Stop environment with docker-compose
	$(COMPOSE) down

.PHONY: restart
restart: ## Restart environment with docker-compose
	$(COMPOSE) down --volumes
	$(COMPOSE) up -d

.PHONY: logs
logs: ## Show logs from environment
	$(COMPOSE) logs -f

.PHONY: ps
ps: ## Show status of environment
	$(COMPOSE) ps

.PHONY: build
build: ## Build the Linux amd64 Go binary
	@echo "> Building $(PROJECT) binary with goreleaser"
	GOOS=linux GOARCH=amd64 goreleaser build --snapshot --clean --single-target

.PHONY: build-container
build-container: build ## Build the container image with test tag
	docker build --platform=linux/amd64 -t $(PROJECT):test -f ci/Dockerfile dist/$(PROJECT)_linux_amd64_v1/

.PHONY: run
run: ## Run the application
	CONFIG_FILE=kafka-mcp.local.yaml go run -ldflags="-X main.date=$(BUILD_DATE) -X main.commit=$(BUILD_COMMIT) -X main.version=$(VERSION)" $(MAIN_FILE)

.PHONY: help
help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
