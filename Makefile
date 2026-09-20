.PHONY: up down restart logs ps build run serve clean

APP_NAME := kafka-debugger
COMPOSE := docker compose

# Docker / Redpanda
up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) down
	$(COMPOSE) up -d

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

# Go
build:
	go build -o bin/$(APP_NAME) ./cmd/server

# The server reads only KAFKA_MCP_CONFIG, so a run needs nothing else.
run:
	KAFKA_MCP_CONFIG=kafka-mcp.local.json go run ./cmd/server

clean:
	rm -rf bin

# Development
dev: up
	KAFKA_MCP_CONFIG=kafka-mcp.local.json go run ./cmd/server