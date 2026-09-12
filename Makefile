.PHONY: up down restart logs ps build run clean

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

run:
	go run ./cmd/server

clean:
	rm -rf bin

# Development
dev: up
	go run ./cmd/server