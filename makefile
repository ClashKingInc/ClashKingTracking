GOOSE = go run github.com/pressly/goose/v3/cmd/goose@latest
TIMESCALE_HOST ?= localhost
TIMESCALE_PORT ?= 5433
TIMESCALE_DATABASE ?= tracking
TIMESCALE_USERNAME ?= tracking
TIMESCALE_PASSWORD ?= tracking
TIMESCALE_SSLMODE ?= disable
TIMESCALE_DSN = postgres://$(TIMESCALE_USERNAME):$(TIMESCALE_PASSWORD)@$(TIMESCALE_HOST):$(TIMESCALE_PORT)/$(TIMESCALE_DATABASE)?sslmode=$(TIMESCALE_SSLMODE)
SCHEMA_DATABASE_DIR ?= ../clashking_schemas/database
TIMESCALE_MIGRATIONS_DIR = $(SCHEMA_DATABASE_DIR)/timescale
TIMESCALE_COMPOSE = $(SCHEMA_DATABASE_DIR)/docker-compose.timescale.yml
VALKEY_COMPOSE = $(SCHEMA_DATABASE_DIR)/docker-compose.valkey.yml

format:
	gofmt -w $$(git ls-files '*.go')

dev-db-up:
	docker compose -f $(TIMESCALE_COMPOSE) -f $(VALKEY_COMPOSE) up -d

dev-db-down:
	docker compose -f $(TIMESCALE_COMPOSE) -f $(VALKEY_COMPOSE) down

dev-db-logs:
	docker compose -f $(TIMESCALE_COMPOSE) -f $(VALKEY_COMPOSE) logs -f

goose-timescale-status:
	$(GOOSE) -dir $(TIMESCALE_MIGRATIONS_DIR) postgres "$(TIMESCALE_DSN)" status

goose-timescale-up:
	$(GOOSE) -dir $(TIMESCALE_MIGRATIONS_DIR) postgres "$(TIMESCALE_DSN)" up

go-test:
	GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod go test ./...

local-e2e:
	CLASHKING_SCHEMAS_ROOT="$(abspath $(SCHEMA_DATABASE_DIR)/..)" ./scripts/run-local-e2e.sh

go-run:
	GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod go run . --script $(script)
