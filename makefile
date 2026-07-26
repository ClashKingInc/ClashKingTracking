GOOSE = go run github.com/pressly/goose/v3/cmd/goose@latest
TIMESCALE_URL ?= postgres://tracking:tracking@localhost:5433/tracking?sslmode=disable
SCHEMA_DATABASE_DIR = /Users/matthewanderson/GolandProjects/clashking_schemas/database
TIMESCALE_MIGRATIONS_DIR = $(SCHEMA_DATABASE_DIR)/timescale
TIMESCALE_COMPOSE = $(SCHEMA_DATABASE_DIR)/docker-compose.timescale.yml
VALKEY_COMPOSE = $(SCHEMA_DATABASE_DIR)/docker-compose.valkey.yml

format:
	ruff format .
	ruff check . --fix

dev-db-up:
	docker compose -f $(TIMESCALE_COMPOSE) -f $(VALKEY_COMPOSE) up -d

dev-db-down:
	docker compose -f $(TIMESCALE_COMPOSE) -f $(VALKEY_COMPOSE) down

dev-db-logs:
	docker compose -f $(TIMESCALE_COMPOSE) -f $(VALKEY_COMPOSE) logs -f

goose-timescale-status:
	$(GOOSE) -dir $(TIMESCALE_MIGRATIONS_DIR) postgres "$(TIMESCALE_URL)" status

goose-timescale-up:
	$(GOOSE) -dir $(TIMESCALE_MIGRATIONS_DIR) postgres "$(TIMESCALE_URL)" up

go-test:
	GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod go test ./...

go-run:
	GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod go run . --script $(script)
