GOOSE = go run github.com/pressly/goose/v3/cmd/goose
TIMESCALE_MIGRATIONS_DIR = migrations/timescale
TIMESCALE_URL ?= postgres://tracking:tracking@localhost:5433/tracking?sslmode=disable
SCHEMA_COMPOSE = /Users/matthewanderson/GolandProjects/clashking_schemas/docker-compose.yml

format:
	ruff format .
	ruff check . --fix

dev-db-up:
	docker compose -f $(SCHEMA_COMPOSE) up -d timescale valkey jaeger redis-insight

dev-db-down:
	docker compose -f $(SCHEMA_COMPOSE) down

dev-db-logs:
	docker compose -f $(SCHEMA_COMPOSE) logs -f timescale valkey jaeger redis-insight

goose-timescale-status:
	$(GOOSE) -dir $(TIMESCALE_MIGRATIONS_DIR) postgres "$(TIMESCALE_URL)" status

goose-timescale-up:
	$(GOOSE) -dir $(TIMESCALE_MIGRATIONS_DIR) postgres "$(TIMESCALE_URL)" up

goose-timescale-down:
	$(GOOSE) -dir $(TIMESCALE_MIGRATIONS_DIR) postgres "$(TIMESCALE_URL)" down

goose-timescale-create:
	@test -n "$(name)" || (echo "usage: make goose-timescale-create name=create_events_table" && exit 1)
	$(GOOSE) -dir $(TIMESCALE_MIGRATIONS_DIR) -s create $(name) sql

go-test:
	GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod go test ./...

go-run:
	GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod go run . --script $(script)
