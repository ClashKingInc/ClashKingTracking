format:
	ruff format .
	ruff check . --fix

go-test:
	GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod go test ./...

go-run:
	GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod go run ./cmd/trackingd --mock-db --dry-run --run-once
