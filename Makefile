.PHONY: tidy fmt vet build run up down logs seed migrate-up test test-integration loadtest

tidy:
	go mod tidy

fmt:
	gofmt -w .

vet:
	go vet ./...
	go vet -tags=integration ./...

build:
	go build -o bin/api ./cmd/api

run:
	go run ./cmd/api

# Start everything (Postgres, Redis, Kafka, API, Prometheus, Grafana).
up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f api

migrate-up:
	docker compose run --rm migrate

seed:
	docker compose exec -T postgres psql -U orders -d orders < scripts/seed.sql

test:
	go test ./...

# Needs Docker: Testcontainers starts a real PostgreSQL.
test-integration:
	go test -tags=integration -race -count=1 -timeout 10m ./tests/...

loadtest:
	k6 run scripts/k6/create_orders.js
