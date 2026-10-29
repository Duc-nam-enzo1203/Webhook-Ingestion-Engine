.PHONY: up down restart logs test test-unit test-integration build benchmark clean help

help:
	@echo "Available commands:"
	@echo "  make up               - Start full stack in Docker (Gateway, Worker, Postgres, Redis, Prometheus, Grafana)"
	@echo "  make down             - Stop all services and remove volumes"
	@echo "  make restart          - Restart all containers"
	@echo "  make logs             - Stream logs of all containers"
	@echo "  make test             - Run all unit tests"
	@echo "  make test-unit        - Run unit tests with vitest"
	@echo "  make test-integration - Run integration tests"
	@echo "  make build            - Compile TypeScript to dist"
	@echo "  make benchmark        - Run k6 Flash Sale load test"

up:
	docker compose up -d --build

down:
	docker compose down -v

restart:
	docker compose restart

logs:
	docker compose logs -f

build:
	npm run build

test:
	npm run test

test-unit:
	npm run test:unit

test-integration:
	npm run test:integration

benchmark:
	k6 run benchmarks/k6-flash-sale-spike.js

