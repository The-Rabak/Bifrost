.DEFAULT_GOAL := help

.PHONY: help up test down

help:
	@printf '%s\n' \
	  'make up    Build and start the local deployment; wait for readiness' \
	  'make test  Run all E2E tests with live output and saved reports' \
	  'make down  Stop the local deployment; keep persisted data'

up: .env
	docker compose up -d --build --wait --wait-timeout 180

.env:
	python3 scripts/configure.py

test:
	@printf '%s\n' 'E2E reports and logs: artifacts/bifrost-test-*/'
	uv run --frozen python scripts/e2e.py

down:
	docker compose down --remove-orphans
