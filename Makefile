.PHONY: up smoke fixtures down

up:
	docker compose up -d clickhouse minio minio_init postgres

fixtures:
	python scripts/generate_fixtures.py

smoke:
	bash scripts/bootstrap/smoke.sh

down:
	docker compose down
