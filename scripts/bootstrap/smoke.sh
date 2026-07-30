#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

export CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-gel}"

echo "==> docker compose up"
docker compose up -d clickhouse minio minio_init postgres

echo "==> wait ClickHouse"
for i in $(seq 1 60); do
  if curl -fsS "http://localhost:8123/ping" >/dev/null; then break; fi
  sleep 1
done

echo "==> generate + load fixtures (DDL + seed via Python)"
python scripts/generate_fixtures.py

echo "==> smoke counts"
t=$(curl -fsS -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" "http://localhost:8123/" --data "SELECT count() FROM gel_helpdesk.demo_tickets")
s=$(curl -fsS -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" "http://localhost:8123/" --data "SELECT count() FROM gel_bot.demo_bot_sessions")
echo "tickets=${t}"
echo "sessions=${s}"
echo "OK - Gelassenheit ELT demo data is up"
