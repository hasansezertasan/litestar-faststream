#!/usr/bin/env bash
# Live-broker smoke for the FastStream Litestar example.
# Requires: docker, uv. Will start/stop a Kafka container.
set -euo pipefail

dir="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$dir/../.." && pwd)"
compose_file="$dir/docker-compose.yml"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ "${SKIP_COMPOSE:-0}" != "1" ]]; then
    docker compose -f "$compose_file" down >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "${SKIP_COMPOSE:-0}" != "1" ]]; then
  echo "==> starting kafka"
  docker compose -f "$compose_file" up -d
else
  echo "==> SKIP_COMPOSE=1, using external kafka"
fi

# Wait for Kafka broker port
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
  nc -z localhost 9092 && break
  sleep 2
done

echo "==> booting HTTP server"
cd "$repo"
uv run litestar --app examples.kafka.app:app run --port 8765 &
SERVER_PID=$!

for i in 1 2 3 4 5 6 7 8 9 10; do
  curl -fs http://localhost:8765/asyncapi.json >/dev/null && break
  sleep 1
done

echo "==> POST /orders"
http_code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST http://localhost:8765/orders \
  -H 'content-type: application/json' \
  -d '{"user_id": 1, "item": "tea"}')
if [[ "$http_code" != "201" ]]; then
  echo "POST /orders returned $http_code (expected 201)"
  exit 1
fi

echo "==> GET /asyncapi"
http_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8765/asyncapi)
if [[ "$http_code" != "200" ]]; then
  echo "GET /asyncapi returned $http_code (expected 200)"
  exit 1
fi

echo "==> smoke OK"
