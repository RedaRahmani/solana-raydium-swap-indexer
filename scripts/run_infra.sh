#!/usr/bin/env bash
set -e

# Go to repo root
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/infra"

echo "[*] Starting Kafka + ClickHouse..."
docker-compose up -d

echo "[*] Waiting 5s for ClickHouse to start..."
sleep 5

echo "[*] Initializing ClickHouse schema..."
cat clickhouse-init.sql | curl -sS 'http://localhost:8123/?user=raywatch&password=supersecret' --data-binary @- || true

echo "[*] Done."
echo "Try:"
echo "  curl 'http://localhost:8123/?query=SHOW+TABLES+FROM+swaps'"
echo "  curl 'http://localhost:8123/?query=SELECT+count()+FROM+swaps.raydium_swaps_raw'"
