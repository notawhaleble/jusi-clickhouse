#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${JUSI_CLICKHOUSE_IMAGE:-jusi-clickhouse-dev}"
CONTAINER_NAME="${JUSI_CLICKHOUSE_CONTAINER:-jusi-clickhouse-dev}"
HTTP_PORT="${JUSI_CLICKHOUSE_HTTP_PORT:-58123}"
NATIVE_PORT="${JUSI_CLICKHOUSE_NATIVE_PORT:-59000}"
CLICKHOUSE_USER="${JUSI_CLICKHOUSE_USER:-jusi}"
CLICKHOUSE_PASSWORD="${JUSI_CLICKHOUSE_PASSWORD:-jusi}"
CLICKHOUSE_DB="${JUSI_CLICKHOUSE_DB:-demo}"
RECREATE="${JUSI_CLICKHOUSE_RECREATE:-0}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker build -t "${IMAGE_NAME}" "${ROOT_DIR}"

if [[ "${RECREATE}" == "1" ]] && docker ps -a --format '{{.Names}}' | grep -Fxq "${CONTAINER_NAME}"; then
  docker rm -f "${CONTAINER_NAME}" >/dev/null
fi

if docker ps --format '{{.Names}}' | grep -Fxq "${CONTAINER_NAME}"; then
  echo "ClickHouse container '${CONTAINER_NAME}' is already running on HTTP port ${HTTP_PORT}."
else
  if docker ps -a --format '{{.Names}}' | grep -Fxq "${CONTAINER_NAME}"; then
    docker rm "${CONTAINER_NAME}" >/dev/null
  fi

  docker run \
    --detach \
    --name "${CONTAINER_NAME}" \
    --publish "127.0.0.1:${HTTP_PORT}:8123" \
    --publish "127.0.0.1:${NATIVE_PORT}:9000" \
    --env "CLICKHOUSE_USER=${CLICKHOUSE_USER}" \
    --env "CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}" \
    --env "CLICKHOUSE_DB=${CLICKHOUSE_DB}" \
    "${IMAGE_NAME}" >/dev/null
fi

echo "Waiting for ClickHouse to accept connections..."
for _ in $(seq 1 90); do
  if docker exec "${CONTAINER_NAME}" clickhouse-client \
    --user "${CLICKHOUSE_USER}" \
    --password "${CLICKHOUSE_PASSWORD}" \
    --database "${CLICKHOUSE_DB}" \
    --query "SELECT 1" >/dev/null 2>&1; then
    cat <<EOF
ClickHouse is running.

jusi.toml:

[sql.targets.local_clickhouse]
provider = "clickhouse"
host = "127.0.0.1"
port = ${HTTP_PORT}
username = "${CLICKHOUSE_USER}"
password = "${CLICKHOUSE_PASSWORD}"
database = "${CLICKHOUSE_DB}"
initial_fetch = 25

Manual test queries:

%%sql local_clickhouse
SELECT * FROM demo.account_summary ORDER BY id

%%sql local_clickhouse
SELECT number FROM system.numbers LIMIT 50

%%sql local_clickhouse
SELECT filename, content_type, payload FROM demo.blob_files
EOF
    exit 0
  fi
  sleep 1
done

echo "ClickHouse did not become ready in time." >&2
docker logs "${CONTAINER_NAME}" >&2 || true
exit 1
