FROM clickhouse/clickhouse-server:25.4-alpine

LABEL org.opencontainers.image.title="jusi-clickhouse-dev"
LABEL org.opencontainers.image.description="Local ClickHouse fixture for jusi-clickhouse development"

COPY docker/clickhouse/initdb.d/ /docker-entrypoint-initdb.d/
