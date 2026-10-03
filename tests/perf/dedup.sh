#!/bin/bash
# Starts a SensApp binary on an empty database, runs tests/perf/dedup.py against it and prints the
# number of rows the database holds at the end (to compare with the distinct samples sent).
#
#   BIN=target/release/sensapp CONN=postgres://user:pass@localhost/sensapp_perf \
#     tests/perf/dedup.sh [series] [samples_per_series]
#
# EXTRA_ENV="SENSAPP_X=y" adds environment variables to the server (the deduplication switch).
# The database must be empty. Row counts use the native client of the backend (psql, sqlite3,
# duckdb, or curl for ClickHouse); the line is skipped when it is not installed.
set -u
BIN=${BIN:?set BIN to the sensapp binary}
CONN=${CONN:?set CONN to the storage connection string}
SERIES=${1:-1000}
HISTORY=${2:-1000}
PORT=${PORT:-3982}
WORK=$(mktemp -d)
trap 'kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; rm -rf "$WORK"' EXIT

# shellcheck disable=SC2086
env SENSAPP_PORT=$PORT SENSAPP_STORAGE_CONNECTION_STRING="$CONN" SENSAPP_SETTINGS_FILE=/nonexistent.toml \
  SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS=600 SENSAPP_HTTP_MAX_CONCURRENT_WRITES=0 ${EXTRA_ENV:-} \
  "$BIN" > "$WORK/server.log" 2>&1 &
PID=$!
for _ in $(seq 1 60); do curl -sf "localhost:$PORT/health/ready" >/dev/null && break; sleep 0.5; done
curl -sf "localhost:$PORT/health/ready" >/dev/null || { echo "server did not start"; tail "$WORK/server.log"; exit 1; }

python3 "$(dirname "$0")/dedup.py" "http://localhost:$PORT" "$SERIES" "$HISTORY"

# DuckDB allows one process on its file: stop the server before counting
kill "$PID"; wait "$PID" 2>/dev/null
SQL="SELECT count(*) FROM float_values"
case "$CONN" in
  postgres:*|timescaledb:*) command -v psql >/dev/null && echo "rows stored: $(psql "${CONN/timescaledb:/postgres:}" -Atc "$SQL")" ;;
  sqlite:*) command -v sqlite3 >/dev/null && echo "rows stored: $(sqlite3 "${CONN#sqlite://}" "$SQL")" ;;
  duckdb:*) command -v duckdb >/dev/null && echo "rows stored: $(duckdb -noheader -list "${CONN#duckdb://}" "$SQL")" ;;
  clickhouse:*)
    # clickhouse://user:pass@host:8123/db
    rest=${CONN#clickhouse://}; creds=${rest%%@*}; hostdb=${rest#*@}
    echo "rows stored: $(curl -s --user "$creds" "http://${hostdb%%/*}/?database=${hostdb#*/}" --data-binary "$SQL")" ;;
esac
