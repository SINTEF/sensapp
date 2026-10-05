#!/bin/bash
# Writes string series through the HTTP API of a SensApp binary, and prints timings.
#
#   BIN=target/release/sensapp CONN=postgres://user:pass@localhost/sensapp_perf \
#     tests/perf/strings.sh [series]
#
# The database must be empty. Three requests of InfluxDB lines with a string field:
#   many series, 10 samples each, drawn from 50 distinct strings (what logs and states look like);
#   one series of 10 000 distinct strings;
#   the first request again (every string and series already known).
set -u
BIN=${BIN:?set BIN to the sensapp binary}
CONN=${CONN:?set CONN to the storage connection string}
SERIES=${1:-3000}
PORT=${PORT:-3981}
WORK=$(mktemp -d)
trap 'kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; rm -rf "$WORK"' EXIT

SENSAPP_AUTH_DISABLED=true SENSAPP_PORT=$PORT SENSAPP_STORAGE_CONNECTION_STRING="$CONN" SENSAPP_SETTINGS_FILE=/nonexistent.toml \
  SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS=300 SENSAPP_HTTP_MAX_CONCURRENT_WRITES=0 "$BIN" > "$WORK/server.log" 2>&1 &
PID=$!
for _ in $(seq 1 60); do curl -sf "localhost:$PORT/health/ready" >/dev/null && break; sleep 0.5; done
curl -sf "localhost:$PORT/health/ready" >/dev/null || { echo "server did not start"; tail "$WORK/server.log"; exit 1; }

python3 - "$SERIES" "$WORK" <<'PY'
import sys
series, work = int(sys.argv[1]), sys.argv[2]
with open(f"{work}/many.lp", "w") as f:
    for s in range(series):
        for i in range(10):
            f.write(f'state,host=h{s % 30},id=s{s} msg="state number {(s + i) % 50} ok" {1700000000 + i * 15}000000000\n')
with open(f"{work}/distinct.lp", "w") as f:
    for i in range(10000):
        f.write(f'journal,host=one msg="entry {i} é ü 日本 {"x" * (i % 40)}" {1700000000 + i}000000000\n')
PY
t() { curl -s -o "$WORK/out" -w "%{time_total} s (HTTP %{http_code})" "$@"; }
echo "series: $SERIES x 10 string samples, 50 distinct strings"
echo "write, all series and strings new:   $(t -X POST "localhost:$PORT/api/v2/write?bucket=b&org=o" --data-binary @"$WORK/many.lp")"
echo "write, one series, 10000 distinct:   $(t -X POST "localhost:$PORT/api/v2/write?bucket=b&org=o" --data-binary @"$WORK/distinct.lp")"
echo "write, the first one again:          $(t -X POST "localhost:$PORT/api/v2/write?bucket=b&org=o" --data-binary @"$WORK/many.lp")"
echo "read one series back (10 samples):   $(t -G "localhost:$PORT/api/v1/query" --data-urlencode 'query={__name__="state msg",id="s7"}[2000d]')"
