#!/bin/bash
# Writes and reads many series through the HTTP API of a SensApp binary, and prints timings.
#
#   BIN=target/release/sensapp CONN=postgres://user:pass@localhost/sensapp_perf \
#     tests/perf/scale.sh [series]
#
# The database must be empty (the script does not clean it). Series are InfluxDB lines
# `cpu,host=hN,core=cM,dc=dK usage=V`, 10 samples each. Timings are indicative: run a release
# build against a local database, and compare before and after a change on the same machine.
set -u
BIN=${BIN:?set BIN to the sensapp binary}
CONN=${CONN:?set CONN to the storage connection string}
SERIES=${1:-3000}
PORT=${PORT:-3980}
WORK=$(mktemp -d)
trap 'kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; rm -rf "$WORK"' EXIT

SENSAPP_PORT=$PORT SENSAPP_STORAGE_CONNECTION_STRING="$CONN" SENSAPP_SETTINGS_FILE=/nonexistent.toml \
  SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS=300 SENSAPP_HTTP_MAX_CONCURRENT_WRITES=0 "$BIN" > "$WORK/server.log" 2>&1 &
PID=$!
for _ in $(seq 1 60); do curl -sf "localhost:$PORT/health/ready" >/dev/null && break; sleep 0.5; done
curl -sf "localhost:$PORT/health/ready" >/dev/null || { echo "server did not start"; tail "$WORK/server.log"; exit 1; }

python3 - "$SERIES" > "$WORK/lines.lp" <<'PY'
import sys
for s in range(int(sys.argv[1])):
    for i in range(10):
        print(f"cpu,host=h{s % 30},core=c{s},dc=d{s % 7} usage={s % 100 + i * 0.1} {1700000000 + i * 15}000000000")
PY

t() { curl -s -o "$WORK/out" -w "%{time_total} s (HTTP %{http_code})" "$@"; }
echo "series: $SERIES"
echo "write, all series new:        $(t -X POST "localhost:$PORT/api/v2/write?bucket=b&org=o" --data-binary @"$WORK/lines.lp")"
echo "write, same series again:     $(t -X POST "localhost:$PORT/api/v2/write?bucket=b&org=o" --data-binary @"$WORK/lines.lp")"
echo "GET /series (default page):   $(t "localhost:$PORT/series")"
echo "GET /series?limit=1000:       $(t "localhost:$PORT/series?limit=1000")"
q() { echo "$1: $(t -G "localhost:$PORT/api/v1/query" --data-urlencode "query=$2")"; }
q "selector, 1 series              " '{__name__="cpu usage",core="c7"}'
q "selector, 10 series             " '{__name__="cpu usage",core=~"c70."}'
q "selector, 100 series (host=h7)  " '{__name__="cpu usage",host="h7"}'
q "selector, 100 series, again     " '{__name__="cpu usage",host="h7"}'
q "selector, 300 series (rejected) " '{__name__="cpu usage",host=~"h1.*"}'
q "sum over 100 series             " 'sum({__name__="cpu usage",host="h7"})'
rr() { echo "$1: $(python3 "$(dirname "$0")/remote_read.py" "http://localhost:$PORT" "cpu usage" "$2" "$3" "${4:-60}" 3)"; }
rr "remote read, step, 1 series        " core c7
rr "remote read, step, 10 series       " core "~c70[0-9]"
rr "remote read, step, 100 series      " host h7
