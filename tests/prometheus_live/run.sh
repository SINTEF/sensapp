#!/usr/bin/env bash
set -euo pipefail

: "${SENSAPP_STORAGE_CONNECTION_STRING:?Set SENSAPP_STORAGE_CONNECTION_STRING to a PostgreSQL database URL}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work_dir="$(mktemp -d)"
prometheus_container="sensapp-prometheus-live-${RANDOM}-$$"
sensapp_pid=""

cleanup() {
  status=$?
  trap - EXIT
  if (( status != 0 )); then
    echo "SensApp log:" >&2
    cat "$work_dir/sensapp.log" >&2 2>/dev/null || true
    echo "Prometheus log:" >&2
    docker logs "$prometheus_container" >&2 2>/dev/null || true
  fi
  docker rm --force "$prometheus_container" >/dev/null 2>&1 || true
  if [[ -n "$sensapp_pid" ]]; then
    kill "$sensapp_pid" >/dev/null 2>&1 || true
    wait "$sensapp_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$work_dir"
  exit "$status"
}
trap cleanup EXIT

cd "$repo_root"
cargo build --locked --no-default-features --features postgres

# The test clients send no token: run open, explicitly
SENSAPP_AUTH_DISABLED=true SENSAPP_ENDPOINT=0.0.0.0 SENSAPP_PORT=3017 \
  "$repo_root/target/debug/sensapp" >"$work_dir/sensapp.log" 2>&1 &
sensapp_pid=$!

docker run --detach \
  --name "$prometheus_container" \
  --add-host host.docker.internal:host-gateway \
  --publish 127.0.0.1:9099:9090 \
  --volume "$repo_root/tests/prometheus_live/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
  "${PROMETHEUS_IMAGE:-prom/prometheus:v3.13.4}" \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/prometheus

python3 tests/prometheus_live/test_live.py
