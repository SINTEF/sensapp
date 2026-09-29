#!/usr/bin/env python3
"""Check remote write and remote read through a running Prometheus server."""

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

SENSAPP = "http://127.0.0.1:3017"
PROMETHEUS = "http://127.0.0.1:9099"


def request(base: str, path: str, payload: bytes | None = None) -> tuple[int, str]:
    headers = {"Content-Type": "application/json"} if payload is not None else {}
    req = urllib.request.Request(base + path, data=payload, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8")


def query(base: str, expression: str) -> tuple[int, str]:
    params = urllib.parse.urlencode({"query": expression})
    return request(base, "/api/v1/query?" + params)


def wait_for(description: str, probe, timeout: int = 90) -> None:
    deadline = time.monotonic() + timeout
    last_result = "no response"
    while time.monotonic() < deadline:
        try:
            matched, last_result = probe()
            if matched:
                print(f"PASS {description}", flush=True)
                return
        except (OSError, ValueError, KeyError, TypeError) as error:
            last_result = str(error)
        time.sleep(1)
    raise AssertionError(f"Timed out waiting for {description}: {last_result}")


def ready(base: str) -> tuple[bool, str]:
    status, body = request(base, "/health/ready" if base == SENSAPP else "/-/ready")
    return status == 200, f"HTTP {status}: {body[:200]}"


def operation_succeeded(kind: str, operation: str) -> bool:
    status, body = request(SENSAPP, "/prometheus/metrics")
    if status != 200:
        return False
    prefix = (
        f'sensapp_operations_total{{kind="{kind}",operation="{operation}",'
        'status="success"} '
    )
    return any(
        float(line.removeprefix(prefix)) > 0
        for line in body.splitlines()
        if line.startswith(prefix)
    )


def test_remote_write() -> None:
    def stored_scrape() -> tuple[bool, str]:
        status, body = query(SENSAPP, 'sensapp_storage_ready{job="sensapp_live"}[5m]')
        if status != 200:
            return False, f"SensApp query HTTP {status}: {body[:300]}"
        records = json.loads(body)
        found = isinstance(records, list) and any(
            record.get("v") == 1 for record in records
        )
        success = operation_succeeded("write", "prometheus_remote_write")
        return found and success, f"sample found={found}, remote write succeeded={success}"

    wait_for("Prometheus remote write stored a scraped metric and its job label", stored_scrape)


def test_remote_read() -> None:
    metric = f"sensapp_remote_read_probe_{uuid.uuid4().hex}"
    payload = json.dumps([{"n": metric, "v": 42.25, "t": int(time.time()) - 10}]).encode()
    status, body = request(SENSAPP, "/publish", payload)
    if status != 200:
        raise AssertionError(f"Could not seed SensApp: HTTP {status}: {body[:300]}")

    def queried_from_prometheus() -> tuple[bool, str]:
        status, body = query(PROMETHEUS, metric)
        if status != 200:
            return False, f"Prometheus query HTTP {status}: {body[:300]}"
        response = json.loads(body)
        results = response["data"]["result"]
        found = any(
            result.get("metric", {}).get("__name__") == metric
            and float(result["value"][1]) == 42.25
            for result in results
        )
        success = operation_succeeded("read", "prometheus_remote_read")
        return found and success, f"Prometheus result={results}, remote read succeeded={success}"

    wait_for("Prometheus remote read returned the SensApp-only probe", queried_from_prometheus)


def main() -> None:
    wait_for("SensApp readiness", lambda: ready(SENSAPP))
    wait_for("Prometheus readiness", lambda: ready(PROMETHEUS))
    test_remote_write()
    test_remote_read()


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, OSError, ValueError, KeyError, TypeError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
