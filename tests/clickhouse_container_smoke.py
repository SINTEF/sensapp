#!/usr/bin/env python3
"""Exercise a running SensApp container against a real ClickHouse service."""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def request(base_url: str, path: str, *, payload: bytes | None = None, content_type: str = "") -> tuple[int, str]:
    headers = {"Content-Type": content_type} if content_type else {}
    req = urllib.request.Request(base_url + path, data=payload, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8")


def wait_for_readiness(base_url: str, expected_status: int, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    last_result = "no response"
    while time.monotonic() < deadline:
        try:
            status, body = request(base_url, "/health/ready")
            last_result = f"HTTP {status}: {body}"
            if status == expected_status:
                state = json.loads(body)
                expected_state = "ready" if expected_status == 200 else "not_ready"
                if state.get("status") == expected_state:
                    print(f"Readiness is {expected_state}: {last_result}")
                    return
        except (OSError, ValueError) as error:
            last_result = str(error)
        time.sleep(2)
    raise AssertionError(f"Readiness did not reach HTTP {expected_status}: {last_result}")


def lifecycle(base_url: str) -> None:
    wait_for_readiness(base_url, 200, 120)
    metric = f"sensapp_ci_{uuid.uuid4().hex}"
    timestamp = int(time.time())
    payload = json.dumps([{"n": metric, "v": 21.5, "t": timestamp}]).encode()
    status, body = request(
        base_url, "/publish", payload=payload, content_type="application/json"
    )
    if status != 200:
        raise AssertionError(f"Publish failed: HTTP {status}: {body}")

    query = urllib.parse.urlencode({"query": f"{metric}[5m]", "format": "csv"})
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        status, body = request(base_url, "/api/v1/query?" + query)
        if status == 200 and metric in body and "21.5" in body:
            break
        time.sleep(2)
    else:
        raise AssertionError(f"Published sample not queryable: HTTP {status}: {body}")

    status, body = request(base_url, "/prometheus/metrics")
    if status != 200 or not body.strip():
        raise AssertionError(f"Service metrics unavailable: HTTP {status}: {body}")
    print(f"Published and queried {metric}; service metrics available")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:3000")
    parser.add_argument("--mode", choices=("lifecycle", "ready", "unready"), default="lifecycle")
    args = parser.parse_args()
    if args.mode == "lifecycle":
        lifecycle(args.base_url)
    else:
        wait_for_readiness(args.base_url, 200 if args.mode == "ready" else 503, 90)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (AssertionError, OSError, ValueError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
