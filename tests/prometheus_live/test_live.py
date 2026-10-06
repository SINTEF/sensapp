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


def request(
    base: str, path: str, payload: bytes | None = None, timeout: int = 5
) -> tuple[int, str]:
    headers = {"Content-Type": "application/json"} if payload is not None else {}
    req = urllib.request.Request(base + path, data=payload, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
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


HOUR = 3600
MINUTES = 6 * 60


def hint_value(series: str, minute: int) -> float:
    """Values that move all the time, so a wrong window or bucket changes the answer."""
    if series == "a":
        return ((minute * 7919) % 1000) / 10.0
    return 500.0 + ((minute * 104729) % 1000) / 10.0


def query_range(expression: str, start: int, end: int, step: int) -> dict[int, float]:
    params = urllib.parse.urlencode(
        {"query": expression, "start": start, "end": end, "step": step}
    )
    status, body = request(PROMETHEUS, "/api/v1/query_range?" + params)
    if status != 200:
        raise AssertionError(f"{expression}: Prometheus HTTP {status}: {body[:300]}")
    result = json.loads(body)["data"]["result"]
    if len(result) != 1:
        raise AssertionError(f"{expression}: expected one series, got {result}")
    return {int(float(t)): float(v) for t, v in result[0]["values"]}


def test_remote_read_hints() -> None:
    """Prometheus evaluates the query itself on what SensApp returns, so what SensApp returns
    for a hint (`func`, `step`, `range`) must give the same answer as the raw samples.

    Prometheus asks for the window (t - range, t] of every evaluation time t, and SensApp
    answers with one bucket per `step`: this is exact when the range is a multiple of the step
    for the functions that can be merged (min, max, sum, last), and the average is then an
    average of equal-sized buckets. Everything else must be answered with the raw samples.
    """
    tag = uuid.uuid4().hex[:8]
    t0 = int(time.time()) // HOUR * HOUR - 7 * HOUR
    names = {s: f"hints_{tag}_{s}" for s in "ab"}
    payload = [
        {"n": names[s], "v": hint_value(s, m), "t": t0 + 60 * m}
        for s in "ab"
        for m in range(MINUTES)
    ]
    status, body = request(SENSAPP, "/publish", json.dumps(payload).encode())
    if status != 200:
        raise AssertionError(f"Could not seed SensApp: HTTP {status}: {body[:300]}")

    def window(series: str, t: int, seconds: int) -> list[float]:
        return [
            hint_value(series, m)
            for m in range(MINUTES)
            if t - seconds < t0 + 60 * m <= t
        ]

    def mean(values: list[float]) -> float:
        return sum(values) / len(values)

    def across(function):
        # What an aggregation operator sees: the last sample of each series within 5 minutes.
        return lambda t: function([window(s, t, 300)[-1] for s in "ab"])

    def over(function, seconds):
        return lambda t: function(window("a", t, seconds))

    selector = f'{{__name__=~"hints_{tag}_.*"}}'
    one = names["a"]
    cases = [
        # (expression, step, expected value at the evaluation time, answered in the database?)
        (f"avg_over_time({one}[1h])", HOUR, over(mean, HOUR), True),
        (f"min_over_time({one}[1h])", HOUR, over(min, HOUR), True),
        (f"max_over_time({one}[1h])", HOUR, over(max, HOUR), True),
        (f"sum_over_time({one}[1h])", HOUR, over(sum, HOUR), True),
        (f"last_over_time({one}[1h])", HOUR, over(lambda w: w[-1], HOUR), True),
        (f"avg_over_time({one}[15m])", 900, over(mean, 900), True),
        (f"max_over_time({one}[15m])", 900, over(max, 900), True),
        # A range that is several steps: the buckets still fit the windows.
        (f"max_over_time({one}[2h])", HOUR, over(max, 2 * HOUR), True),
        (f"min_over_time({one}[2h])", HOUR, over(min, 2 * HOUR), True),
        (f"sum_over_time({one}[2h])", HOUR, over(sum, 2 * HOUR), True),
        (f"avg_over_time({one}[2h])", HOUR, over(mean, 2 * HOUR), True),
        # A plain selector and the aggregation operators are evaluated on the latest sample of
        # each series at each step: SensApp answers with the last sample of each step.
        (f"sum({selector})", HOUR, across(sum), True),
        (f"avg({selector})", HOUR, across(mean), True),
        (f"min({selector})", HOUR, across(min), True),
        (f"max({selector})", HOUR, across(max), True),
        (f"count({selector})", HOUR, across(len), True),
        (one, HOUR, lambda t: window("a", t, 300)[-1], True),
        (one, 900, lambda t: window("a", t, 300)[-1], True),
        # Not answered with buckets, or the answer would be wrong:
        # a window shorter than the step, a count of samples.
        (f"avg_over_time({one}[30m])", HOUR, over(mean, 1800), False),
        (f"max_over_time({one}[10m])", 900, over(max, 600), False),
        (f"count_over_time({one}[1h])", HOUR, over(len, HOUR), False),
        # A subquery is evaluated on its own grid: the raw samples
        (
            f"max_over_time({one}[2h:30m])",
            HOUR,
            lambda t: max(window("a", t - 1800 * i, 300)[-1] for i in range(4)),
            False,
        ),
    ]

    failures = []
    for expression, step, expected, _in_database in cases:
        # The first evaluation time is chosen so that every window is full of samples.
        start, end = t0 + 2 * HOUR, t0 + 5 * HOUR
        got = query_range(expression, start, end, step)
        for t in range(start, end + 1, step):
            want = expected(t)
            if t not in got or abs(got[t] - want) > 1e-9 * max(1.0, abs(want)):
                failures.append(
                    f"{expression} step={step}s at T0+{(t - t0) / HOUR:.2f}h: "
                    f"Prometheus says {got.get(t)}, the raw samples say {want}"
                )
                break
    if failures:
        raise AssertionError("remote read hints give wrong answers:\n  " + "\n  ".join(failures))
    print(f"PASS remote read hints give the answer of the raw samples ({len(cases)} queries)", flush=True)


def query_range_values(
    expression: str, start: int, end: int, step: int
) -> dict[int, float]:
    """Like `query_range`, for an expression that may have no point at all."""
    params = urllib.parse.urlencode(
        {"query": expression, "start": start, "end": end, "step": step}
    )
    status, body = request(PROMETHEUS, "/api/v1/query_range?" + params, timeout=60)
    if status != 200:
        raise AssertionError(f"{expression}: Prometheus HTTP {status}: {body[:300]}")
    result = json.loads(body)["data"]["result"]
    if len(result) > 1:
        raise AssertionError(f"{expression}: expected one series, got {result}")
    return {int(float(t)): float(v) for t, v in result[0]["values"]} if result else {}


def test_remote_read_plain_selector_over_a_long_range() -> None:
    """A series of more samples than a read may return, graphed at a coarse step: the raw samples
    are rejected over 100,000 of them, and a plain selector only needs the last sample of each
    step. A series with a gap longer than the lookback of Prometheus must keep its gaps."""
    tag = uuid.uuid4().hex[:8]
    minutes = 120_000  # 83 days, one sample a minute
    t0 = int(time.time()) // HOUR * HOUR - 90 * 24 * HOUR
    steady, sparse = f"long_{tag}_steady", f"long_{tag}_sparse"

    def steady_value(minute: int) -> float:
        return float((minute * 7919) % 1000)

    def sparse_value(minute: int) -> float:
        return float(minute % 1000)

    # The sparse series has samples between minutes 10 and 19 of each hour only
    payload = [{"n": steady, "v": steady_value(m), "t": t0 + 60 * m} for m in range(minutes)]
    payload += [
        {"n": sparse, "v": sparse_value(m), "t": t0 + 60 * m}
        for m in range(minutes)
        if 10 <= m % 60 < 20
    ]
    for chunk in range(0, len(payload), 50_000):
        status, body = request(
            SENSAPP,
            "/publish",
            json.dumps(payload[chunk : chunk + 50_000]).encode(),
            timeout=120,
        )
        if status != 200:
            raise AssertionError(f"Could not seed SensApp: HTTP {status}: {body[:300]}")

    failures = []
    # Evaluations on the hour, and half an hour after: the steps are not tied to the hour
    for offset in (0, 1800):
        start = t0 + HOUR + offset
        end = start + (minutes // 60 - 3) * HOUR
        for expression, step, expected in [
            (steady, HOUR, lambda t: steady_value((t - t0) // 60)),
            (f"max({steady})", HOUR, lambda t: steady_value((t - t0) // 60)),
        ]:
            got = query_range_values(expression, start, end, step)
            wrong = [
                t
                for t in range(start, end + 1, step)
                if t not in got or got[t] != expected(t)
            ]
            if wrong:
                failures.append(
                    f"{expression} offset={offset}s: {len(wrong)} of "
                    f"{(end - start) // step + 1} points differ from the raw samples, "
                    f"the first at T0+{(wrong[0] - t0) / HOUR:.1f}h: got {got.get(wrong[0])}, "
                    f"want {expected(wrong[0])}"
                )

    # Every 15 minutes: only the evaluation at :15 has a sample of the last 5 minutes (the one of
    # minute 19 is 11 minutes old at :30), the others have no point
    start, end = t0 + HOUR, t0 + 40 * 24 * HOUR
    got = query_range_values(sparse, start, end, 900)
    for t in range(start, end + 1, 900):
        minute = (t - t0) // 60
        want = sparse_value(minute) if 10 <= minute % 60 < 20 else None
        if got.get(t) != want:
            failures.append(
                f"{sparse} step=900s at T0+{(t - t0) / HOUR:.2f}h: got {got.get(t)}, want {want}"
            )
            break

    if failures:
        raise AssertionError(
            "remote read of a plain selector over a long range:\n  " + "\n  ".join(failures)
        )
    print("PASS a plain selector over more samples than the limit gets the answer of the raw samples", flush=True)


def main() -> None:
    wait_for("SensApp readiness", lambda: ready(SENSAPP))
    wait_for("Prometheus readiness", lambda: ready(PROMETHEUS))
    test_remote_write()
    test_remote_read()
    test_remote_read_hints()
    test_remote_read_plain_selector_over_a_long_range()


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, OSError, ValueError, KeyError, TypeError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
