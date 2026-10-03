#!/usr/bin/env python3
"""Write patterns that tell what removing duplicates at ingestion costs, against a SensApp server.

    tests/perf/dedup.py http://localhost:3982 [series] [samples_per_series]

Stdlib only. The database must be empty. InfluxDB lines `cpu,host=hN,core=cM usage=V`, the value
only depends on the series and the sample number, so that a replay is made of exact duplicates.
Prints one line per step: seconds, samples sent, samples per second. The last line is the number of
distinct samples sent, to compare with the row count of the database (`dedup.sh` does it).
"""
import http.client
import statistics
import sys
import time
import urllib.parse

BASE = urllib.parse.urlparse(sys.argv[1])
SERIES = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
HISTORY = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
T0 = 1_700_000_000  # seconds, 15 s between samples
APPEND = 20  # new samples per series for the append steps
LINES_PER_REQUEST = 100_000

sent = 0
distinct = set()


def line(series: int, i: int) -> str:
    value = series % 100 + (i % 1000) * 0.1
    return f"cpu,host=h{series % 30},core=c{series} usage={value} {(T0 + i * 15) * 10**9}"


def post(conn: http.client.HTTPConnection, lines: list[str]) -> float:
    body = "\n".join(lines).encode()
    start = time.perf_counter()
    conn.request("POST", "/api/v2/write?bucket=b&org=o", body)
    response = conn.getresponse()
    response.read()
    elapsed = time.perf_counter() - start
    if response.status >= 300:
        sys.exit(f"HTTP {response.status} for {len(lines)} lines")
    return elapsed


def connect() -> http.client.HTTPConnection:
    return http.client.HTTPConnection(BASE.hostname, BASE.port, timeout=600)


def request_lines(lines: list[str], conn: http.client.HTTPConnection) -> float:
    """One or more requests of at most LINES_PER_REQUEST lines; the time of all of them."""
    global sent
    total = 0.0
    for start in range(0, len(lines), LINES_PER_REQUEST):
        total += post(conn, lines[start : start + LINES_PER_REQUEST])
    sent += len(lines)
    return total


def report(name: str, seconds: float, samples: int) -> None:
    print(f"{name:<44} {seconds:8.3f} s  {samples:>8} samples  {samples / seconds:>10.0f} /s", flush=True)


def batch(series_range, sample_range) -> list[str]:
    out = []
    for s in series_range:
        for i in sample_range:
            out.append(line(s, i))
            distinct.add((s, i))
    return out


conn = connect()
print(f"series: {SERIES}, history: {HISTORY} samples each ({SERIES * HISTORY} rows)")

# 1. The history, written once. The registration of the series is in this step.
lines = batch(range(SERIES), range(HISTORY))
report("history (fresh data, series registered)", request_lines(lines, conn), len(lines))

# 2. What a collector does: every series gets new samples (APPEND each) in one request.
append = batch(range(SERIES), range(HISTORY, HISTORY + APPEND))
report("append, new samples of every series", request_lines(append, conn), len(append))

# 3. The same request again (a retry after a timeout): every sample is a duplicate.
report("replay of that append (all duplicates)", request_lines(append, conn), len(append))

# 4. A replay of old data: 100 series, their first 200 samples, long before the end of the table.
old = [line(s, i) for s in range(min(100, SERIES)) for i in range(min(200, HISTORY))]
report("replay of old samples (all duplicates)", request_lines(old, conn), len(old))

# 5. Half new, half already there, in one request.
half = batch(range(SERIES), range(HISTORY + APPEND, HISTORY + APPEND + APPEND // 2))
mixed = half + [line(s, HISTORY + i) for s in range(SERIES) for i in range(APPEND // 2)]
report("half new, half duplicates", request_lines(mixed, conn), len(mixed))

# 6. Small requests, one after the other (a sensor that posts its last 10 samples): latency.
start_i = HISTORY + 2 * APPEND
small = [
    [line(k % SERIES, start_i + 10 * (k // SERIES) + j) for j in range(10)] for k in range(200)
]
for k in range(200):
    for j in range(10):
        distinct.add((k % SERIES, start_i + 10 * (k // SERIES) + j))


def latencies(requests: list[list[str]]) -> list[float]:
    global sent
    times = []
    for lines in requests:
        times.append(post(conn, lines))
        sent += len(lines)
    return times


for name in ("200 small requests, new samples", "200 small requests again (duplicates)"):
    times = sorted(latencies(small))
    print(
        f"{name:<44} median {statistics.median(times) * 1000:7.1f} ms   "
        f"p95 {times[int(len(times) * 0.95)] * 1000:7.1f} ms   total {sum(times):6.2f} s",
        flush=True,
    )

print(f"samples sent: {sent}, distinct: {len(distinct)}")
