#!/usr/bin/env python3
"""Time a Prometheus remote read request with a `step` hint against a SensApp server.

    tests/perf/remote_read.py http://localhost:3980 'cpu usage' host h7 [step_seconds] [repeat]
    tests/perf/remote_read.py http://localhost:3980 'cpu usage' core '~c70[0-9]'   # a regex

Selects `{__name__="<metric>", <label>="<value>"}` over 150 seconds from 2023-11-14T22:13:20Z
(the data of `scale.sh`), averaged by buckets of `step_seconds` (default 60). Prints the median time
of `repeat` requests (default 3), the status and the size of the answer. No dependency: the request
is a protobuf written by hand, and snappy-compressed with literals only (a valid snappy stream).
"""
import statistics
import sys
import time
import urllib.error
import urllib.request


def varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def field_varint(number: int, value: int) -> bytes:
    return varint(number << 3) + varint(value)


def field_bytes(number: int, payload: bytes) -> bytes:
    return varint(number << 3 | 2) + varint(len(payload)) + payload


def snappy_literals(data: bytes) -> bytes:
    out = bytearray(varint(len(data)))
    for start in range(0, len(data), 60):
        chunk = data[start : start + 60]
        out.append((len(chunk) - 1) << 2)
        out += chunk
    return bytes(out)


def read_request(metric: str, label: str, value: str, step_ms: int) -> bytes:
    start_ms, end_ms = 1_700_000_000_000, 1_700_000_150_000
    # A value that starts with "~" is a regular expression (anchored by the server)
    matchers = b"".join(
        field_bytes(
            3,
            field_varint(1, 2 if val.startswith("~") else 0)
            + field_bytes(2, name.encode())
            + field_bytes(3, val.lstrip("~").encode()),
        )
        for name, val in (("__name__", metric), (label, value))
    )
    hints = (
        field_varint(1, step_ms)
        + field_bytes(2, b"avg_over_time")
        + field_varint(3, start_ms)
        + field_varint(4, end_ms)
    )
    query = field_varint(1, start_ms) + field_varint(2, end_ms) + matchers + field_bytes(4, hints)
    return field_bytes(1, query)


def main() -> None:
    url, metric, label, value = sys.argv[1:5]
    step_seconds = int(sys.argv[5]) if len(sys.argv) > 5 else 60
    repeat = int(sys.argv[6]) if len(sys.argv) > 6 else 3
    body = snappy_literals(read_request(metric, label, value, step_seconds * 1000))
    timings, status, size = [], 0, 0
    for _ in range(repeat):
        request = urllib.request.Request(
            f"{url}/api/v1/prometheus_remote_read",
            data=body,
            headers={"content-encoding": "snappy", "content-type": "application/x-protobuf"},
        )
        started = time.monotonic()
        try:
            with urllib.request.urlopen(request) as response:
                status, size = response.status, len(response.read())
        except urllib.error.HTTPError as error:
            status, size = error.code, len(error.read())
        timings.append(time.monotonic() - started)
    print(f"{statistics.median(timings):.4f} s (HTTP {status}, {size} bytes)")


if __name__ == "__main__":
    main()
