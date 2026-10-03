# RRDCached: reach "good enough"

## Goal

RRDCached is a side-track backend ("store Prometheus' data in rrdtool"). It does not have to be
production-grade, but it has to **work**, perform well enough, and its differences from the other backends
must be written down. Unsupported features are fine as long as they are documented limitations.

## Audit (3 Oct 2026, against `rrdcached` 1.7.2 from the CI image and 1.11 from Homebrew)

The 12 existing integration tests pass, but most only print what they find. Probing a real daemon found:

| # | Finding | Impact |
|---|---------|--------|
| 1 | Data source heartbeat is 20 s. Samples more than 20 s apart become `NaN`. Measured through `POST /publish`: five samples a minute apart, one point comes back. | Data loss for any series that is not sampled every few seconds, Prometheus' default 1 min scrape included |
| 2 | `CREATE` overwrites an existing file (no `-O`). `created_sensors` is only an in-memory cache: after a SensApp restart, or from a second instance, the next write recreates the file and **erases the history**. Two concurrent requests of one process can do the same. | Data loss |
| 3 | A batch with one stale or duplicate timestamp (`illegal attempt to update using time`) makes `publish` fail with a 500, although rrdcached applied the rest. A Prometheus retry then fails forever. Samples of a series are not sorted before being sent. | Failing writes on retries and out of order input |
| 4 | `min_timestamp as u64 - 10` underflows for timestamps below 10 s (panic in debug, wrapped start in release); the RRD start is the oldest sample of the whole batch, not of the sensor. | Edge case |
| 5 | `FLUSHALL` after every publish, before every query and in the health check | Defeats the write cache; the readiness probe flushes everything |
| 6 | Queries: the first row of a window is missed (rows are stamped with the end of their interval), `end` alone or `start > end` return "not found", any daemon error is reported as "series not found", `limit` is ignored, a series with no sample in the window is "not found" instead of empty, the freshest rows of a long window are `NaN` because a coarse archive is chosen | Wrong or missing data |
| 7 | `list_series`: the daemon's `LIST` order is arbitrary but the bookmark assumes sorted order, so pagination skips or repeats series; files that are not SensApp's get a random UUID at every call | Broken pagination |
| 8 | `query_sensors_by_labels` lists, fetches every series to discover them, then the selector fetches them again | Twice the round trips |
| 9 | A cancelled request (HTTP timeout) leaves the shared connection mid-protocol: the next command reads the reply of the previous one | Wrong answers after a timeout |
| 10 | Non-numeric samples are dropped with a log line only; names, units, labels, types are not stored (by design) | Documented limitation |

## Done

- [x] Task file and audit, with an ignored benchmark (`rrdcached_performance`)
- [x] Heartbeat of an hour, `?heartbeat=` to change it (findings 1)
- [x] Never recreate an existing file: `no_overwrite` on each `CREATE`, "File exists" accepted (2)
- [x] Sorted, deduplicated batches; the daemon's refusal of old times is not a failed write (3, 4)
- [x] Cancelled requests drop the connection, 15 s timeout, `PING` health check, no `FLUSHALL`, daemon outages
      are 503 (5, 9)
- [x] Reads: window, first row, open windows, limit, errors, empty versus missing, freshest rows (6)
- [x] Listing sorted and paginated, only the files named like a series (7)
- [x] Selectors read each series once (8)
- [x] `rrdcached+unix://` was never routed by the storage factory ("Unsupported storage type"): fixed and tested
      by hand against a Homebrew daemon over a Unix socket
- [x] `docs/RRDCACHED.md` rewritten (differences, presets, time and gap semantics, operations, performance),
      `docs/BACKENDS.md`, `docs/CONFIGURATION.md`
- [x] CI runs the factory tests too

## Results

Unit tests: 54 with a scripted daemon (`storage::rrdcached`). Integration tests against a real daemon: 12 weak
tests replaced by 26 that assert, and 20 of the 24 first ones fail on the previous code. Passing on `rrdcached`
1.7.2 (Debian bookworm and trixie, what CI uses) and 1.11 (Homebrew).

Benchmark (`rrdcached_performance`, debug build, daemon in Docker on a laptop; two runs each):

| | before | after |
|---|---|---|
| Bulk load, 400 000 samples | 190 000 samples/s | 188 000 - 191 000 samples/s |
| Small write, 50 series | 1.95 - 2.09 ms | 1.28 - 1.35 ms (no `FLUSHALL`) |
| Scrape of 1 000 series | 8.0 - 9.3 ms | 7.7 - 8.0 ms |
| First write of 1 000 series | 0.72 - 0.82 s | 0.72 - 1.2 s (1.15 - 1.27 s while a `LAST` check came first) |
| Read, 1 hour, per series | 2.5 ms | 2.1 - 2.5 ms |
| Read, 22 hours, per series | 34 ms | 34 - 35 ms |
| Health check | 0.47 ms | 0.41 - 0.46 ms |
| `list_series`, 256 of 6 000 files | 74 - 96 ms | 35 - 49 ms |

## Not done

See `ideas/rrdcached-follow-ups.md`: a metadata sidecar, a connection pool.

## Client and image (follow-up of the same day)

`rrdcached-client` 0.3.0 (ours) is used. The test image is now the one of that repository: it builds `rrdcached` 1.11
from the upstream release (Debian's package is 1.7.2 on bookworm and trixie), runs as non-root.
Unit and integration tests pass on it, the benchmark is unchanged.

Then `rrdcached-client` 0.4.0 (`no_overwrite`, `-O` on each `CREATE`): SensApp creates files with it and no longer asks
the daemon first (no `LAST` before a creation, no lock), so the daemon does not need to run with `-O`. The image is a
plain copy of the client's. The 26 integration tests pass three times on a daemon started without `-O`; the first write
of 1 000 series is back to 0.7 ms per file.
