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

## Plan

- [ ] Task file and audit
- [ ] Tests that really assert (module tests with a scripted client, integration tests against the daemon)
- [ ] Heartbeat
- [ ] Never recreate an existing file
- [ ] Robust batches: sorted, deduplicated, stale samples tolerated
- [ ] Connection safety: cancelled requests, `PING` health check, no `FLUSHALL`
- [ ] Queries: window, limit, errors, empty windows, fresh tail
- [ ] Listing: sorted, paginated, only SensApp files
- [ ] Selectors: list once
- [ ] Documentation of the differences (`docs/RRDCACHED.md`, `docs/BACKENDS.md`)
- [ ] CI and Docker image
- [ ] Measured performance before and after
- [ ] Full gate (check, clippy, tests of the backends that share code) and pull request

## Progress notes

