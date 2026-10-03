# Experiment: deduplicate samples at ingestion

Branch `dedup-at-ingestion`. This is an experiment, not a commitment: measure, then decide whether it
goes further (and into a pull request) or stays a branch.

## Goal

Today a sample written twice is stored twice; `POST /api/v1/admin/vacuum` removes the exact duplicates
afterwards (`done/sample-deduplication-in-vacuum.md`). The question: what does it cost to drop them
**when they are written**, in latency, throughput and complexity, backend by backend? Is it worth making
it a real feature, or is the vacuum enough?

## Semantics (same as the vacuum, on purpose)

- A duplicate is an **exact** one: same series, same timestamp, same value (same coordinates for a
  location). It is dropped silently; the request succeeds. The first one written stays.
- Two different values at one timestamp are both kept. No conflict policy is invented here. (The other
  possible rule, "one value per series and timestamp" with a unique key, is a different feature: it rejects
  or overwrites corrections. Not tested.)
- Duplicates inside one request are dropped too.
- Opt-in switch while this is an experiment, so that the same binary gives the before and the after:
  `SENSAPP_DEDUPLICATE_ON_INGEST` (default off).
- **Must hold with several instances** (SensApp scales horizontally, behind a load balancer). A read
  before the insert is not enough: a retry after a timeout can reach another instance while the first
  request is still running, and under READ COMMITTED its `NOT EXISTS` cannot see the uncommitted rows.
  A design that does not survive two concurrent writers of the same sample is only a best-effort filter.
  The options, per backend:
  - a unique constraint plus `ON CONFLICT DO NOTHING`: rock solid, but a B-tree on every row (instead of
    BRIN) and a different key rule on TimescaleDB;
  - PostgreSQL family without schema change: a transaction-level advisory lock per series, taken in sorted
    order, *before* the `NOT EXISTS` (each statement takes a new snapshot, so it then sees what the other
    writer committed). Writers of the same series are serialized, other series are not;
  - SQLite and DuckDB: one writer at a time already, one process;
  - ClickHouse: nothing is rock solid at insert time (no unique key, no transaction). Only eventual
    (`ReplacingMergeTree` merges, the vacuum).
  A concurrency test (the same new samples written by many tasks at once must be stored once) decides.

## Approach (no schema change)

The value tables only have a BRIN index on PostgreSQL and a plain `(sensor_id, timestamp_us)` index on
SQLite, and a unique index would turn the cheap BRIN into a B-tree on every row (and TimescaleDB makes
unique indexes on compressed chunks slower). So the experiment filters in the write itself:

- PostgreSQL, TimescaleDB, SQLite: the `INSERT` selects the rows of the batch that are not already stored
  (`WHERE NOT EXISTS` on sensor, timestamp, value), bounded to the time window of the batch, with
  `DISTINCT` for the duplicates inside the batch.
- DuckDB: appender into a temporary table, then the same `INSERT .. SELECT`.
- ClickHouse: no transaction and no unique key, so the keys of the batch window are read first and the
  rows already there are filtered out in Rust.

Phase A: PostgreSQL, TimescaleDB, SQLite (the same shape). Phase B: DuckDB and ClickHouse. Measure after
each phase. BigQuery and RRDCached: not part of it.

## Benchmark

`tests/perf/dedup.sh` (server and database) and `tests/perf/dedup.py` (the requests). Release build,
one run each, same machine, **indicative**: PostgreSQL is the Homebrew 18 server on the host, TimescaleDB
and ClickHouse run in Docker, so compare a backend with itself, not backends together.

A table of 1 000 series x 1 000 samples (1 million rows) is loaded, then:

| step | what it tells |
|---|---|
| history | cost of fresh data and of registering the series (nothing to deduplicate) |
| append | one request, 20 new samples of each of the 1 000 series (20 000 samples): the normal write |
| replay of the append | the same request again: a retry after a timeout, 100% duplicates |
| replay of old samples | 100 series, their 200 oldest samples: duplicates far back in the table |
| half new, half duplicates | one request, 10 000 new and 10 000 stored |
| 200 small requests | one series, 10 samples, one after the other: latency of a sensor that posts often |
| same small requests again | the same, all duplicates |

The last line is the number of rows the database holds, against the distinct samples sent
(1 032 000 of the 1 084 000 sent).

## Baseline (3 Oct 2026, `main` at e925cbb, no deduplication)

| step | SQLite | PostgreSQL | TimescaleDB | DuckDB | ClickHouse |
|---|---|---|---|---|---|
| history, 1 M samples | 2.57 s | 6.94 s | 46.1 s | 2.96 s | 4.19 s |
| append, 20 000 samples | 0.096 s | 0.130 s | 0.874 s | 0.059 s | 0.090 s |
| replay of the append | 0.170 s | 0.141 s | 0.839 s | 0.059 s | 0.104 s |
| replay of old samples (20 000) | 0.068 s | 0.113 s | 0.876 s | 0.049 s | 0.080 s |
| half new, half duplicates (20 000) | 0.139 s | 0.117 s | 0.865 s | 0.057 s | 0.068 s |
| 200 small requests, median | 0.4 ms | 0.6 ms | 3.7 ms | 0.7 ms | 6.2 ms |
| 200 small requests again, median | 0.3 ms | 0.5 ms | 3.7 ms | 0.8 ms | 5.6 ms |
| rows stored (sent 1 084 000, distinct 1 032 000) | 1 084 000 | 1 084 000 | 1 084 000 | 1 084 000 | 1 084 000 |

Every duplicate is stored: the row count equals the number of samples sent.

## Done when

- [ ] Phase A implemented behind the switch, tests (backend-generic) green on SQLite, PostgreSQL, TimescaleDB.
- [ ] Phase A measured, numbers below.
- [ ] Phase B (DuckDB, ClickHouse) implemented and measured.
- [ ] Verdict: complexity, latency, throughput, what is left unsolved. Decide with the maintainer.

## Progress

3 Oct 2026: benchmark written, baseline taken (above).
