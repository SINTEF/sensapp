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

## What was built (no schema change)

The value tables only have a BRIN index on PostgreSQL and a plain `(sensor_id, timestamp_us)` index on
SQLite, and a unique index would turn the cheap BRIN into a B-tree on every row (see the numbers below).
So the write itself filters, behind `SENSAPP_DEDUPLICATE_ON_INGEST=true` (or
`StorageInstance::set_deduplicate_on_ingest`, which the tests use):

- **PostgreSQL, TimescaleDB** (`src/storage/pg_samples.rs`, shared): the `unnest` insert becomes
  `WITH u AS (<source>) INSERT .. SELECT DISTINCT .. FROM u WHERE NOT EXISTS (stored row in the time
  window of the statement with the same series, time, value)`. Three things were needed to make it work:
  1. the **window bound**: probing the BRIN index once per row costs 0.36 ms a row (20 000 samples = 7 s);
     bounding the stored side to the batch window gives one bitmap scan and a hash anti join (2.5 ms for
     4 000 rows);
  2. a **planner guard** (`SET LOCAL enable_nestloop/enable_seqscan = off` for the inserts only): new samples
     are newer than the statistics, so the window is estimated at one row and the planner chose a nested
     loop (3 s to 5 s for 20 000 samples); a never analyzed table got a sequential scan;
  3. a **per-series advisory lock** (`pg_advisory_xact_lock`, sorted, before the strings) for several
     writers, see Concurrency.
- **SQLite**: `WITH u AS (VALUES ..) INSERT .. SELECT DISTINCT .. WHERE NOT EXISTS`, answered by the
  `(sensor_id, timestamp_us)` index.
- **DuckDB**: the appenders write a temporary table (`stage_<table>`), one statement copies the new rows,
  looking at the stored rows of the time window only.
- **ClickHouse, BigQuery, RRDCached**: not done, `set_deduplicate_on_ingest` answers "unsupported" (see
  Verdict for ClickHouse).

## Concurrency (several instances)

A read before the insert is only a best-effort filter. The test
`at_ingestion_concurrent_writers_of_the_same_samples_store_them_once` (8 writers, the same 50 new samples,
6 rounds) first stored **401 rows where 51 were expected** on PostgreSQL. What holds now:

| backend | several writers / instances | how |
|---|---|---|
| PostgreSQL, TimescaleDB | yes | advisory lock per series until the end of the transaction: the second writer waits, its next statement takes a new snapshot and sees the committed rows. Writers of the same series are serialized, other series are not. Locks are taken in id order and before anything else that can wait (strings), so they cannot deadlock. Costs one lock in the shared lock table per series of the batch (`max_locks_per_transaction`): a batch of thousands of series needs a higher value |
| SQLite, DuckDB | yes | one writer at a time, one process |
| ClickHouse | **no** | no transaction, no unique key: nothing exact is possible at insert time |

## Benchmark

`tests/perf/dedup.sh` (server and database) and `tests/perf/dedup.py` (the requests). Release builds, one
run each, same machine, **indicative**. PostgreSQL is the Homebrew 18 server on the host, TimescaleDB and
ClickHouse run in Docker: compare a backend with itself, not backends together.

A table of 1 000 series x 1 000 samples (1 million rows), written **in time order** (a fleet of collectors;
`ORDER=series` is the import case), then: **append** = one request, 20 new samples for each of the 1 000
series; **replay** = the same request again (a retry after a timeout, 100% duplicates); **old** = 100 series,
their 200 oldest samples (duplicates far back); **half** = 10 000 new and 10 000 stored in one request;
**small** = 200 requests of 1 series x 10 samples one after the other (median latency); **small, dup** = the
same again. Every run with deduplication stored exactly the distinct samples (1 032 000 of 1 084 000 sent);
without it, all 1 084 000.

Baseline and deduplication, as `without -> with`:

| | history (1 M) | append (20 k) | replay | old | half | small | small, dup |
|---|---|---|---|---|---|---|---|
| SQLite | 3.84 -> 4.02 s | 0.144 -> 0.167 s | 0.145 -> 0.073 s | 0.073 -> 0.059 s | 0.131 -> 0.134 s | 0.3 -> 0.3 ms | 0.3 -> 0.2 ms |
| DuckDB | 2.80 -> 2.97 s | 0.057 -> 0.065 s | 0.058 -> 0.059 s | 0.051 -> 0.057 s | 0.059 -> 0.062 s | 0.7 -> 1.9 ms | 0.6 -> 1.6 ms |
| TimescaleDB, as loaded | 42.9 -> 42.8 s | 0.82 -> 0.79 s | 0.77 -> 0.10 s | 0.78 -> 0.38 s | 0.79 -> 0.45 s | 3.8 -> 4.0 ms | 3.5 -> 4.3 ms |
| TimescaleDB, after `VACUUM ANALYZE` | (same) | 0.86 -> 0.86 s | 0.89 -> 0.09 s | 0.81 -> 0.32 s | 0.83 -> 0.51 s | 3.2 -> 4.8 ms | 3.4 -> 4.6 ms |
| PostgreSQL, as loaded | 6.06 -> 10.6 s | 0.119 -> 0.311 s | 0.110 -> 0.272 s | 0.105 -> 0.304 s | 0.108 -> 0.291 s | 0.5 -> **64 ms** | 0.5 -> **64 ms** |
| PostgreSQL, after `VACUUM ANALYZE` | (same) | 0.141 -> 0.146 s | 0.124 -> 0.071 s | 0.109 -> 0.179 s | 0.116 -> 0.105 s | 0.5 -> 4.1 ms | 0.5 -> 3.9 ms |
| PostgreSQL in Docker, `autosummarize` on, autovacuum every second | 6.76 -> 8.82 s | 0.134 -> 0.174 s | 0.126 -> 0.079 s | 0.128 -> 0.171 s | 0.126 -> 0.107 s | 2.1 -> 3.5 ms | 2.1 -> 4.1 ms |
| ClickHouse (baseline only) | 3.93 s | 0.084 s | 0.079 s | 0.065 s | 0.071 s | 5.7 ms | 6.6 ms |

What the PostgreSQL rows say: the probe reads the BRIN window, and **BRIN returns every range that is not
summarized yet in full**. Summaries are made by (auto)vacuum, so right after a bulk load, or in the tail of
a live table that autovacuum has not reached, a probe reads that part of the table (64 ms at 1 million
rows, and it grows with the unsummarized tail: about 20% of the table at the default insert threshold,
which at 100 million rows is seconds a request). With `autosummarize = on` on the BRIN indexes and a
responsive autovacuum it is 3.5 ms. Reads of the same table share the weakness; the dedup makes every write
depend on it. A model that would not: a B-tree on `(sensor_id, timestamp_us)`. Measured without any code
change (baseline binary, B-tree added): bulk load +17% (6.06 -> 7.1 s), small writes 0.5 -> 0.7 ms, **+35 MB
on a 54 MB table** (BRIN: 32 kB), and the probe becomes microseconds. Not implemented: a unique index would
also give the exact guarantee without locks, but values of JSON and blobs larger than about 2.7 kB cannot
be indexed, TimescaleDB unique indexes must contain the time column and are slower on compressed chunks, and
existing databases with duplicates would need the vacuum first.

## Verdict (to decide with the maintainer)

- **Cost**: SQLite and DuckDB: nothing visible for batches, DuckDB +1.3 ms on small requests. TimescaleDB:
  about +0.5 to +1.5 ms on small requests, and *faster* for replays (a replay inserts nothing, 0.8 s -> 0.1 s).
  PostgreSQL: +1 to +3 ms per small request and +0 to +50% on batches **when BRIN is summarized**; a cliff
  (64 ms, 3x on batches) when it is not.
- **Complexity**: not small. One shared SQL builder, a planner guard, advisory locks, a switch, a temp table
  on DuckDB, one more `SELECT` per transaction. About 400 lines and subtle: both the nested loop and the
  unsummarized BRIN were found by measuring, and the race by a test. It needs the PostgreSQL operational
  advice (`autosummarize`, `max_locks_per_transaction`) to be documented, or the B-tree.
- **Guarantee**: exact on PostgreSQL, TimescaleDB, SQLite, DuckDB. **Not possible on ClickHouse** with a
  read: the horizontally scaled backend is the one where it cannot be exact. The only race-free ClickHouse
  mechanism is block deduplication (`insert_deduplication_token`, needs `non_replicated_deduplication_window`
  on the tables), which removes a *retried identical request*, not partial overlaps or the same sample in
  another request. Not tried.
- **What it does not fix**: the same sample sent twice in two different requests *and* both transactions
  already past the lock (impossible by construction on the SQL backends), a `delete` plus a rewrite of the
  same samples, or any duplicate written before the switch was turned on (the vacuum still does that).
- **Reading of the numbers**: if the goal is "a retried request must not duplicate", this works on the SQL
  backends at a cost of a few milliseconds per request, with a PostgreSQL operational condition. If the goal
  is only cleanliness, the vacuum (2.4 s for 3 million rows) is much cheaper and has no condition. The
  decision is about whether retries are common enough (the Python SDK retries timeouts) to pay a few
  milliseconds on every write, and whether ClickHouse needs the token mechanism instead.

## Done when

- [x] Benchmark and baseline.
- [x] Implemented behind the switch on PostgreSQL, TimescaleDB, SQLite, DuckDB; backend-generic tests
  (every type, repeats inside a request, exact duplicates only, partial overlap, switching off, concurrent
  writers) green on those four; full suites on the five backends and clippy on all features.
- [x] Measured, verdict above.
- [ ] Decide: stop here (leave the branch), or make it a feature (docs, `autosummarize` migration or B-tree,
  ClickHouse token, config entry, pull request).

## Progress

3 Oct 2026: benchmark and baseline; PostgreSQL and TimescaleDB (found the race with a test, the nested
loop and the unsummarized BRIN by measuring); SQLite and DuckDB; results above. Branch `dedup-at-ingestion`.
