# First numbers: ingestion and reads on SQLite, PostgreSQL and TimescaleDB

A quick comparison made on 4 October 2026 while fixing the write timeout. It is **not** the proper
benchmark that is planned: one series, one client, one laptop, a Docker VM (Colima) between SensApp and
the two PostgreSQL-family databases, a median of 9 runs after a warm-up. Use it for orders of magnitude and
to choose what the real benchmark must cover. **Lesson for the real benchmark**: a PostgreSQL measured
right after a bulk load is not the same database as one measured a minute later (BRIN summaries, statistics,
plan caches), so wait for autovacuum or run `VACUUM ANALYZE`, and say which.

Setup: release build of `write-timeouts` (`--features timescaledb`), the Zeblab series
`zeb_320_001_338_320_001_Hpu001_AlmFl` (1,331,266 float samples, 2024-02-13 to 2026-10-01, about one per
minute, 0 or 1), written in **one** Arrow request with the SDK's default timeouts, `ANALYZE` on both
PostgreSQL databases afterwards. PostgreSQL 16.6; TimescaleDB 2.17.2 (the compose file pins 2.30.2).
Reads are `format=arrow`.

## Writing the whole history (one request)

| | SQLite | PostgreSQL | TimescaleDB |
|---|---|---|---|
| time | 5.1 to 5.9 s | 17.6 to 18.9 s | 51.6 to 56.6 s |
| samples/s | 230,000 to 260,000 | 70,000 to 76,000 | 24,000 to 26,000 |
| size on disk | 49 MB | 76 MB | 136 MB (not compressed) |

## Reading (median ms)

The PostgreSQL columns are the **settled** ones: measured after the BRIN index was summarized and the
table analyzed (see below). The first PostgreSQL run of the day, made right after the load, was up to 4
times slower on windows for that reason and is not shown. "B-tree only" is an experiment: the BRIN index
replaced by `CREATE INDEX ON float_values (sensor_id, timestamp_us)` before the load.

| query | SQLite (after the index fix) | PostgreSQL (BRIN) | PostgreSQL (B-tree only) | TimescaleDB |
|---|---|---|---|---|
| raw, one week (~20k samples) | 26 | 22 | **18** | 22 |
| raw, `limit=100000` | 106 | 141 | **74** | 85 |
| `step=1h avg`, one week (168 buckets) | **5.0** | 21 | 23 | 8.2 |
| `step=1m avg`, one month (~43k buckets) | **76** | 109 | 110 | 84 |
| `step=1d avg`, whole history (875) | 471 | 561 | 417 | **181** |
| `step=1h avg`, whole history (20k) | 466 | 579 | 582 | **247** |
| `step=1h max`, whole history | 464 | 419 | 420 | **237** |
| `step=1d last`, whole history | 1,039 | 561 | 560 | **285** |
| latest sample (`/last`) | **0.5** | 65 | 105 (5 on a fresh server) | 12 |
| availability, `step=1d`, 2.6 years | **103** | 262 | 265 | 217 |

## Reading the numbers

- **SQLite's windows were slow because of our queries**, not SQLite (`done/sqlite-windowed-reads-use-the-index.md`):
  fixed, `/last` went from 54 to 0.5 ms. What is left in the SQLite column is the cost of reading every row.
- **BRIN is not the problem for windows.** Settled, PostgreSQL with BRIN reads a week in 22 ms, as fast as
  TimescaleDB and as the B-tree (18 ms). Two things made it look bad:
  - *A BRIN index is only useful once summarized.* Right after the bulk load the index had no usable summary:
    for a one-week window PostgreSQL rechecked 1,306,082 rows and read all 8,448 heap blocks (91 ms, the whole
    table); after `brin_summarize_new_values` it rechecked 9,984 rows and read 192 blocks. With
    `autosummarize = on` an autovacuum worker catches up: about 20 s in this container (`autovacuum_naptime`
    10 s), but the default naptime is 1 min, and what a dashboard reads first is the newest data.
  - *The `$2::BIGINT IS NULL OR timestamp_us >= $2` form of the PostgreSQL queries* (the same pattern as SQLite's).
    A prepared statement uses a custom plan for its first executions and then may switch to a generic plan,
    where that form cannot be used as an index condition. Forced generic plan, one series of 1.33 M samples,
    one-week window / the last sample: **B-tree 115 ms / 94 ms, BRIN 30 ms / 26 ms (sequential scan)**;
    with `timestamp_us >= COALESCE($2, -9223372036854775807)`: **B-tree 1.8 / 0.03 ms, BRIN 3.0 / 0.34 ms**.
    In the benchmark it shows as `/last` going from 5 ms on a fresh server to 105 ms after ten raw reads,
    for every call that follows. That is also why small PostgreSQL numbers moved so much between runs.
- **What BRIN cannot do, in SQL on a scratch database** (1.33 M rows, 66 MB table, fully summarized):

  | | BRIN | B-tree |
  |---|---|---|
  | index size | 32 kB | 40 MB (51 MB with 1,000 series) |
  | `latest sample` (`ORDER BY ... DESC LIMIT 1`), 1 series | 67 ms, reads the whole table | **0.06 ms**, 7 buffers |
  | 1-hour window, 1 series | 0.49 ms, 56 buffers | **0.07 ms**, 8 buffers |
  | 1-week window, 1 series | 3.1 ms | **1.5 ms** |
  | 1,000 series interleaved by time: 1-hour window | 3.9 ms, 470 buffers | **0.27 ms**, 68 buffers |
  | 1,000 series interleaved: 1-week window | 40 ms, 4,278 buffers | **2.4 ms**, 676 buffers |
  | 1,000 series interleaved: latest sample | 32 ms, whole table | **0.06 ms** |

  A BRIN index stores the minimum and maximum of each range of 32 pages. It skips ranges that cannot match,
  but it cannot give rows in order (no `ORDER BY ... LIMIT 1`, so the latest sample is a scan of the table),
  it returns whole ranges to be rechecked row by row, and when many series are written side by side every
  range holds every series, so reading one series costs a share of all of them (the 1,000-series rows).
- **Writing**: a B-tree costs nothing visible through SensApp on this load (17.7 s with a B-tree only,
  18.0 s with both, against 17.2 to 18.9 s with BRIN only; in plain SQL it doubled the insert, 1.2 to 2.4 s),
  and 40 MB on a 66 MB table. The load is limited by something else (parsing, the transaction, the `unnest`
  insert). Not measured: a table larger than memory, random arrival order, many small requests.
- **The whole-history aggregations are 2.5 times faster on TimescaleDB, and no index changes that**
  (417 to 582 ms on PostgreSQL with BRIN or a B-tree). In `src/storage/postgresql/queries.rs` every row goes
  through `date_bin(..., to_timestamp(timestamp_us / 1000000.0), ...)` and `EXTRACT(EPOCH ...)`, while
  TimescaleDB uses `time_bucket` on its native timestamp. In `psql` on the same data (875 daily buckets, the
  same averages): **1.25 s with the current expression, 0.21 s with integer arithmetic, 0.065 s for a bare
  `avg(value)` scan.**
- **What TimescaleDB costs**: 3 times the write time of PostgreSQL for a backfill (a chunk per week of
  history to create, and the B-tree), and 1.8 times the disk, before compression (not tried). Its hypertables
  (7-day chunks, 2 hash partitions per series, a B-tree on time) get the B-tree, chunk exclusion and
  `time_bucket` for free; the two PostgreSQL fixes below give plain PostgreSQL most of that.
- Whole-history aggregations touch every row, so they scale with the number of samples. A series of
  100 M samples will not answer in a few seconds without pre-aggregation (TimescaleDB continuous
  aggregates, not tried).

## For the proper benchmark

Several series and several sizes (1 M to 1 G samples), concurrent clients, data arriving in time order as
well as a backfill, compression on and off for TimescaleDB, the version of the compose file, a repeat count
with a spread (the runs above disagree by 2 to 4 times on small queries), the cost of `format=arrow`
against `csv`, ClickHouse and DuckDB, the write side with `SENSAPP_DEDUPLICATE_ON_INGEST` on, and the
PostgreSQL variants of `ideas/postgresql-windows-and-buckets.md`.
