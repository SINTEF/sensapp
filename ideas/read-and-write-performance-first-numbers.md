# First numbers: ingestion and reads on SQLite, PostgreSQL and TimescaleDB

A quick comparison made on 4 October 2026 while fixing the timeouts and the read queries. It is **not** the
proper benchmark that is planned: one series, one client, one laptop, a Docker VM (Colima) between SensApp and
the two PostgreSQL-family databases, a median of 9 runs after a warm-up.

Setup: release build of branch `write-timeouts` (`--features timescaledb`), the Zeblab series
`zeb_320_001_338_320_001_Hpu001_AlmFl` (1,331,266 float samples, 2024-02-13 to 2026-10-01, about one per
minute, 0 or 1), written in **one** Arrow request with the SDK's default timeouts. PostgreSQL 16.6;
TimescaleDB 2.17.2 (the compose file pins 2.30.2). Reads are `format=arrow`.

**The reads start 60 s after the load, with no manual `ANALYZE` or `VACUUM`**, as most workloads do not read
what they have just written. This matters: right after a bulk load a PostgreSQL BRIN index has no usable
summary (a one-week window rechecked 1,306,082 rows and read the whole table, 91 ms) until autovacuum has
summarized it (about 20 s in this container, `autovacuum_naptime` is 10 s here, 1 min by default), and the
first PostgreSQL run of the day, made right after the load, was up to 4 times slower on windows.

## Writing the whole history (one request)

| | SQLite | PostgreSQL | TimescaleDB |
|---|---|---|---|
| time | 5.1 to 5.9 s | 17.6 to 19.7 s | 51.6 to 58.4 s |
| samples/s | 230,000 to 260,000 | 68,000 to 76,000 | 23,000 to 26,000 |
| size on disk | 49 MB | 76 MB | 136 MB (not compressed) |

TimescaleDB: **23.5 s** in a throwaway experiment that restores the default plan caching inside its write
transaction (`ideas/timescaledb-write-plan-cache.md`): most of its slow load was that, not chunk creation.

## Reading (median ms, 60 s after the load)

"PostgreSQL before" is the settled BRIN run made before the two fixes of
`done/postgresql-windows-and-buckets.md` (manual `ANALYZE`, several minutes after the load); "after" is the
current code. SQLite is after its own fix (`done/sqlite-windowed-reads-use-the-index.md`).

| query | SQLite | PostgreSQL before | PostgreSQL after | TimescaleDB |
|---|---|---|---|---|
| raw, one week (~20k samples) | 22 | 22 | 25 | **20** |
| raw, `limit=100000` | **97** | 141 | 157 | 87 |
| `step=1h avg`, one week (168 buckets) | **4.6** | 21 | 9.6 | 7.9 |
| `step=1m avg`, one month (~43k buckets) | 73 | 109 | **58** | 77 |
| `step=1d avg`, whole history (875) | 452 | 561 | 135 | **99** |
| `step=1h avg`, whole history (20k) | 453 | 579 | 153 | **117** |
| `step=1h max`, whole history | 448 | 419 | 146 | **109** |
| `step=1d last`, whole history | 994 | 561 | 312 | **109** |
| latest sample (`/last`) | **0.5** | 65 | 47 | 12.6 |
| availability, `step=1d`, 2.6 years | **100** | 262 | 277 | 208 |

## Reading the numbers

- **SQLite's windows were slow because of our queries**, not SQLite: fixed, `/last` went from 54 to 0.5 ms.
  What is left in the SQLite column is the cost of reading every row (0.45 to 1 s for a whole-history
  aggregation of 1.33 M samples).
- **PostgreSQL got 2 to 4 times faster on aggregations** from the integer bucket expression (every row used
  to go through `date_bin(..., to_timestamp(timestamp_us / 1000000.0), ...)`: 1.25 s against 0.21 s in `psql`
  for 875 daily buckets, the same buckets on 1.4 M random timestamps) and from the time bounds, see below.
  TimescaleDB is still ahead on whole-history aggregations (99 to 117 ms against 135 to 153 ms), and
  PostgreSQL is now close or ahead on week and month windows.
- **BRIN is not the problem for windows.** Settled, a week costs 22 ms through the API (3 ms in SQL), like
  TimescaleDB. Its limits, in SQL on a scratch database (1.33 M rows, 66 MB table, fully summarized):

  | | BRIN | B-tree |
  |---|---|---|
  | index size | 32 kB | 40 MB (51 MB with 1,000 series) |
  | `latest sample` (`ORDER BY ... DESC LIMIT 1`), 1 series | 67 ms, reads the whole table | **0.06 ms**, 7 buffers |
  | 1-hour window, 1 series | 0.49 ms, 56 buffers | **0.07 ms**, 8 buffers |
  | 1-week window, 1 series | 3.1 ms | **1.5 ms** |
  | 1,000 series interleaved by time: 1-hour window | 3.9 ms, 470 buffers | **0.27 ms**, 68 buffers |
  | 1,000 series interleaved: 1-week window | 40 ms, 4,278 buffers | **2.4 ms**, 676 buffers |
  | 1,000 series interleaved: latest sample | 32 ms, whole table | **0.06 ms** |

  BRIN stores the minimum and maximum of each range of 32 pages: it cannot give rows in order (the latest
  sample reads the table, `/last` is 47 ms and grows with the table), it returns whole ranges to recheck, and
  when many series are written side by side every range holds every series. Writing a B-tree cost nothing
  visible through SensApp on this load (17.7 to 18.0 s with a B-tree, against 17.2 to 18.9 s with BRIN only;
  in plain SQL the insert took twice as long), for 40 MB on a 66 MB table. BRIN is kept for now.
- **Prepared statements and plans were the real PostgreSQL problem.** `sqlx` prepares every statement and
  PostgreSQL uses a generic plan after five executions, where the `($2 IS NULL OR timestamp_us >= $2)` bounds
  are only a filter: one week took 115 ms with a B-tree and 30 ms with BRIN instead of 2 to 3 ms, and `/last`
  went from 5 ms to 105 ms after ten reads. Rewriting the bounds as `COALESCE($2, <lowest>)` fixes the
  windows (3 ms) but not the reads without bounds (312 ms instead of 117 ms in a generic plan, a bitmap scan
  and a sort), so the PostgreSQL connections plan every statement with its parameters
  (`force_custom_plan`, as the TimescaleDB backend already did), and the **write transactions go back to
  the default** (`SET LOCAL plan_cache_mode = auto`): planning the bulk `INSERT .. SELECT unnest(...)` again
  for every batch took 200 ms and made the load 2.7 times slower (50.7 s).
- **What TimescaleDB costs**: 1.8 times the disk before compression (not tried), and a slower load that is
  mostly the forced custom plans (see above). What it gives: a B-tree on time, 7-day chunks with chunk exclusion
  at run time, and `time_bucket` on a native timestamp.
- Whole-history aggregations touch every row, so they scale with the number of samples. A series of
  100 M samples will not answer in a few seconds without pre-aggregation (TimescaleDB continuous
  aggregates, not tried).

## For the proper benchmark

Several series and several sizes (1 M to 1 G samples), concurrent clients, data arriving in time order as
well as a backfill, the delay between the load and the reads written down (and a run right after the load, which
is what a dashboard on fresh data sees), compression on and off for TimescaleDB, the version of the compose
file, a repeat count with a spread, the cost of `format=arrow` against `csv`, ClickHouse and DuckDB, the write
side with `SENSAPP_DEDUPLICATE_ON_INGEST` on, and PostgreSQL with a B-tree.
