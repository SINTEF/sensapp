# PostgreSQL: windows, the latest sample and buckets

Done on 4 October 2026 (branch `write-timeouts`) for points 1 and 2, see the Outcome at the end. BRIN stays (point 3). Original analysis:

Measured on 4 October 2026 (1.33 M samples of one series, release build, PostgreSQL 16.6; numbers in
`ideas/read-and-write-performance-first-numbers.md`). Nothing was changed yet. In order of value, and
none of the first two touches the schema.

## 1. The `IS NULL OR` form of the time bounds (the same bug as SQLite's)

Every PostgreSQL read writes its bounds as `($2::BIGINT IS NULL OR timestamp_us >= $2)`
(`src/storage/postgresql/mod.rs`, `batch_queries.rs`, `selector.rs`, and probably the TimescaleDB twins).
`sqlx` prepares the statement: the first executions on a connection use a custom plan, where the
parameters are known and the condition folds to a plain range, and later ones may use a **generic plan**, where
it cannot be an index condition. One-series table of 1.33 M samples, generic plan forced, one-week window /
the last sample:

| bounds written as | B-tree | BRIN |
|---|---|---|
| `($2 IS NULL OR timestamp_us >= $2)` (today) | 115 ms / 94 ms | 30 ms / 26 ms (sequential scan) |
| `timestamp_us >= COALESCE($2::BIGINT, -9223372036854775807)` | **1.8 ms / 0.03 ms** | **3.0 ms / 0.34 ms** |

Seen through the API: `/last` 5 ms on a fresh server, then 105 ms after ten raw reads, for all later calls
(`done/sqlite-windowed-reads-use-the-index.md` has the SQLite version). It also explains why small
PostgreSQL numbers varied so much between runs. The fix is the one that worked for SQLite
(`COALESCE` with the lowest and highest `i64`), with the same test (`tests/integration/time_window_reads.rs`
passes on PostgreSQL and TimescaleDB today; add a case that runs the same read more than five times on one
connection, which is when a plan changes). Check the aggregated queries (`$2::BIGINT IS NULL OR` in
`queries.rs`), the last-sample and availability paths, and TimescaleDB's (`time` column).

## 2. The bucket expression converts every row

`bucketed_query` in `src/storage/postgresql/queries.rs` (line ~639) buckets with
`EXTRACT(EPOCH FROM date_bin($4 * interval '1 millisecond', to_timestamp(timestamp_us / 1000000.0),
to_timestamp($5 / 1000000.0))) * 1000000` for each row, on a column that is already an integer in
microseconds. SQLite buckets with integer arithmetic. In `psql`, same data and same result (875 daily
buckets, the same averages):

| expression | time |
|---|---|
| current (`date_bin` over `to_timestamp(timestamp_us / 1000000.0)`) | 1.25 to 1.30 s |
| `origin + ((timestamp_us - origin) / step_us) * step_us` | 0.21 s |
| `SELECT avg(value)` with no bucket (the floor: reading the rows) | 0.065 s |

Through the API the same query takes 0.4 to 0.6 s (another plan, it seems), so the gain there is to be
measured. TimescaleDB uses `time_bucket` and is 2.5 times faster than PostgreSQL on these queries today; no
index changes that. Care: the integer form needs the same alignment as `date_bin` (origin = start of the
window, or 0), and a negative `timestamp_us - origin` truncates toward zero instead of flooring (samples
before the origin are not read, but say it in a comment). Check the `first`/`last` window-function variants
and the availability query (`COUNT(DISTINCT ...)` over buckets, 260 ms).

## 3. BRIN or B-tree (a schema decision, last)

BRIN is not the cause of slow windows: settled and with a plannable query it reads a week in 22 ms through the
API (3 ms in SQL), as fast as TimescaleDB, in a 32 kB index. Its two real limits:

- It cannot order: `ORDER BY timestamp_us DESC LIMIT 1` (the latest sample, `MAX(timestamp_us)`) reads the
  table: 67 ms for 66 MB, and the cost follows the size of the **table**, not of the series.
- It returns whole ranges: with 1,000 series written side by side every range holds every series, and a
  one-hour window costs 14 times a B-tree's (3.9 ms against 0.27 ms), a week 17 times (40 against 2.4 ms).
  Series that are written in turn, one after the other, would not have this problem.

And it is only useful once summarized: right after a bulk load PostgreSQL rechecked all 1.3 M rows (91 ms)
until autovacuum caught up (about 20 s here with `autovacuum_naptime` at 10 s; the default is 1 min, and
`autosummarize` is only a work item for autovacuum).

A B-tree on `(sensor_id, timestamp_us)` costs 40 to 51 MB on a 66 MB table and nothing visible on the load
through SensApp (17.7 to 18.0 s against 17.2 to 18.9 s with BRIN only; in plain SQL the insert took twice as
long). To decide, with a bigger table than memory, many small writes and random arrival order: whether the
B-tree *replaces* BRIN (one kind of index, no summarization to wait for, 2 DDL lines per table) or only
serves the latest sample. A third way keeps BRIN and the schema, and answers "the latest sample" and
availability from a small per-series table written with the samples (more schema, more write path,
multi-instance care): only if the B-tree turns out too expensive.

## Suggested order

1 and 2 first: no schema change, no write cost, tests exist. Re-measure, then decide 3 with the numbers of
the real benchmark.

## Outcome

- **Bucket expression (point 2)**: `bucketed_cte` computes the bucket on the integer,
  `timestamp_us - (((timestamp_us - origin) % step_us) + step_us) % step_us`, floored like `date_bin`.
  Compared with the old expression on 1.4 M random timestamps (7 steps, 4 origins, negative origin,
  samples before the origin): 0 differences. 1.25 s to 0.21 s for 875 daily buckets in `psql`.
- **Time bounds (point 1)**: the 38 predicates of `src/storage/postgresql/` are
  `timestamp_us >= COALESCE($2::BIGINT, -9223372036854775807)` (and the `<=` twin), as for SQLite.
  `tests/integration/time_window_reads.rs::time_bounds_are_written_in_a_plannable_form` fails when the
  `IS NULL OR` form comes back in `sqlite/` or `postgresql/`. TimescaleDB is left alone: with a forced
  generic plan it still excludes 131 of 133 chunks at run time and answers in a few milliseconds.
- **The `COALESCE` form alone is not enough**, which the benchmark showed: with a generic plan a read
  without bounds takes 312 ms (a bitmap scan on BRIN, then a sort) instead of 117 ms, and `MAX` 174 ms
  instead of 60 ms, because the planner cannot drop a predicate that is always true. Each way of writing the
  bounds fails one case in a generic plan, so the connections of the PostgreSQL backend now plan every
  statement with its parameters (`SET plan_cache_mode = force_custom_plan` in `after_connect`, as the
  TimescaleDB backend already did).
- **That setting made the load 2.7 times slower** (50.7 s instead of 18.6 s for 1.33 M samples): the bulk
  `INSERT .. SELECT unnest($1, $2, $3)` was planned again for every batch with thousands of constants, 200 ms
  each. `publish_once` runs `SET LOCAL plan_cache_mode = auto` first, which gives the writes their plan cache
  back (load 19.7 s again) and leaves the reads on custom plans.
- Measured (1.33 M samples, release build, **60 s between the load and the reads**, no manual `ANALYZE`;
  PostgreSQL median ms, before to after): raw week 22 to 25, `1h avg` week 21 to **9.6**, `1m avg` month
  109 to **58**, `1d avg` whole history 561 to **135**, `1h avg` 579 to **153**, `1h max` 419 to **146**, `1d
  last` 561 to **312**, `/last` 65 to **47** (BRIN cannot do better: it reads the table). Unchanged:
  `limit=100000` (141 to 157), availability (262 to 277).
- Left for later: the B-tree question (point 3), the availability query (`COUNT(DISTINCT ...)`), and
  `ideas/timescaledb-write-plan-cache.md`.
