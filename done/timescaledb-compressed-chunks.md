# TimescaleDB: compressed and partially compressed chunks

## Context

PR 44 CI failed on `Test Suite (ubuntu-latest, stable, timescaledb)`: six tests that run the vacuum failed
with `transparent decompression only supports tableoid system column`. The same job had passed on the commit
before, with no change to that code.

## What was wrong

The Timescale schema compresses chunks older than 7 days with a background policy. The test data is from
2024, so whether a test sees compressed chunks depended on when the policy job happened to run. Digging into
that showed three separate problems, none of them specific to the test data:

1. **The vacuum failed on any compressed data.** `deduplicate_samples` deleted by `(tableoid, ctid)` and
   TimescaleDB cannot read `ctid` through a compressed chunk. In production, where old data is compressed,
   the vacuum could not work at all. `DATA_LIFECYCLE.md` said "tested on uncompressed chunks".
2. **Reads returned wrong results, silently.** sqlx prepares statements and PostgreSQL switches to a cached
   generic plan after five executions. TimescaleDB 2.17 does not invalidate such a plan when a chunk is
   compressed or receives a late write, so a long-lived connection kept an old plan: the raw read of a series
   of 140 samples returned 16 (a fresh connection returned 140). It needs a partially compressed chunk, that
   is a write into an already compressed chunk (a late or retried sample, a backfill).
3. **`COUNT(*)` failed to plan.** The aggregated `count` of one series, `GROUP BY time_bucket(..) ORDER BY ..`,
   failed with `MergeAppend child's targetlist doesn't match MergeAppend` over many chunks (a table with 162
   chunks, all compressed, reproduces it; whether it also needs compression was not isolated). `COUNT(*)` and `COUNT(time)` fail, `COUNT(value)`, `sum`, `avg`,
   `min` and `max` plan fine. It looked like a race with the compression job because it appeared intermittently
   in a shared test database, but a table with 162 chunks reproduces it with plain SQL.

## Delivered

- `deduplicate_samples` (TimescaleDB): finds the chunks that hold duplicates with `tableoid`, which
  transparent decompression supports, then for each chunk, in one transaction, decompresses it, deletes the
  duplicates by `ctid` on the plain chunk, and compresses it again if it was compressed. Duplicates are always
  in one chunk (same series, same time). Chunks without duplicates are not touched.
- Every TimescaleDB connection runs `SET plan_cache_mode = force_custom_plan`. A custom plan is made with the
  real parameters at each execution, which also lets the planner exclude chunks outside of the time window.
- The aggregated count is `COUNT(value)` (the value columns are `NOT NULL`, so it is the same number).
- Tests, all on TimescaleDB only:
  - `deduplication::duplicates_in_compressed_chunks_are_removed`: compresses every chunk, removes the
    duplicates, checks the clean series is untouched and that the chunks are compressed again.
  - `timescale_compressed::reads_give_the_same_answers_on_compressed_chunks`: records about 160 answers (raw
    reads, windows and limits, latest, availability, seven aggregations with and without a window, the
    selector reads) with eight series, then compresses one chunk of each pair, the old weeks, everything, and
    finally makes every chunk of both slices partially compressed, and requires the same answers each time.
    It fails without the plan cache setting (16 samples out of 140).
  - `timescale_compressed::writes_reach_compressed_chunks`: a write and a retry into compressed history, a
    range delete, a series delete, the vacuum.
- The test harness removes the compression policies from its TimescaleDB databases, so that no test depends on
  the moment a background job runs. Compression is done explicitly where it is under test. The production
  migration is unchanged.

## Verified

- Full TimescaleDB integration suite, 280 tests: green twice normally, and green twice while a loop compressed
  every chunk of every test database twice a second. That loop made the suite fail consistently before.
- SQLite (277), DuckDB (274), ClickHouse (290) suites green; clippy clean on all five feature sets.

## Not done

- The second slice of the schema (`by_hash('sensor_id', 2)`) is why every week has two chunks that overlap in
  time. It was not changed.
- CI keeps the pinned image `timescale/timescaledb:2.17.2-pg16`. A newer TimescaleDB may not need the two
  workarounds; this was not tried.
