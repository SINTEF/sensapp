# Code review of 10 November 2026: fix order

This file is the index of the twenty task files written after the logic and safety review of
10 November 2026 (on `main` at `1404d19`, release 0.4.2). Fix them in the order below. Delete this file when
the last task has moved to `done/`.

## How the findings were produced, and what that means for the developer

The review read the code; it did not run it against any database. Each task file says, per item, whether it
was confirmed by reading or only reported. Treat every item as a hypothesis until a test shows it:

1. Write the failing test first. If the test cannot be made to fail, the finding is wrong: note that in the
   task file and move on.
2. Fix, then run `cargo check`, `cargo test`, `cargo clippy` and `cargo clippy --tests`.
3. Write tests against the generic storage harness so they run on every backend. Do not special-case
   PostgreSQL.
4. One commit per item. Where a task asks for a measurement, write the numbers in the task file.
5. SensApp runs as several instances on one database: no fix may rely on in-process state for correctness.
6. Line numbers are those of `1404d19` and will drift as fixes land; search for the quoted code.
7. When a task is finished, move its file to `done/` with the outcome written in it.

The suggested solutions are suggestions. Where a file says "decide", write the decision in the file.

## Order

### Step 1: data corruption and the authorization bypass

| # | Task file | Why now |
| --- | --- | --- |
| 1 | `sqlite-id-cache-survives-rollback.md` | Samples filed under the wrong sensor after a timeout. Small fix: remove five caches. |
| 2 | `sensor-identity-check-on-write.md` | Allow-list bypass, and samples written where no read finds them. Touches registration on every backend, so do it before the other storage tasks. |
| 3 | `handler-panics-kill-the-instance.md` | One request stops an instance. Each fix is a few lines. |
| 4 | `integer-timestamp-conversions.md` | Wrong timestamps on write, and the SenML export is wrong today. One shared helper. |

### Step 2: wrong data accepted with a success answer

| # | Task file | Why now |
| --- | --- | --- |
| 5 | `csv-timezone-and-timestamp-inference.md` | Offsets applied with the wrong sign; row numbers stored as timestamps. |
| 6 | `arrow-import-nulls-and-sensor-columns.md` | Nulls stored as zeros; also closes the compressed-body memory hole. |
| 7 | `clickhouse-numeric-and-uuid-identity.md` | Values stored off by a power of ten; depends on task 2 for the registration check. |
| 8 | `senml-import-export-consistency.md` | Depends on tasks 2 and 4. |

### Step 3: availability

| # | Task file | Why now |
| --- | --- | --- |
| 9 | `request-input-limits.md` | Stack overflows and unauthenticated memory growth. |
| 10 | `graceful-shutdown-on-sigterm.md` | Every rollout cuts writes. Small. |
| 11 | `payload-errors-answer-400.md` | Prometheus retries a bad batch forever. |
| 12 | `postgres-regex-validation-and-statement-timeout.md` | Regex validation is needed by task 13; the statement timeout protects the pool. |

### Step 4: wrong query results

| # | Task file | Why now |
| --- | --- | --- |
| 13 | `selector-and-promql-semantics.md` | `__name__` ignored on `/series`; changes the identity of remote-write series, so do it before more data is written. |
| 14 | `numeric-and-non-finite-values-across-backends.md` | Needs decisions on NaN and Numeric rules; one generic test suite. |
| 15 | `prometheus-remote-read-corrections.md` | Remove the `println!` calls first (five minutes); the hint items need checking against Prometheus. |
| 16 | `delete-series-racing-publish.md` | Concurrency; needs a new concurrent test before any change. |

### Step 5: per-backend and hardening

| # | Task file | Why now |
| --- | --- | --- |
| 17 | `sqlite-divergences-and-limits.md` | After task 1. |
| 18 | `duckdb-connection-model-and-divergences.md` | Largest change of the list; measure before and after. |
| 19 | `bigquery-and-rrdcached-corrections.md` | Experimental backends. |
| 20 | `auth-config-and-deployment-hardening.md` | Many small independent items. |

## Dependencies between tasks

- Task 2 before 7 and 8 (they use the registration check).
- Task 4 before 8 (SenML timestamps).
- Task 12 before 13 (regex validation).
- Task 1 before 17 (same files).
- Task 3 adds a catch-panic layer; task 9 covers what that layer cannot catch (stack overflows).

## Corrections to the review summary given in chat

- The remote-read path has 17 `println!` calls, not 18 (9 in `src/http/prometheus_read.rs`, 8 in
  `src/parsing/prometheus/stream_writer.rs`).

## After the fixes

The reviewer will read the result. For that review, keep in each task file: which items were fixed, which
were found wrong and why, the decisions taken, and the measurements.
