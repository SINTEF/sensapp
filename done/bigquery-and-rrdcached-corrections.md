# BigQuery and RRDCached corrections

Source: code review of 10 November 2026. Reported from reading the code and the client crates; nothing was
run against BigQuery or rrdcached. Items marked (suspected) depend on service behaviour that was not checked.

## BigQuery

1. **An append with zero responses is a success** (`src/storage/bigquery/publishers.rs:151-175`). The loop
   only looks for errors, so it passes when `responses` is empty, when there are fewer responses than the
   requests the client cut the batch into (it splits at 10 MB), or when `response.response` is `None`. If
   the stream ever ends early with an OK status, a write is acknowledged that BigQuery never confirmed.
   (suspected: no evidence that the service does this.)
   - Fix: require one `Some(Response::AppendResult(_))` per request sent, else return an error.
2. **Availability uses the trait default** (`src/storage/mod.rs:149-165`): it downloads every sample
   (`LIMIT 10000000`, `bigquery/mod.rs:381`) to compute a count, a minimum and a maximum, and truncates
   silently above 10 M samples.
   - Fix: one `SELECT COUNT(*), MIN(timestamp), MAX(timestamp), COUNT(DISTINCT bucket)` as ClickHouse does.
3. **`sensors_set()` uses `ANY_VALUE(uuid) … GROUP BY sensor_id`** (`bigquery/reads.rs:163-170`): if two
   UUIDs ever share an id, the one found is non-deterministic. Covered by the UUID check in
   `clickhouse-numeric-and-uuid-identity.md`.

11. **`list_metrics` returns no label keys** (`src/storage/bigquery/mod.rs:341-380`, `Vec::new()`), and
    groups by unit. Added 11 November 2026: the generic test `a_label_name_with_a_comma_is_one_label_key`
    (SQLite divergences task) expects the keys, and SQLite, PostgreSQL, DuckDB and ClickHouse return them.
    Aggregate the label names per metric (`ARRAY_AGG(DISTINCT ... IGNORE NULLS ORDER BY ...)`).

## RRDCached

4. **A far-future `end` produces an unbounded FETCH** (`src/storage/rrdcached/mod.rs:388-400`): `end` is
   only clamped below. `end = now + 1 year` at a 10 s step is about 3 million `NaN` lines read into memory
   while holding the only connection mutex (`connection.rs:169`), for up to 15 s, twice with the retry.
   (suspected: depends on `rrd_fetch` not clamping to the last update.)
   - Fix: `end = end.min(now + step)`.
5. **`list_series` filter is a substring match** (`rrdcached/mod.rs:684`, `name.contains(filter)`); every
   other backend matches exactly.
6. **Rows can be stamped after `end`** (`rows_in_window`, `mod.rs:145-155`): up to `step - 1` s past the
   requested end. Either clip or document it.
7. **The health check has no timeout of its own** (`mod.rs:859-867`): it waits behind any running FETCH or
   BATCH, up to 30 s. Give it 3 s, as ClickHouse has.
8. **A far-future sample blocks a series for good**: later real samples are "stale" (acknowledged, logged,
   not stored) and there is no API delete. Reject samples more than a configurable margin in the future
   (for example one step ahead of now) with an error instead of storing them.
9. **`list_metrics` returns an empty list silently**: return `Unsupported`, like the other unsupported
   methods.
10. **`rrdcached-client` near-miss** (our crate, `~/work/rrdcached-client`): `batch()` calls
    `command.to_command_string()?` after `BATCH` was sent; an error there would leave the daemon in batch
    mode. It cannot fail today because the timestamp is always `Some`. Build all command strings before
    sending `BATCH`, in the next release of the crate.

## Tests

Unit tests with a fake append stream for 1; the generic availability test for 2; RRDCached integration tests
for 4 (assert the FETCH range sent), 5, 7 and 8.

## Done when

The tests pass, and `docs/BIGQUERY.md` and `docs/RRDCACHED.md` are updated where behaviour changed.

## Outcome (11 November 2026)

BigQuery was **not run live**: a run bills the company project and needs the user's credentials. The BigQuery
changes have unit tests only, and the next live run should cover them (`list_metrics`, availability, writes).

1. **Fixed**: `check_append_results` asks for one `AppendResult` per request. The number of requests is not
   returned by the client, so it is bounded from below by the bytes sent (`bytes_sent / 10 MiB`, rounded up:
   the client cuts at 9 MiB of rows and counts the stream name and the schema too). No answer, an answer
   without a result, or fewer results than that bound is a failure. Unit test
   `an_append_is_a_success_only_when_every_request_is_confirmed`. Whether the service ever does this is still
   unknown.
2. **Fixed**: availability is one statement (`COUNT(*)`, `MIN`, `MAX`, `COUNT(DISTINCT DIV(t - @start, @step))`)
   instead of reading every sample. Unit test on the statement.
3. **Nothing to do**: every read by UUID compares the UUID next to the id (`s.uuid = @uuid`), done in the
   ClickHouse Numeric and UUID identity task.
4. **Reproduced live**: `FETCH` of an existing file from an hour ago to a year ahead answered 3,153,967 lines on
   the local daemon (rrdtool 1.9). The end of a read is now at most `future` seconds and one row after now. Unit
   test `an_end_in_the_future_is_read_up_to_now`.
5. **Fixed**: the `metric` filter of `list_series` is the whole UUID. Unit test
   `the_series_filter_is_the_whole_name`.
6. **Documented** in `docs/RRDCACHED.md`: the last row is stamped with the end of its interval, up to one row
   after `end`. Clipping it would drop the samples of that interval that are in the window.
7. **Fixed**: the health check waits 3 s at most, then answers unavailable. Unit test
   `the_health_check_does_not_wait_for_a_stuck_request` (fails without the timeout: it waited the 15 s of the
   stuck request).
8. **Fixed**: a sample more than `future` seconds after now (connection parameter, default 600) refuses the
   whole write with `400`. Unit test `a_sample_far_in_the_future_is_refused_and_nothing_is_written`.
9. **Fixed**: `list_metrics` is `Unsupported` (`501`).
10. **Fixed in the crate**, on the branch `batch-commands-before-batch` of `~/work/rrdcached-client` (commit
    fdb2f40): every command string is built before `BATCH` is sent. Not released; SensApp keeps 0.4.0 until the
    next release. Its unit tests pass, except `test_ping_unix`, which fails the same way without the change
    (it needs a daemon on a Unix socket).
11. **Fixed**: BigQuery `list_metrics` groups by name and type and returns the label keys as a JSON array
    (`TO_JSON_STRING(ARRAY_AGG(DISTINCT ...))`). Unit test on the statement.

Suites run: RRDCached unit tests (62) and live integration tests (27, the way CI runs them), BigQuery unit tests
(36), `cargo clippy` and `cargo clippy --all-features --tests`. The other backends were not run: no shared code
changed.
