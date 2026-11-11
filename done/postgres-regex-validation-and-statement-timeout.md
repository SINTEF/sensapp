# PostgreSQL and TimescaleDB: regex validation, statement timeout, schema details

Source: code review of 10 November 2026. The missing validation and timeout were confirmed by reading; the
PostgreSQL behaviours were reasoned, not executed.

## Problem

1. **Regex matchers reach the database unvalidated.** `anchor_regex` (`src/storage/query.rs:97-116`) only
   wraps the pattern as `^(?:pattern)$`. All values are bound, so there is no SQL injection, but:
   - `a)|(b` becomes `^(?:a)|(b)$`, which is valid and unanchored. Prometheus rejects that pattern.
   - PostgreSQL accepts back-references and lookahead, which RE2 forbids, and its engine is not linear-time.
   - `\b` means backspace in PostgreSQL; `(?-i)` and `\p{L}` are errors; `(?q)` makes the anchors literal
     text; `(?x)eu # c` swallows `)$`.
   - A regex error is a 500 (`src/http/app_error.rs:129-135`).
2. **No `statement_timeout` or `lock_timeout` anywhere in `src`.** The pool has 10 connections. Dropping the
   request future on the HTTP timeout does not cancel the server-side statement, so a few slow statements
   starve the writers (`PoolTimedOut`, 503).
3. **Unique B-tree on dictionary text** (`src/storage/postgresql/migrations/20240110123122_init.sql:27,45`,
   same in TimescaleDB): a string sample or label value above about 2.7 kB fails with "index row size exceeds
   btree maximum" and takes the whole batch with it (500).
4. **Migration with its own `BEGIN`/`COMMIT`**
   (`src/storage/postgresql/migrations/20250121000000_migrate_to_microseconds.sql:5,68`): sqlx already wraps
   the script, so the bookkeeping row is written outside the transaction. A crash in between leaves the
   migration applied and unrecorded.
5. **Indexes**: each dictionary column is indexed twice (the `UNIQUE` constraint and an explicit B-tree,
   `init.sql:115-119`); nothing supports the matcher subqueries (`labels(name, description)`,
   `sensors(name)`); TimescaleDB hypertables only have the default `time` index, so `WHERE sensor_id = $1`
   on uncompressed chunks filters every series' rows; TimescaleDB `list_series` runs one label query per
   series (`timescaledb/mod.rs:906-923`).
6. **Other 500s**: a very large `step` overflows `$4::bigint * 1000` (`postgresql/queries.rs:647`);
   `query_sensor_data_latest` runs two statements and can return `Some` with no sample after a concurrent
   delete (`postgresql/mod.rs:805-885`); `batch_query_samples` spawns up to 8 detached tasks against a pool
   of 10 (`batch_queries.rs:61-166`).

## Suggested solution

1. Validate every regex matcher with the `regex` crate (RE2-like syntax, the Prometheus dialect) in
   `LabelMatcher::new` or a fallible constructor, and answer 400 on failure. Reject, for PostgreSQL, the
   constructs whose meaning differs (`\b`, `\p{..}`, inline flags other than a leading `(?i)`), or translate
   the validated pattern to the PostgreSQL dialect. Validation fixes the unbalanced-parenthesis anchoring too.
2. Set `statement_timeout` (a little above the HTTP request timeout) and `lock_timeout` in the pool's
   `after_connect`, configurable. Vacuum and deduplication need their own, longer value (`SET LOCAL`).
3. Replace the unique index on the text with a unique index on a hash (`md5(value)` or `sha256`), and make
   the upserts conflict on that expression. Or cap string length at validation with a 400; the hash index
   is the complete fix.
4. Leave the old migration file alone (its checksum is recorded). Add a comment in the task file and make
   sure new migrations never contain `BEGIN`/`COMMIT`; a CI grep is enough.
5. One new migration: drop the four duplicate indexes, add `labels(name, description)` and `sensors(name)`,
   and `(sensor_id, time DESC)` on the TimescaleDB hypertables. Measure a selector read and a write batch
   before and after, and note the numbers here. Fetch TimescaleDB labels in one query.
6. Bound `step` in `SensorDataQueryOptions::validate`; make `latest` one statement
   (`ORDER BY timestamp_us DESC LIMIT 1`); lower the task fan-out or await the tasks so cancellation stops
   them.

## Tests

Regex cases above (400), a slow statement cancelled by the timeout (`pg_sleep`), a 10 kB string value stored
and read back, migrations applied on a fresh database and on a 0.4.2 database.

## Done when

The tests pass on PostgreSQL and TimescaleDB, with the measured numbers written here.

## Outcome (10 November 2026)

Each item was reproduced by a failing test first, except the race in `latest`, which was fixed by reading
the code.

1. **Regex matchers** (commit 7e511c2). A regex must compile with the `regex` crate, and may use only the
   constructs that mean the same thing on PostgreSQL. The leading flags `i` and `s` are allowed, along with
   the listed escapes, `(...)` and `(?:` groups, and character classes. Anything else answers 400.
   Anchoring puts the flags first: `(?s)^(?:body)$`. So `.` matches a newline on every backend, as it
   does in Prometheus. Tests: `regex_matchers::regexes_mean_the_same_on_every_backend` and
   `unsupported_regexes_are_refused`, plus a unit test in `query.rs`.
2. **Statement and lock timeouts** (commit 4adaf7c). Every pooled connection gets `statement_timeout`
   (130 s) and `lock_timeout` (60 s), unless the connection string sets them. Both are configurable. The
   vacuum and the deduplication run on a connection of their own, detached from the pool, with the
   maintenance timeout. Test: `pg_sleep(5)` is cancelled by the server after about 1 s (57014).
3. **Long texts** (commit fa8f200). The dictionaries are unique by `md5(text)`. The upserts conflict on
   that expression, and a lookup compares the hash, then the text. If a hash collides, the batch answers
   400. md5 was chosen over sha256 because `sha256(textsend(...))` cannot go in an index: `textsend` is
   only STABLE. Test: `long_texts` stores a 10 kB string value, a 10 kB label value and a 5 kB name. It
   passes on PostgreSQL, TimescaleDB, SQLite, DuckDB and ClickHouse.
4. **Migrations that open their own transaction** (commit fa8f200). The old migration file is left alone,
   because its checksum is recorded in the deployed databases. The unit test
   `migrations_do_not_open_transactions_of_their_own` refuses `BEGIN`/`COMMIT` in any other migration. A
   check run showed that it catches the old file.
5. **Indexes.** Migration `20261110100000_matcher_indexes.sql` adds:
   - `labels (name, description)`;
   - a hash index on `sensors (name)`;
   - on TimescaleDB, `(sensor_id, time DESC)` on the 8 hypertables.

   The B-trees on the dictionary texts were already replaced in item 3. TimescaleDB `list_series` now
   loads the labels of a page in one query. Measured on 20,000 sensors with 3 labels each, and 2 M float
   samples written one minute at a time for all the series together (PostgreSQL 18, TimescaleDB 2.31, a
   laptop):

   | Query | PostgreSQL before | PostgreSQL after | TimescaleDB before | TimescaleDB after |
   | --- | --- | --- | --- | --- |
   | Label matcher | 10.79 ms | 0.35 ms | 5.50 ms | 0.13 ms |
   | Name matcher | 4.53 ms | 1.38 ms | 0.99 ms | 0.34 ms |
   | One series (100 samples) | 19.6 ms | 22.0 ms | 22.44 ms | 0.50 ms |
   | Latest sample | 17.7 ms | 18.3 ms | 0.15 ms | 0.04 ms |
   | Write of 8,192 samples | 25.6 ms | 25.7 ms | 86.2 ms | 90.9 ms |

   On PostgreSQL the series reads are unchanged: the value tables keep their BRIN index, which does not
   help when the series interleave. A B-tree there costs a write about 40% more time and is about 1,500
   times bigger, so this is left as a decision in `ideas/postgres-btree-on-interleaved-series.md`. The
   TimescaleDB index was also created on a hypertable that holds a compressed chunk.
6. **Small items.**
   - `step` is limited to the span of the storable years (`MAX_STEP_MS`), in `validate()` and in the
     CRUD parser. Before the fix, `step=200000000y` answered 500 on PostgreSQL and made SQLite panic on a
     multiply overflow. Test: `request_panics::a_step_longer_than_the_storable_years_is_refused`.
   - If a sensor is deleted between the two statements of `latest`, the call now returns no sample
     instead of failing.
   - The batch queries are boxed futures, run two at a time in the request's own task. They are no longer
     spawned tasks, so cancelling a request stops them.

**Upgrade.** A database created and written by v0.4.2 was started with this build, on PostgreSQL and on
TimescaleDB. The two new migrations applied. The old samples, and a new 6,000-character string value, were
read back.

**Suites run.**
- Full: SQLite (354 integration tests), PostgreSQL (354) and TimescaleDB (358).
- DuckDB and ClickHouse: `regex_matchers`, `long_texts` and `request_panics`.
- BigQuery: unit tests.
- `cargo clippy --all-features --tests` is clean.
