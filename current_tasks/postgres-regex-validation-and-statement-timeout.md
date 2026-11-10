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
