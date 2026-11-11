# ClickHouse and BigQuery: guard the sensor key format, and remaining items

Source: review of pull request 60 on 11 November 2026 (HEAD `7d8b396`). Confirmed by reading; nothing was run
against ClickHouse or BigQuery.

## Problem

1. **Nothing stops the new binary from writing into a database with the old keys.** The series key changed
   from the XOR of the UUID halves to a BLAKE3 hash (`src/storage/common.rs:137-142`). There is no schema or
   version marker: `reject_legacy_metadata_tables` (`src/storage/clickhouse/mod.rs:208-229`) only checks the
   table engine, and BigQuery only checks `LEGACY_TABLES`. On a database that was not emptied, a write for a
   stored UUID computes the new key, finds no sensor, and registers a second `sensors` row for the same
   UUID. Reads by UUID then see only the new samples, while `list_series`, selectors and `list_metrics` show
   two series with the same UUID. Nothing errors. The changelog says the old series are "not found"; they
   are also duplicated.
2. **The collision check has an undocumented race.** The hash is now keyed by `SENSAPP_SENSOR_SALT`, so with
   a secret salt a client that chooses UUIDs cannot search for a 64-bit collision offline; with the public
   default salt it still can (about 2^32 hashes, minutes). Two instances registering two
   colliding UUIDs at the same instant both see "not existing". On ClickHouse the `ReplacingMergeTree`
   later keeps one row, which then owns both sets of samples; on BigQuery both rows stay and
   `ANY_VALUE(uuid)` (`src/storage/bigquery/reads.rs:174-181`) picks one non-deterministically. The docs
   describe the race for one UUID with two identities only.
3. **BigQuery `check_append_results` uses a lower bound, not the request count**
   (`src/storage/bigquery/publishers.rs:306-345`): `bytes_sent.div_ceil(10 MiB)`, while the client cuts at
   9 MiB. 9.5 MiB of rows is two requests with a bound of one, so a missing last response passes. The docs
   and the changelog say every request is confirmed.
4. **ClickHouse delete, residual races** (`src/storage/clickhouse/mod.rs:381-420`, `:653-672`): the verify
   loop on `sensors` can still delete a registration made during the delete; a delete of a UUID with no
   sensor row can remove an acknowledged first publish (a window of two queries); five re-registrations
   during the loop give a 500.
5. **BigQuery code no test executes**: `availability_sql`, `query_availability`, `list_metrics_sql` and its
   JSON parsing, and `check_append_results` against a real stream. One doubt from reading:
   `IFNULL(ARRAY_AGG(...), [])` relies on the untyped `[]` coercing to `ARRAY<STRING>`.
6. Small: `docs/DATAMODEL.md` says the ClickHouse Numeric average is rounded, the code truncates;
   `decimal_from_clickhouse_raw` (`clickhouse_utilities.rs:45-49`) rounds one digit at a time, which can
   double-round an aggregate over 96 bits.

## Suggested solution

1. At startup, read a few rows (`SELECT sensor_id, uuid FROM sensors LIMIT 10`) and refuse to start when
   `uuid_to_sensor_id(uuid) != sensor_id`, with a message that says to empty the database or to restore the
   salt (a changed `SENSAPP_SENSOR_SALT` now gives other keys too). Same on BigQuery.
   Correct the changelog sentence.
2. Document the residual race in `docs/DATAMODEL.md`. To close it on ClickHouse, re-read the `sensors` row
   after the insert and fail the write when another UUID holds the key (the loser then answers 409 before
   writing samples); do the same on BigQuery when the cache entry is created.
3. Count the requests exactly by replaying the client's split over the encoded row sizes, or state the
   weaker guarantee in the docs and the changelog.
4. For `sensors`, delete once and do not loop; treat a sensor row that reappears as a new registration.
   Document what remains.
5. Run the live BigQuery suite before the release (it bills the company project, so the maintainer runs
   it), and add the availability and `list_metrics` cases to it.
6. Fix the doc wording; round once from the full value.

## Tests

- Start against a database holding one row with the old key: startup fails with the message.
- Two concurrent registrations of the pinned colliding pair: at most one succeeds.

## Done when

The tests pass on ClickHouse, the guard exists on both backends, and the BigQuery live run is recorded here.

## Outcome (11 November 2026)

Items 1, 2 (docs), 3 (docs) and 6 are done. Items 4 and 5 are not.

1. **Guard**: `storage::common::check_sensor_keys` compares the key of ten stored sensors with the key this
   version and `SENSAPP_SENSOR_SALT` give them, and the message says to empty the database or to set the salt
   back. ClickHouse runs it at the end of `create_or_migrate` (`SELECT uuid, sensor_id FROM sensors LIMIT 10`),
   BigQuery too (the same query on the dataset, `INT64` read as the same 64 bits). Tests: a unit test (empty,
   good, the XOR key, another salt) and `test_clickhouse_refuses_a_database_with_the_old_sensor_keys`, which
   inserts a row with the XOR key into a throwaway database and expects the startup error (passes on the local
   ClickHouse 26.8). **The BigQuery query was compiled and not run**: the maintainer's live run has to cover it.
   The changelog sentence now says the new version refuses to start, instead of "does not find".
2. **Residual race of the collision check**: documented in `docs/DATAMODEL.md` (ClickHouse keeps one row that owns
   both series, BigQuery keeps both and a read picks one; needs colliding UUIDs, so a public default salt).
   Not closed in code; the one-UUID-two-identities race was measured on ClickHouse: both writers succeed
   (`ideas/clickhouse-concurrent-registration.md`).
3. **`check_append_results`**: the code was left, the docs and the guarantee now say it is a lower bound (9.5 MiB
   is two requests and a bound of one).
4. **ClickHouse delete, residual races**: not done.
5. **BigQuery live run and untested code**: not done, billed to the company project: the maintainer runs it
   before the release. The cases to add: availability, `list_metrics`, the key guard.
6. **Small**: the ClickHouse average of Numeric values truncates (checked live: 2/3 is `0.66666666`), the doc
   said rounded; fixed. `decimal_from_clickhouse_raw` rounded one digit at a time (`…49` over 96 bits became
   `…5`, then rounded up again, and lost a digit more): the new test failed with
   `79228162514264337593543.95034` for `…950335`; it now drops the digits it must in one rounding from the whole
   value.

Ran: SQLite, PostgreSQL, ClickHouse in full (see the commit), clippy with and without all features.
