# Sensor key: BLAKE3 instead of XOR

Source: discussion of 10 November 2026, after `clickhouse-numeric-and-uuid-identity.md` added the 64-bit key
collision check (`StorageError::SensorKeyCollision`, `409 Conflict`).

**Breaking change** for the ClickHouse and BigQuery on-disk format: every stored `sensor_id` changes.

## Problem

ClickHouse and BigQuery key a series by `uuid_to_sensor_id` (`src/storage/common.rs`): the XOR of the two
64-bit halves of the UUID. That works for the UUIDs SensApp derives, which are hashes. It does not work
for UUIDs chosen by SenML and Arrow writers, because XOR keeps their structure and colliding keys come
out of ordinary schemes:

- two numbers in the two halves, such as (device, channel): `(1, 2)` and `(2, 1)` get the same key;
- every UUID whose two halves are equal gets key `0` (nil UUID, `ffff…`, `(n, n)` counters);
- hand-written patterns (`0…01-0…0` against `0…0-0…01`).

The collision check refuses these with a `409`, so the data stays correct, but users with a structured
scheme get conflicts they cannot understand.

## Solution

Key a series by the first 8 bytes of the BLAKE3 hash of the 16 UUID bytes (`uuid.as_bytes()`), read as a
little-endian `u64`, the same way `unit_name_to_id` builds unit ids. `blake3` is already a dependency and
its output is fixed by its specification. Do not use `DefaultHasher`, `ahash` or similar: their output may
change between releases.

**Keep the collision check.** With any 64-bit key, a writer can find a collision by brute force in about
2³² tries, and at billions of sensors a random collision becomes possible (about n²/2⁶⁵). The hash
removes the collisions caused by structure, not all of them.

The fold is done only in Rust: no SQL query or migration computes it (the key is stored in the `sensors`
table next to the UUID). The change is one function.

## Steps

1. `uuid_to_sensor_id` in `src/storage/common.rs`: BLAKE3, and rewrite its doc comment.
2. Pinned tests: `sensor_ids_are_pinned` (`src/storage/common.rs`) and
   `sensor_ids_are_the_clickhouse_ids_as_signed_integers` (`src/storage/bigquery/publishers.rs`). New pinned
   values, including the cases that collide under XOR (`(a, b)` and `(b, a)`, equal halves), which must
   now give different keys.
3. A generic integration test: publishing two SenML series with UUIDs `(a, b)` and `(b, a)` succeeds on
   every backend, and each one reads back its own samples. Today it gives a `409` on ClickHouse and
   BigQuery.
4. Keep a test that still triggers `SensorKeyCollision` on a forced collision (two UUIDs with the same
   key, found by brute force once and pinned in the test, or with the key function injected in a unit
   test). Decide which one.
5. Docs: `docs/DATAMODEL.md` (the paragraph on 64-bit keys, drop the `a-b` / `b-a` example) and
   `docs/BIGQUERY.md` ("the UUID folded to 64 bits").
6. Existing data. Decide: write in the changelog that ClickHouse and BigQuery databases must be emptied
   and reloaded (pre-production, no migration), or write a one-off re-keying tool (read `sensors.uuid`,
   compute the new id, rewrite `sensors`, `labels` and the sample tables). ClickHouse cannot compute
   the new key in SQL, so a re-key has to go through Rust.
7. Cost: a BLAKE3 hash of 16 bytes takes some tens of nanoseconds per distinct sensor in a batch, next to
   a database round trip. No measurement needed unless a write benchmark moves.

## Outcome (11 November 2026)

1. `uuid_to_sensor_id` is the first 8 bytes of `blake3::hash(uuid.as_bytes())`, little-endian, with its doc comment
   rewritten. Nothing else computes the key.
2. Pins: `sensor_ids_are_pinned` has six UUIDs, among them the four that XOR mapped to `0` and `u64::MAX`, and
   `sensor_ids_are_the_clickhouse_ids_as_signed_integers` one positive and one negative key. The values come from the
   `blake3` crate; `unit_ids_are_pinned` already ties that crate to the published BLAKE3 test vector.
3. `structured_uuids_are_separate_series` (`tests/integration/sensor_identity.rs`, every backend but RRDcached):
   `(a, b)` against a stored `(b, a)`, and `(a, a)` with `(b, b)` in one batch. Through storage, like the other
   identity tests, rather than SenML over HTTP: the key is a storage matter. It failed on ClickHouse with the XOR
   key (run with the old function put back), and passes on SQLite, PostgreSQL, DuckDB, TimescaleDB and ClickHouse.
4. Decided: a pinned collision. A Pollard rho search with distinguished points over the UUIDs `(0, x)` found
   `x = 0xb91ca66043fde277` and `0x6cda2deea4740435`, both with key `0xb4601f96ba047f9a`, after 7.3 billion
   hashes, in 2 minutes on 8 cores (the program was a temporary example, removed). The unit test
   `the_pinned_collision_collides_and_structures_do_not` checks the pair with the real function, and
   `two_uuids_with_the_same_storage_key_never_share_a_series` uses it: refused in one batch and against a stored
   sensor on ClickHouse (and BigQuery), stored twice elsewhere, and the test asserts which backends refuse. An
   injected key function would have tested a copy of the registration, not the registration.
5. Docs: `docs/DATAMODEL.md` and `docs/BIGQUERY.md`.
6. Decided: no re-keying tool. SensApp is pre-production; the CHANGELOG says to empty and reload a ClickHouse or
   BigQuery database.
7. Not measured: one hash of 16 bytes per distinct sensor of a batch.

Suites: `cargo clippy` both ways; `cargo test --lib` three times; ClickHouse in full (341 + 389 + 2); the identity
tests on SQLite, PostgreSQL, DuckDB and TimescaleDB; BigQuery unit tests of the key. BigQuery not run live. With
`--no-default-features --features bigquery`, three lib tests that build a SQLite storage fail with "Unsupported
storage type": they need the SQLite feature, unrelated to this change.

Found on the way: the configuration tests set refused values in the environment, which the other tests that
load the configuration read since `load()` checks them (task 20). Fixed in its own commit.
