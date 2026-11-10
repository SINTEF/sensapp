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

## Outcome

(To fill in.)
