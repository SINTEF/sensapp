# TimescaleDB: the forced custom plans may be what makes its writes slow

## Observation (4 October 2026)

The TimescaleDB backend sets `plan_cache_mode = force_custom_plan` on every connection
(`src/storage/timescaledb/mod.rs`, `after_connect`), because TimescaleDB 2.17 (fixed by 2.30) does not
invalidate a generic plan when a chunk is compressed or receives a late write, and a long-lived connection
then returns wrong results.

Loading the 1.33 M-sample series in one request took 51.6 to 58.4 s, against 17.6 to 19.7 s on PostgreSQL. I
first put that on the creation of a chunk per week of history. On the PostgreSQL backend the same setting
made the load 2.7 times slower (50.7 s against 18.6 s), because the bulk `INSERT .. SELECT unnest($1, $2, $3)`
is planned again for every batch of 8,192 samples, with the arrays as constants (about 200 ms each), so
`PostgresStorage::publish_once` now runs `SET LOCAL plan_cache_mode = auto` first.

Throwaway experiment, not committed: the same `SET LOCAL plan_cache_mode = auto` at the start of
`TimescaleStorage::publish_once`. **The load took 23.5 s** (against 51.6 to 58.4 s), with exactly 1,331,266 rows
and 1,331,266 distinct timestamps. So the chunks cost about 4 s, and the custom plans about 30 s.

## To do

- Apply it, and run the TimescaleDB suite (`--no-default-features --features timescaledb`), in particular
  `timescale_compressed` and `timescale_deadlock`, which are the tests of the problems the setting hides.
  The wrong results of 2.17 were seen on reads; check that a generic plan of the **insert** is safe when a
  chunk is compressed, or receives a late write, on 2.17 and on the 2.30 of the compose file. The reads keep
  their custom plans.
- Measure the load of a stream of small requests (Prometheus sends a few hundred samples at a time), where
  planning is a larger part of the cost than for a 1.3 M-sample backfill.
