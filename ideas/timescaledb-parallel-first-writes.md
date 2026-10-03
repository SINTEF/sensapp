# TimescaleDB: keep the parallelism of bulk loads of new series

`done/timescaledb-first-write-deadlock.md` fixed a deadlock by making a batch that registers new sensors lock
the hypertables it writes to (`SHARE UPDATE EXCLUSIVE`, until its commit) before inserting the sensors. Cost,
measured: four parallel writers of 3 000 new series each take 11.6 s instead of 4.8 s, the batches take turns
per value type. Only batches that create series pay, one time per series; a normal write is unchanged.

It matters for a migration with several workers, or Prometheus shards sending their first samples to an empty
database. Options, none tried:

- Lock only when a chunk may be missing: the batch's time range (min and max of its samples) is covered by
  existing chunks of the table (`show_chunks`, both hash partitions of every week). Most batches after the first
  ones would not lock. The catch: it reads the catalog on every registering batch and hard-codes the layout of the
  hypertables (7 days, 2 hash partitions).
- Optimistic: do not lock, and lock on the retry after a `deadlock_detected`. Costs the 1 s `deadlock_timeout` on
  every collision, which is the common case on an empty database.
- Create the chunks before the sensors of the batch exist: not possible, the hash partition depends on the sensor
  id and the foreign key needs the row.
