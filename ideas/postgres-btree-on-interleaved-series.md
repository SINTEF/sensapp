# PostgreSQL: a B-tree for the series written together

The PostgreSQL value tables only have a BRIN index on `(sensor_id, timestamp_us)` (see
`done/postgresql-windows-and-buckets.md` and `done/ingestion-deduplication.md`). BRIN works when the rows of a series sit together
on disk, which holds when a series is written in large batches. When many series write a few samples at
the same time, as a fleet of sensors reporting every minute does, the rows of all the series interleave
and a BRIN range holds every series.

Measured on 10 November 2026 (PostgreSQL 18, a laptop):

- 20,000 series, 2 M float samples, written one minute at a time for all the series together.
- Reading one series (100 samples) took 20 ms with BRIN, and 1.1 ms with a B-tree on
  `(sensor_id, timestamp_us)`.
- Reading the latest sample of a series took 18 ms with BRIN, and 0.12 ms with the B-tree.
- Writing 8,192 samples took 26 ms with BRIN, and 36 ms with the B-tree.
- The B-tree was 61 MB for a 108 MB table. The BRIN index was 40 kB.

To decide: whether SensApp targets the interleaved case on PostgreSQL (TimescaleDB is the answer there,
and now has `(sensor_id, time DESC)`), or whether PostgreSQL should get the B-tree, or both indexes.
