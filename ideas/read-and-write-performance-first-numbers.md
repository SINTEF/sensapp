# First numbers: ingestion and reads on SQLite, PostgreSQL and TimescaleDB

A quick, single-run comparison made on 4 October 2026 while fixing the write timeout. It is **not** the
proper benchmark that is planned: one series, one client, one laptop, a Docker VM (Colima) between
SensApp and the two PostgreSQL-family databases, one run per cell (the small numbers move by a factor of
two between runs). Use it to see orders of magnitude and to choose what the real benchmark must cover.

Setup: release build of `write-timeouts` (`--features timescaledb`), the Zeblab series
`zeb_320_001_338_320_001_Hpu001_AlmFl` (1,331,266 float samples, 2024-02-13 to 2026-10-01, about one per
minute, 0 or 1), written in **one** Arrow request with the SDK's default timeouts, `ANALYZE` on both
PostgreSQL databases afterwards. PostgreSQL 16.6; TimescaleDB 2.17.2 (the compose file pins 2.30.2).
Reads are `format=arrow`, 1 warm-up and 9 timed runs, median shown.

## Writing the whole history (one request)

| | SQLite | PostgreSQL | TimescaleDB |
|---|---|---|---|
| time | 5.1 s | 17.6 s | 51.6 s |
| samples/s | 260,000 | 76,000 | 26,000 |
| size on disk | 49 MB | 76 MB | 136 MB (not compressed) |

## Reading (median ms)

| query | SQLite | PostgreSQL | TimescaleDB |
|---|---|---|---|
| raw, one week (~20k samples) | 78 | 113 | **19** |
| raw, `limit=100000` | 95 | 143 | **81** |
| `step=1h avg`, one week (168 buckets) | 65 | 105 | **6.6** |
| `step=1m avg`, one month (~43k buckets) | 131 | 112 | **70** |
| `step=1d avg`, whole history (875 buckets) | 478 | 568 | **171** |
| `step=1h avg`, whole history (20k buckets) | 441 | 588 | **230** |
| `step=1h max`, whole history | 445 | 424 | **219** |
| `step=1d last`, whole history | 984 | 562 | **270** |
| latest sample (`/last`) | 54 | 66 | **9.8** |
| availability, `step=1d`, 2.6 years | **99** | 269 | 201 |

## Reading the numbers

- **TimescaleDB reads fastest on nearly everything**: 4 to 16 times faster for a window of one week (the
  chunks that do not overlap the window are not touched, and each chunk has its own index), 2 to 3.5 times
  for aggregations over the whole history, only 1.2 to 1.8 times for 100,000 raw samples, and no gain for
  the availability query. It pays for it when writing (a chunk per week of history has to be
  created: 3 times slower than PostgreSQL for a backfill) and on disk (1.8 times PostgreSQL, before
  compression, which was not tried).
- **The SQLite and PostgreSQL numbers say as much about our schema and queries as about the engines**:
  SQLite scans the whole series for any window because of the `? IS NULL OR` form of its queries
  (`ideas/sqlite-windowed-reads-use-the-index.md`: 133 ms for one hour of a 1.33 M series that should take
  microseconds), and the PostgreSQL value tables only have a BRIN index, which is good for windows and
  bad for `limit=1` and `/last`. Fix those before reading the SQLite and PostgreSQL columns as a verdict.
- Whole-history aggregations touch every row, so they scale with the number of samples: 0.2 to 1 s for 1.3 M
  samples. A series of 100 M samples will not answer in 30 s without pre-aggregation (TimescaleDB
  continuous aggregates, not tried).

## For the proper benchmark

Several series and several sizes (1 M to 1 G samples), concurrent clients, data arriving in time order as
well as a backfill, compression on and off for TimescaleDB, the version of the compose file, a repeat count
with a spread, the cost of `format=arrow` against `csv`, ClickHouse and DuckDB, and the write side with
`SENSAPP_DEDUPLICATE_ON_INGEST` on.
