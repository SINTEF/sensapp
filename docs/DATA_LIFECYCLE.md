# Data Lifecycle

How to remove unwanted data and correct mistakes in SensApp.

SensApp follows what the time-series databases do: data is **appended**, and the supported
remedies for mistakes are **deleting** a series or a time range. There is no API to update a sample
in place, and no retention setting yet.

## Deleting

Both endpoints are documented in the OpenAPI reference (`/docs`, tag `Admin`).

### Delete a time range of a series

```bash
curl -X DELETE \
  "http://localhost:3000/series/$SERIES_UUID/samples?start=2024-01-01T00:00:00Z&end=2024-01-01T06:00:00Z" \
  -H "Authorization: Bearer $TOKEN"
```

```json
{ "series_uuid": "0198…", "deleted_samples": 42 }
```

- `start` and `end` are both **required** and both **inclusive**. Requiring both means a forgotten
  parameter cannot wipe a whole series. `start` equal to `end` deletes the samples at one exact timestamp.
- The series itself, and its labels, are kept.
- `deleted_samples: 0` means the window matched nothing. Check the timestamps and their precision:
  SensApp stores microseconds.
- `404` when the series does not exist, `400` for an invalid UUID, missing bounds, or `start > end`.

### Delete a whole series

```bash
curl -X DELETE "http://localhost:3000/series/$SERIES_UUID" -H "Authorization: Bearer $TOKEN"
```

Removes the samples, the labels and the series itself. `204` on success, `404` when it does not exist.

Both operations are permanent. Take a backup, or export the range first (`GET /series/{uuid}?format=csv&start=…&end=…`), if you may need it back.

## Correcting data

Do not expect an update API. The usual recipe, the same one used with Prometheus, is to
**delete the wrong range, then publish the corrected samples**:

1. Delete the range (`DELETE /series/{uuid}/samples?start=…&end=…`).
2. Publish the fixed samples again, with the same sensor name, labels and unit.

For CSV, InfluxDB Line Protocol, Prometheus Remote Write, SenML, and Arrow, the series UUID is derived
from the sensor name, type, unit and labels, so the corrected data lands in the same series, including
after a whole-series delete. The derivation is keyed by the configured `sensor_salt`: changing the salt
changes every derived UUID.

Two importers also accept an explicit UUID, which then identifies the series:

- **SenML**: a base name (`bn`) that is a UUID, as written by the SenML export. The name is read
  from the `_name` field when present. SenML has no labels, so the unit is the only extra input to the
  derived UUID: publishing the same name with and without a unit creates two series.
- **Arrow**: the `sensapp.sensor.uuid` schema metadata or a `sensor_id` column. Without a UUID, the
  sensor name (`sensapp.sensor.name` metadata or `sensor_name` column) is required. A payload with
  neither is rejected with `400`.

### Publishing the same timestamp twice keeps both samples

Writes are plain appends. Publishing a sample at a `(series, timestamp)` that already exists stores a second
sample, it does not replace the first. This is by design, because enforcing uniqueness costs ingestion
performance (it can be turned on for the identical samples, see
[Duplicate samples](#duplicate-samples), but two different values at one timestamp are still both kept). It
is why correcting means deleting first. Removing duplicates is a maintenance task, or an option at ingestion,
see [Duplicate samples](#duplicate-samples).

### Several SensApp instances

Each instance caches the internal id of a sensor for up to 2 minutes. The instance that runs
`DELETE /series/{uuid}` forgets it immediately. On PostgreSQL and TimescaleDB, another instance
that still holds the old id gets a foreign-key error on its next publish, forgets the id, and
retries once, which recreates the series. This also covers series removed by hand with SQL.
No action is needed.

## Authorization

With [authentication](JWT_AUTH.md) on, deleting requires the `delete` scope.
The default `read write` scope does **not** include it, so a leaked edge-device token cannot erase history.

```bash
sensapp generate-token cleanup --scope delete --duration 900
sensapp generate-token admin --scope readwrite,delete
```

A `delete` token is still limited by its `sensors` allow list. Series it cannot access are reported
as `404`, like on the read endpoints. The `delete` scope does not allow reading: combine it with `read`
to be able to list series and check what was removed.

With authentication disabled (`SENSAPP_AUTH_DISABLED`), like every other endpoint, deletes are open.

Each deletion is logged at `INFO` level with the series UUID, the token subject, and the number of samples.

## Duplicate samples

By default SensApp does not reject a sample that already exists: a unique index on every insert would cost ingestion speed. A retried write (the Python SDK retries timeouts), a client that sends a sample twice or a crash in the middle of a request can therefore leave duplicates.

**Nothing removes them automatically.** There is no scheduler and no vacuum after a write: a duplicate stays in the database until someone runs the vacuum operation, and until then every read sees it. `count` and `avg` count a duplicated sample twice, and the values of a series list it twice. Run the vacuum after an incident that may have produced duplicates (a retried write that had timed out, a crash during a large import), or from your own scheduler (a cron job calling the endpoint) if you want it regularly.

`POST /api/v1/admin/vacuum` removes them, then cleans up the database as the backend supports. It needs the `delete` scope, on a token without a sensor allow list, and answers with the number of samples it removed:

```json
{"status": "ok", "duplicates_removed": 12}
```

Only exact duplicates go: the same series, the same timestamp and the same value (the same coordinates for a location), and the first one written is kept. Two different values at the same timestamp are both kept, there is no rule to choose one. Run it again and it removes nothing. `duplicates_removed` is `null` on a backend that cannot remove duplicates (see below); the count is exact on the SQL backends (it is the number of rows their `DELETE` removed, whatever else is written). On ClickHouse a merge does not say how many rows it dropped, so SensApp counts the rows of each table before and after: it is exact on a database that nothing writes to, and an estimate otherwise, since every row inserted during the run lowers it (to 0 at the lowest) and every row deleted raises it. On a large database the operation scans every value table and is slow. It has a timeout of its own, `SENSAPP_HTTP_MAINTENANCE_TIMEOUT_SECONDS` (one hour by default, the other requests have 30 seconds). If it is exceeded the answer is a `504` and the database carries on. Do not start a second vacuum while one is running: they would compete for the same rows.

### Not writing them: deduplication at ingestion

`SENSAPP_DEDUPLICATE_ON_INGEST=true` (or `deduplicate_on_ingest = true` in the settings file) makes the write itself leave out the samples that are stored already, and write the repeated samples of one request once. The rule is the vacuum's: a duplicate has the same series, timestamp and value (the same coordinates for a location), two different values at one timestamp are both kept, and nothing is rejected: the request succeeds. It works for requests that overlap or repeat each other, not only for retries. It does not remove the duplicates that were written before it was turned on: run the vacuum for those. **The server refuses to start** when the backend cannot do it.

| Backend | At ingestion | Several writers or instances |
|---|---|---|
| PostgreSQL | yes | exact: a lock per series (1 024 buckets) held until the end of the transaction. Writers of the same series take turns, and so do two large batches, which hold most of the buckets |
| TimescaleDB | yes | exact, same locks |
| SQLite | yes | exact, a single writer |
| DuckDB | yes | exact, a single process |
| ClickHouse | **no** | not possible: there is no transaction and no unique key. Use the vacuum, which merges the duplicates away |
| BigQuery, RRDCached | no | |

How it works, and what it costs. The statement that writes the samples of a batch leaves out the ones that exist already, looking only at the stored samples inside the time window of the batch (the oldest to the newest timestamp of the request). A request that spans a long time is therefore more expensive than a request about the last minute. Measured on a table of one million rows, release build, for a request of 20 000 new samples: no visible difference on SQLite, DuckDB and TimescaleDB; PostgreSQL 0.14 s against 0.14 s. A small write (one series, 10 samples) costs about 1.5 ms more on DuckDB and TimescaleDB and 1 to 3 ms more on PostgreSQL. Writing again samples that are all stored is as fast or faster, since nothing is inserted.

**PostgreSQL: keep the BRIN indexes summarized.** The probe reads the window through the BRIN index of the value table, and a BRIN index returns every range it has not summarized yet in full. A table that has just been bulk loaded and not vacuumed yet, or the newest part of a live table that autovacuum has not reached, is read entirely by every write: a small write took 64 ms instead of 3.5 ms on a table of one million rows, and the cost grows with the unsummarized part (by default autovacuum visits an insert-only table every 20% of growth). The migrations set `autosummarize` on the indexes, which asks autovacuum to summarize a range as soon as it is complete; make sure autovacuum runs, and after a large import run `VACUUM ANALYZE` (the vacuum endpoint runs `VACUUM`, which summarizes too). TimescaleDB does not use BRIN indexes and does not have this condition. With the default autovacuum settings a table that was loaded and left alone for 90 seconds was back to 3.9 ms per small write by itself. A large import is slower with the switch on, because each request probes the table that the previous ones filled and nothing has summarized yet (1 million samples in requests of 100 000: 10.5 s instead of 6 s on PostgreSQL): turn it off for a one-time import of data that is known to be clean. Reads of the time windows of a table benefit from the same summaries.

Two requests that write the same new sample at the same moment, on different instances, store it once on the SQL backends: the second waits for the first to commit. This is also what makes the retry of a request that timed out safe while the first one is still running.

## Backend support

| Backend | Delete samples | Delete series | Remove duplicates | Notes |
|---|---|---|---|---|
| PostgreSQL | yes | yes | yes | Space is reused by autovacuum, run `POST /api/v1/admin/vacuum` to compact (and to remove duplicate samples) |
| SQLite | yes | yes | yes | `VACUUM` shrinks the file |
| TimescaleDB | yes | yes | yes | Works on compressed chunks. `DELETE` on a compressed chunk decompresses what it touches first, which is slower. The vacuum decompresses only the chunks that hold duplicates, removes them and compresses those chunks again |
| DuckDB | yes | yes | yes | Exact duplicates are found with `rowid` |
| ClickHouse | yes | yes | yes | Lightweight `DELETE`: rows disappear from queries at once and are physically removed by later merges. The sample count is taken just before the delete |
| BigQuery | yes | yes | no | `DELETE` statements, which bill the bytes they scan. BigQuery runs 2 mutating statements at a time per table and queues 20: deleting many series at once fails. Duplicates cannot be removed: `501 Not Implemented` |
| RRDCached | no | no | no | Returns `501 Not Implemented` |

## How other systems do it

| System | Retention | Delete | Correcting a value |
|---|---|---|---|
| Prometheus | Global time/size limit, drops whole blocks | Admin API, disabled by default, by series selector and time range | None: delete, then backfill |
| VictoriaMetrics, Mimir | Global or per tenant | Same style of delete API, documented as rare and costly | None |
| InfluxDB 1.x/2.x | Per database or bucket | Delete by predicate, start/stop required in 2.x | Writing the same series and timestamp overwrites |
| TimescaleDB | `add_retention_policy` (drops chunks) | SQL `DELETE` | SQL `UPDATE` |
| ClickHouse | Table `TTL`, `DROP PARTITION` | Lightweight `DELETE` | Mutations or `ReplacingMergeTree` |

SensApp keeps the common denominator: delete by series and time range behind an explicit permission.
Retention is described in `ideas/data-retention.md`.
