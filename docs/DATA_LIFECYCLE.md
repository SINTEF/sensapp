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
performance. It is why correcting means deleting first. Removing duplicates is a maintenance task,
see [Duplicate samples](#duplicate-samples).

### Several SensApp instances

Each instance caches the internal id of a sensor for up to 2 minutes. The instance that runs
`DELETE /series/{uuid}` forgets it immediately. On PostgreSQL and TimescaleDB, another instance
that still holds the old id gets a foreign-key error on its next publish, forgets the id, and
retries once, which recreates the series. This also covers series removed by hand with SQL.
No action is needed.

## Authorization

With [JWT authentication](JWT_AUTH.md) enabled, deleting requires the `delete` scope.
The default `read write` scope does **not** include it, so a leaked edge-device token cannot erase history.

```bash
sensapp generate-token cleanup --scope delete --duration 900
sensapp generate-token admin --scope readwrite,delete
```

A `delete` token is still limited by its `sensors` allow list. Series it cannot access are reported
as `404`, like on the read endpoints. The `delete` scope does not allow reading: combine it with `read`
to be able to list series and check what was removed.

Without JWT authentication, like every other endpoint, deletes are open.

Each deletion is logged at `INFO` level with the series UUID, the token subject, and the number of samples.

## Duplicate samples

SensApp does not reject a sample that already exists: a unique index on every insert would cost ingestion speed. A retried write (the Python SDK retries timeouts), a client that sends a sample twice or a crash in the middle of a request can therefore leave duplicates.

`POST /api/v1/admin/vacuum` removes them, then cleans up the database as the backend supports. It needs the `delete` scope, on a token without a sensor allow list, and answers with the number of samples it removed:

```json
{"status": "ok", "duplicates_removed": 12}
```

Only exact duplicates go: the same series, the same timestamp and the same value (the same coordinates for a location), and the first one written is kept. Two different values at the same timestamp are both kept, there is no rule to choose one. Run it again and it removes nothing. `duplicates_removed` is `null` on a backend that cannot remove duplicates (see below); the count is exact unless writes happen at the same time. On a large database the operation scans every value table: it can take longer than the request timeout (`504`), and the database carries on.

## Backend support

| Backend | Delete samples | Delete series | Remove duplicates | Notes |
|---|---|---|---|---|
| PostgreSQL | yes | yes | yes | Space is reused by autovacuum, run `POST /api/v1/admin/vacuum` to compact (and to remove duplicate samples) |
| SQLite | yes | yes | yes | `VACUUM` shrinks the file |
| TimescaleDB | yes | yes | yes | Tested on uncompressed chunks. TimescaleDB supports `DELETE` on compressed chunks too, but it has to decompress them first, which is slower |
| DuckDB | yes | yes | yes | Exact duplicates are found with `rowid` |
| ClickHouse | yes | yes | yes | Lightweight `DELETE`: rows disappear from queries at once and are physically removed by later merges. The sample count is taken just before the delete |
| BigQuery | no | no | no | Returns `501 Not Implemented` |
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
