# RRDCached Storage Backend

SensApp can store its numeric series in [RRDtool](https://oss.oetiker.ch/rrdtool/) files, through the
[RRDCached](https://oss.oetiker.ch/rrdtool/doc/rrdcached.en.html) daemon. RRDtool is a mature system made for
monitoring: a file never grows, old data is consolidated into coarser rows, and the daemon batches the writes.
It fits the story *"keep Prometheus' data in RRDtool"*, and anything that sends a number every few seconds or
minutes.

This backend is **good enough, not complete**: it stores and returns numbers, and nothing else. Read
[What is different](#what-is-different) before choosing it. It has no promise beyond that (see
[BACKENDS.md](BACKENDS.md)), and is the cheapest backend to run next to a monitoring stack that already uses
RRDtool files.

## Quick start

Start the daemon, then give SensApp its address:

```bash
rrdcached -g -B -O -b /var/lib/rrdcached/db -j /var/lib/rrdcached/journal -l 127.0.0.1:42217
```

```bash
SENSAPP_STORAGE_CONNECTION_STRING="rrdcached://127.0.0.1:42217?preset=hoarder" sensapp
```

or in `settings.toml`:

```toml
storage_connection_string = "rrdcached://127.0.0.1:42217?preset=hoarder"
```

The build must include the feature (`--features rrdcached`; the container image has it).

### Connection string

| Form | Meaning |
|---|---|
| `rrdcached://host:port` (or `rrdcached+tcp://`) | TCP |
| `rrdcached+unix:///path/to/rrdcached.sock` | Unix socket |

| Parameter | Default | |
|---|---|---|
| `preset` | `hoarder` | The archives of the files that SensApp creates: `hoarder` or `munin`, see [Presets](#presets) |
| `heartbeat` | `3600` | Seconds. A gap between two samples longer than this is unknown, see [Gaps](#gaps) |

Anything else is refused at startup, as is a daemon that cannot be reached. The parameters only apply to the files
SensApp creates: an existing file keeps its own.

### The daemon

- **`-O` is required when you have several SensApp instances**, and recommended always. The daemon replaces a file
  that it is asked to create, with all its history, unless it was started with `-O`. SensApp asks whether the file
  exists before creating it, and creates one file at a time inside a process, but two instances that see a new series
  at the same moment can only be told apart by the daemon. With `-O` the second `CREATE` is refused and SensApp
  carries on.
- `-j <dir>` keeps a journal: the updates that were not written to the files yet are replayed after a crash.
- `-B` confines the daemon to its base directory (`-b`). Give SensApp a directory of its own: it only lists and reads
  the files named `<uuid>.rrd` and ignores the others, but it does not make a difference for the disk space.
- `-w` (seconds between two writes of a file, 300 by default) and `-f` are the daemon's cache settings. A longer `-w`
  means fewer disk writes. SensApp never asks for a flush: a read makes the daemon write what it needs.

The image used by the tests (`docker/rrdcached/Dockerfile`) is a good starting point: it builds the latest upstream
`rrdcached` (1.11, Debian's package is 1.7.2), runs it as a non-root user, keeps the files in `/data/db` and the journal
in `/data/journal`, and starts it with `-O`. It is a copy of the image of
[rrdcached-client](https://github.com/SINTEF/rrdcached-client).

## What is different

| | RRDCached | The SQL backends and ClickHouse |
|---|---|---|
| What is stored | One number per sample, in a file per series named after its UUID | Typed values with names, units and labels |
| Value types | Integers, floats, decimals and booleans, **all read back as floats** (booleans as 0 and 1). Strings, locations, JSON and blobs are **not stored**: they are left out of a write, with a warning in the log, and the rest of the request is stored | All |
| Sensor name, labels, unit | **Lost.** A series is its UUID: the name returned is the UUID, there are no labels and no unit | Kept |
| What a read returns | The **consolidated rows** of the archives of the file, on a regular grid, not the samples that were written | The samples |
| Time precision | Whole seconds, in rows of 10 seconds or more | Microseconds |
| Duplicate and out-of-order samples | One per second, the last one wins. A sample **not after the last update** of its series cannot be stored (see [Time](#time)) | Stored (and removed by the vacuum) |
| Retention | Fixed by the preset: old rows are consolidated, then overwritten. **A file never grows** | Unbounded, deleted by hand |
| Delete a series or its samples | No: `501 Not Implemented` | Yes |
| Remove duplicates, deduplicate at ingestion | No (the server refuses to start with `SENSAPP_DEDUPLICATE_ON_INGEST=true`) | Most |
| Aggregations (`step`) | By SensApp, on the rows that were read (they are already consolidated) | In the database |
| Several instances | Safe: they share the daemon (see [the daemon](#the-daemon)) | |
| Arrow import and export, search by name or label | Not useful: there is nothing to match | Yes |

The generic integration tests, which assume names, labels, types and deletion, do not apply: the backend has a test
module of its own (see [Tests](#tests)).

### What this means for Prometheus

- **Remote write works**: a series is created on its first sample, whatever its labels.
- **Remote read cannot find a series by its name or labels**, since they are not stored. A series is selected with
  `{__name__="<uuid>"}`, where the UUID is the one SensApp derives from the name, the type, the unit and the labels of
  the series: the same on every instance with the same `SENSAPP_SENSOR_SALT`. `GET /series` lists the UUIDs.
- A selector with a label (`{job="x"}`) matches nothing, and a negated one (`{job!="x"}`) matches every series.
  Selectors on `__name__` with a regular expression match the UUID text. Regular expressions are anchored, as in
  Prometheus.

If you need names and labels, use another backend. A sidecar that stores the metadata is the main idea that is not
done, see `ideas/rrdcached-follow-ups.md`.

## How the data is stored

Each series has one file, `<uuid>.rrd`, created when a series is first written, with a base step of 10 seconds and
one gauge data source named `sensapp`.

### Presets

The preset chooses the round robin archives of new files. A row is the **average** of the 10-second steps it covers.

| Preset | Row | Kept for | Size of a file |
|---|---|---|---|
| `hoarder` | 10 s | 1 day | 196 KiB (24 938 rows) |
| | 1 min | 2 days | |
| | 10 min | 7 days | |
| | 1 h | 1 year | |
| | 1 day | 10 years | |
| `munin` | 5 min | 50 hours | 24 KiB (2 872 rows) |
| | 30 min | 14.6 days | |
| | 2 h | 64.6 days | |
| | 1 day | 797 days | |

A thousand series take 196 MiB with `hoarder`, forever. The `munin` preset has no row finer than 5 minutes: it is
meant for sensors that report that often.

An archive row is unknown (`NaN`, not returned) when more than half of the steps it consolidates are unknown (the
*xfiles factor*, 0.5). A series that has just started has an unknown first row in the `munin` preset and in the
coarse archives of both presets until half of the row is covered.

### Gaps

A gauge sample is the value of the whole interval since the previous sample of the series, so the rows between two
samples have the value of the second one. If the gap is longer than the **heartbeat**, the whole gap is unknown
instead. The default is one hour. A series that reports less often than that needs a longer `heartbeat` in the
connection string; a monitoring series that should show a hole when it stops for ten minutes needs a shorter one.

**The heartbeat is stored in the file.** Files created by an older version of SensApp have a heartbeat of 20 seconds,
which turns samples that are more than 20 seconds apart into unknown values. Change it with `rrdtool` while the
daemon is stopped, or flushed and not writing:

```bash
rrdtool tune /var/lib/rrdcached/db/<uuid>.rrd --heartbeat sensapp:3600
```

### Time

- A row is stamped with the **end** of the interval it covers: a sample at `12:00:03` is in the row `12:00:10`. For
  a sample at a multiple of 10 seconds, the row and the sample have the same time.
- The sample times are floored to the second, and there is one value per second per series: if a request has several
  samples in the same second, the last one is kept.
- RRDtool only takes updates **after the last one** of a file, and nothing before the start of the file, which is
  10 seconds before the first sample ever written. A sample that is not after the last update of its series is
  **ignored, with a warning in the log**, the write does not fail. This is also what makes retrying a request safe
  (the same samples are written twice, once). Consequences: data cannot be backfilled behind a series that already
  has newer data, and two instances that write the same series at the same time can only both keep the samples that
  are in order.
- Times before 1970 are not stored.
- A value that is `NaN` (a Prometheus stale marker) is an unknown value: it is stored as a hole.

## How the data is read

- A window is `[start, end]`. Without `end` it ends now. Without `start` it begins where the longest archive of the
  preset begins, so a request without a window returns the consolidated history of the whole file.
- RRDtool serves a window from **one archive: the finest that covers all of it**. A window of a few hours is
  served by the rows of 10 seconds, a window of a month by the rows of 10 minutes or an hour. The rows are not
  resampled: the time between two samples that you get back depends on the width of the window.
- The row that holds the last update is not complete in a coarse archive until its interval is over, so the
  newest minutes of a long window would be missing. SensApp reads them from the finer archives and appends them:
  a long window is coarse for the old data, then finer at its end. It costs a few more requests, only when a coarse
  archive served a window that ends after the last update of the series.
- All the rows of the window are returned, except the unknown ones: a series that has no value in the window is
  empty, and a series that has no file is "not found" (`404`).
- `limit` keeps the first rows of the window. A request with a `step` and an `aggregation` aggregates the rows after
  they were read, and then applies the limit.
- The "last sample" of a series is the last known row of the window, which is the average of the last 10 seconds
  with data, not necessarily the last value that was written.

## Operations

- One TCP or Unix connection to the daemon is shared by all the requests of an instance, which take turns: a
  request is a few round trips, about 0.1 ms each on a local network. For writes that is enough for tens of
  thousands of samples per second (see [Performance](#performance)). It is replaced when it breaks, and the request
  that found it broken is sent once more.
- A request to the daemon that takes more than 15 seconds is abandoned and the connection is replaced. A request
  that is cancelled by the HTTP timeout does the same. A daemon that cannot be reached is a `503` (so Prometheus
  retries), a refusal of the daemon is a `500`.
- `GET /health/ready` sends a `PING`: it does not write anything.
- `GET /series` has to list the files of the daemon, whose cost grows with the number of series (about 50 ms
  with 6 000). Pages are in UUID order.
- A selector that has the UUID of a series as its `__name__` is read directly. Anything else lists the files first.

## Performance

Measured on a laptop, with the daemon in a Docker container and a debug build of SensApp (the daemon's own time
dominates), the `rrdcached_performance` test of `tests/integration/rrdcached_integration.rs`:

| | |
|---|---|
| Bulk load, 50 series, batches of 8 192 samples | 190 000 samples/s |
| First write of series that have no file | 1.2 ms per file (1.2 s for 1 000 series). Checking that the file does not exist costs 0.4 ms of it: this is what protects the history |
| A scrape of 1 000 series, one sample each | 8 ms |
| Read of a series, one hour (360 rows) | 2.3 ms |
| Read of a series, 22 hours (8 000 rows) | 35 ms |
| Health check | 0.5 ms |

The writes do not wait for the disk: the daemon acknowledges an update when it has it in memory and its journal. A
`FLUSHALL` after each write used to be sent; its removal made small writes a third faster.

## Tests

Unit tests with a scripted daemon run with the library tests (`cargo test --features rrdcached --no-default-features
storage::rrdcached`). The integration tests need a daemon started with `-O` on port 42217, which is what CI does:

```bash
docker build -t sensapp-rrdcached docker/rrdcached
docker run -d --name sensapp-rrdcached -p 42217:42217 sensapp-rrdcached

TEST_DATABASE_URL="rrdcached://127.0.0.1:42217?preset=hoarder" \
  cargo test --no-default-features --features rrdcached --test integration rrdcached_integration::
```

They write series with a new UUID each, because the files stay in the daemon. The benchmark is ignored by
default: add `rrdcached_performance -- --ignored --nocapture`.

CI runs the tests against `rrdcached` 1.11. The same tests also passed against 1.7.2, the version of Debian's package,
and the Unix socket scheme was checked by hand with 1.11.

## Not done

- **A metadata sidecar** (names, labels, units, types), which would make the series searchable and Prometheus remote
  read useful. It needs a store that SensApp instances share, which is a bigger design than this backend deserves
  today.
- A pool of connections: the single one is not the limit that was measured.
- Deleting a series: the daemon has no command for it, the files would have to be removed from its directory.
- Other consolidation functions (`MIN`, `MAX`, `LAST`) as options; the files only have `AVERAGE` archives.
