# RRDCached Follow-Ups

The backend is "good enough" since `done/rrdcached-good-enough.md`: it works, its differences are written in
`docs/RRDCACHED.md`. These are the things that were left out on purpose.

## Metadata sidecar

Names, labels, units and types are not stored, so a series cannot be found by name and Prometheus remote read
cannot select by name. A sidecar needs a store that all instances share (a small SQLite or PostgreSQL table, or
a directory of JSON files next to the RRD files). Worth it only if someone needs Prometheus remote read on RRDCached.

## Smaller ideas

- A pool of connections: one connection did 190 000 samples/s on bulk loads and 8 ms for a scrape of 1 000 series,
  so it was not measured as a limit.
- `list_series` lists every file of the daemon for each page: a short cache of the sorted UUIDs would make
  pagination of 100 000 series cheap.
- Archives with `MIN` and `MAX`, as options of the presets, and a way to ask for them.
- Delete a series: the daemon has no command, the files have to be removed from its directory.
- The last sample of a series is the last known row of the window. The daemon's `INFO` has the last raw value
  (`last_ds`), which would be exact.
