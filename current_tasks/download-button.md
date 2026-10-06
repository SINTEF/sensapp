# Download button in the explorer

A **Download** button next to **Code** opens a dialog: a format, a sampling (raw or aggregated), a window
(the chart's or all the data). It saves one file per selected series. No job queue: a series over the
sample cap (100,000 by default) shows the server's error in the dialog.

From `ideas/download-button-and-signed-download-links.md` (part 1 and step 2). What is left of that idea
(signed links, streaming exports, a combined file) is in `ideas/signed-download-links.md`.

## Decisions

- **One file per series**, from `GET /series/{uuid}`. Each series has its own sample cap, a failure does not
  stop the others. `/api/v1/query` writes several series in one file, but only for a selector over a window
  ending now, not for a selection over a chosen start and end.
- **Progress in the dialog**, one series after another. Closing the dialog cancels the rest.
- The UI keeps the token in a header, so a plain link cannot download on a server with authentication: the
  dialog uses `fetch`, holds the file in a `Blob` (a few MB at most under the cap) and saves it with `<a download>`.
- The server names the file (`Content-Disposition`, asked with `?download=true`), so `curl -OJ` and the UI agree.
  The name has the labels: a single-series CSV is only `timestamp,value`, and selected series often share a name.

## Plan

1. [x] Backend: `?download=true` on `GET /series/{uuid}` adds `Content-Disposition: attachment` with
   `filename` (ASCII fallback) and `filename*` (UTF-8). Unit tests for the name and hostile names,
   backend-generic integration tests. OpenAPI document and generated client.
2. [ ] Frontend: `DownloadButton`, `DownloadDialog`, `lib/download.ts`, tests.
3. [ ] Docs (`docs/FRONTEND.md`, the API doc of `GET /series/{uuid}`), live check, move this file to `done/`.

## Progress

- Backend: `src/http/download.rs` (file name, header), `?download=true` in `get_series_data`. The 3 new
  integration tests and the rest of `query_export` pass on SQLite, PostgreSQL, TimescaleDB and ClickHouse
  (26 tests each).
- Found on the way: an InfluxDB write percent-encodes the measurement name (`Sanitæranlegg` is stored as
  `Sanit%C3%A6ranlegg`), not the tag values. Existing behaviour, left as it is; the test puts its non-ASCII
  text in a tag. An InfluxDB series also has `influxdb_bucket` and `influxdb_org` labels, which are in its
  file name.
