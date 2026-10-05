# Frontend: third review round (5 October 2026)

Branch: `frontend-good-enough`. Comments from the user after using the UI.

## Decisions

- **"N series" in the chart header**: removed, the series list says it (`Clear (3)`, `3 selected`).
- **End time is now**: a preset range is re-resolved every minute while the tab is visible, when the tab
  comes back to the front, and when the active preset is clicked again. Queries keep their previous data
  while the new range loads (no flicker), the zoom of the chart is kept.
- **Metric names**: a maximum width on the name column, truncated, the full name in the tooltip.
- **Selecting a metric selects its series** when there are at most 8 (the size of the palette: more would
  repeat colours and the chart would be noise). More than that: the user picks.
- **Series list**: one column per label dimension, one row per series, the uuid small and last. The Type
  column only appears when the series of the metric are not all of one type. This can happen: the server
  groups metrics by (name, type) and `/series?metric=` filters on the name only, so a name that exists
  as a float and as a string is two rows in the metrics list and one list of series.
- **Colours belong to the series list**: a swatch on each selected row, the echarts legend is gone. The
  colour of a series is fixed while it is selected (lowest free slot), disabling another one does not
  repaint it. Toggling a series animates in the chart (merge instead of a full replace).
- **"Connected" badge**: removed. A server that is down already shows the error of the request.
- **Logo, favicon, theme**: the SensApp logo in the header (light, dark and small variants of `public/`), a
  favicon made from it, a theme taken from the logo blue, and the validated categorical palette of the
  dataviz skill for the series (light and dark steps).

## Steps

1. [x] Small cleanups: header count, health badge, metric name width, metric row keys
2. [x] Live range (`hooks/useLiveRange.ts`). `keepPreviousData` does not work with `useQueries` (a new key is a
   new observer), so the chart keeps the last data of each series itself.
3. [x] Series list: dimension columns, small uuid, type when mixed, colour swatch, auto-select
4. [x] Chart: no legend, animated toggles, stable colours (`lib/palette.ts`)
5. [x] Logo, favicon, theme, palette
6. [x] Docs (`docs/FRONTEND.md`), live check in a browser

## Results

Verified in a browser against a live SensApp (SQLite, Influx-written data, dark and light, 1280 and 375 px):
auto-select of a 3-series metric with swatches matching the chart, no auto-select of a 12-series one,
unselecting a series leaves the colours of the others and reselecting gives it its colour back, a zoom
survives toggling, the range moved by itself after a minute, the metrics table does not scroll sideways.
`npm test` (130), lint, typecheck and build pass.

Found on the way: the server sends `Float` and `String`, so the colours of the metric type badges, chosen on
`float` and `string`, never applied. Fixed. Not looked at: the animation itself (only its result), the dark
logo in the pane at 1280 px.

Left (in `ideas/`): `frontend-constant-label-columns.md` (the Influx importer adds `influxdb_org` and
`influxdb_bucket` to every series, two useless columns), and more than 8 series at once (`frontend-next-steps.md`).
The original logo files in `frontend/public/` (`sensapp_logo.png`, `_white`, `_small`, `_small_white`) are not
committed: the page uses the `-fs8` ones.
