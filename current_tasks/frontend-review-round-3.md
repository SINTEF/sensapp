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

1. [ ] Small cleanups: header count, health badge, metric name width, metric row keys
2. [ ] Live range
3. [ ] Series list: dimension columns, small uuid, type when mixed, colour swatch, auto-select
4. [ ] Chart: no legend, animated toggles, stable colours
5. [ ] Logo, favicon, theme, palette
6. [ ] Docs (`docs/FRONTEND.md`), live check in a browser, move to `done/`
