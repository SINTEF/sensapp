# Frontend: next steps

What was left out of `done/frontend-good-enough.md` on purpose, most useful first.

- **Strings and locations.** Listed but not drawn. A table of the values over the range, or the latest value
  (`/series/{uuid}/last`), would show strings; locations want a map.
- **Several instances, one UI.** The API base is `VITE_SENSAPP_API_URL` at build time. A runtime setting would
  allow one build against any instance; only useful if the UI is ever served apart from SensApp.
- **TypeScript 7.** Held at 5.9 until `typescript-eslint` supports it (peer range `<6.1`).
- The DCAT types of `src/hooks/useMetrics.ts` and `useSeries.ts` are written by hand because the OpenAPI
  document types the catalogs as a plain string. Typing them in the server would remove the hand-written copy.
- **Expression box (PromQL).** Decided against for now: the series browser with step and aggregation covers one
  series at a time, and the box would only add combining series (`avg by (room)`, through `/api/v1/query`).
  Revisit when someone asks for that.
- Not planned: dashboards, alerting, a query builder (Grafana's job).
- **An overview of the whole life of the series** in place of the echarts slider that was removed (the drag on the
  chart and the bar below it do the zooming now): `/series/{uuid}/availability` could say where there is data, as
  a thin strip above the time bar, clicking in it sets the window. A third thing to look at, so only if people
  lose their way in long histories.
- **Wheel zoom and pan by drag** on the chart. The drag is a selection (as in Grafana) and the wheel scrolls the
  page on small screens; `‹ ›` and `−` cover them.
- **More than 24 series** repeat colors. Past 8 the colors cannot say which is which anyway (see
  `docs/FRONTEND.md`); small multiples, or a warning, would be the answer.
- **Auto-refresh** was declined at first, but a preset range now follows the clock every minute (the end is now),
  which is the same thing for the chart. A refresh switch would only be needed to stop it.
