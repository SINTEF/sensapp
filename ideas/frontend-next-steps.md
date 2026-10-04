# Frontend: next steps

What was left out of `done/frontend-good-enough.md` on purpose, most useful first.

- **Selection in the URL.** Metric, series and time range as query parameters, so that a chart can be shared
  or reloaded. Small with the router already in place.
- **Non-numeric series.** Strings, booleans and locations are listed but not drawn. A table of the latest
  values (`/series/{uuid}/last`) would show them.
- **Refresh.** A live "last 15 minutes" that follows the clock, with an auto-refresh interval.
- **Several instances, one UI.** The API base is `VITE_SENSAPP_API_URL` at build time. A runtime setting would
  allow one build against any instance; only useful if the UI is ever served apart from SensApp.
- **TypeScript 7.** Held at 5.9 until `typescript-eslint` supports it (peer range `<6.1`).
- The DCAT types of `src/hooks/useMetrics.ts` and `useSeries.ts` are written by hand because the OpenAPI
  document types the catalogs as a plain string. Typing them in the server would remove the hand-written copy.
