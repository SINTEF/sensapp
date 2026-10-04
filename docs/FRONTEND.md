# Web UI

SensApp ships a small explorer: pick a metric, pick series, draw them over a time range. It is a React single page application in [`frontend/`](../frontend), built to static files that SensApp serves itself. There is no separate container.

## Serving

- The files are served under `/ui/`, and `/` redirects there. The container image carries them (the Dockerfile builds them in its own Node stage); with `cargo run`, build them first with `npm run build` in `frontend/`.
- `SENSAPP_UI_ENABLED=false` turns it off, `SENSAPP_UI_DIR` says where the files are. See [CONFIGURATION.md](CONFIGURATION.md#web-ui). A missing build is a warning at startup, never a failure.
- The files are public. Responses carry a `Content-Security-Policy` (the page only talks to its own origin, and cannot be framed), `X-Content-Type-Options: nosniff` and `Cache-Control: no-cache`.
- The UI calls the SensApp API of its own origin: `/metrics`, `/series`, `/series/{uuid}` and `/health/ready`.

## Charts

Two selectors next to the time range control what the server computes, as in the Influx and Prometheus UIs:

- **Step**: `Auto`, `Raw`, or a fixed duration (`5s` … `1d`). `Auto` reads ranges under about 2.8 hours as they are, and above that cuts the range in at most 2 000 buckets of a round duration, because the server refuses more than 100 000 raw samples of a series ([HTTP_LIMITS.md](HTTP_LIMITS.md)). `Raw` on a range that is too wide gets that refusal.
- **Aggregation**: `avg`, `min`, `max`, `sum`, `count`, `first`, `last`, applied to the samples of each step. Disabled on `Raw`.

Series are drawn as a `line`, `step`, `area`, `stacked` or `bars` chart, on a linear or `log` scale. Booleans are drawn as a 0/1 step line on an axis of their own, whatever the style, and always read as they are (the server cannot aggregate them). Strings and locations are listed but not drawn.

## Address

The explorer is in the address, so a link shares a view and a reload keeps it: `/ui/?metric=cpu&series=<uuid>&series=<uuid>&range=24h&step=5m&agg=max&style=area&log=1`.

| Parameter | Meaning | Left out when |
| --- | --- | --- |
| `metric` | the selected metric | none |
| `series` | a selected series, repeated | none |
| `range` | a preset (`15m`, `1h`, `6h`, `24h`, `7d`, `30d`), ending when the page opens | `1h` |
| `from`, `to` | an absolute range (ISO 8601), once dates were typed | a preset is used |
| `step`, `agg` | the step (`raw` or a duration) and the aggregation | `auto`, `avg` |
| `style`, `log` | the chart style, `log=1` for the log scale | `line`, linear |

The address follows the explorer without adding history entries. What is wrong in an address is ignored; a series that no longer exists is dropped. A series is named by its uuid only: the UI asks the server for the first sample of each one (its name, labels and type), and on a server with authentication a link asks for a token like any other page.

The list of series is paged by the server (256 per page, cursor based): Previous and Next appear when there is more than one page, and the selection is kept from page to page. The theme follows the OS (light or dark), the chart included.

## Authentication

With `SENSAPP_JWT_SECRET` unset the UI works without further ado. When it is set, the API answers `401` and the UI shows a dialog asking for a token: paste the output of `sensapp generate-token <name> --scope read`. See [JWT_AUTH.md](JWT_AUTH.md).

- The token is kept in `sessionStorage`: it is gone when the tab closes and never shared with other tabs.
- An expired or invalid token (`401`), or a token without the `read` scope (`403`), shows the dialog again with the answer of the server.
- "Sign out" in the header forgets the token.
- A token with a sensor allow list shows only its sensors, as with any client.

SensApp has no login endpoint: it does not know users, only signed tokens.

## Development

```bash
cd frontend
npm ci
npm run dev        # http://localhost:5173/ui/, proxies the API to http://localhost:3000
npm run lint && npm run typecheck && npm test && npm run build
```

Run SensApp on port 3000 next to it (`cargo run`).

`src/client` is generated from `openapi.json`, and both are committed. `openapi.json` is the OpenAPI document of the server: a Rust test fails when it is out of date. After an API change:

```bash
UPDATE_OPENAPI=1 cargo test frontend_openapi_document   # rewrites frontend/openapi.json
cd frontend && npm run openapi-ts                       # regenerates src/client
```

`VITE_SENSAPP_LIVE_URL=http://localhost:3000 npm test` also runs the test that talks to a running SensApp (without authentication).
