# Web UI

SensApp ships a small explorer: pick a metric, pick series, draw them over a time range. It is a React single page application in [`frontend/`](../frontend), built to static files that SensApp serves itself. There is no separate container.

## Serving

- The files are served under `/ui/`, and `/` redirects there. The container image carries them (the Dockerfile builds them in its own Node stage); with `cargo run`, build them first with `npm run build` in `frontend/`.
- `SENSAPP_UI_ENABLED=false` turns it off, `SENSAPP_UI_DIR` says where the files are. See [CONFIGURATION.md](CONFIGURATION.md#web-ui). A missing build is a warning at startup, never a failure.
- The files are public. Responses carry a `Content-Security-Policy` (the page only talks to its own origin, and cannot be framed), `X-Content-Type-Options: nosniff` and `Cache-Control: no-cache`.
- The UI calls the SensApp API of its own origin: `/metrics`, `/series`, `/series/{uuid}` and `/health/ready`.

## Charts

The server refuses to send more than 100 000 raw samples of a series ([HTTP_LIMITS.md](HTTP_LIMITS.md)), so the chart asks for a `step` once the time range is longer than about 2.8 hours: the range is cut in at most 2 000 buckets of a round duration (`10s`, `1m`, `10m`, `1h`, …) and the samples of a bucket are averaged (`aggregation=avg`). Shorter ranges are read as they are. Series that are not numbers (strings, booleans) are always read as they are, and are not drawn.

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
