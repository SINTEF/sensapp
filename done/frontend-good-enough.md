# Frontend: good enough, minimal, served by SensApp

Branch: `frontend-good-enough` (no pull request yet).

## Goal

A small, solid explorer UI (metrics, series, chart) that ships with the SensApp image and works with and
without JWT authentication. Keep it minimal.

## Decisions

- No separate container. The Docker build gets a Node stage that builds the frontend; the image carries
  the static files and SensApp serves them under `/ui/`.
- SensApp serves the UI by default (`SENSAPP_UI_ENABLED=true`). When enabled, `/` redirects to `/ui/`.
  The files come from `SENSAPP_UI_DIR`; a missing directory is a warning, not a startup failure.
- Authentication: there is no login endpoint (tokens come from `sensapp generate-token`), so on a 401 or 403
  the UI asks for a token, keeps it for the browser tab only (`sessionStorage`) and sends it as a Bearer token.

## Steps

1. [x] Analysis, dependency refresh
2. [x] Serve the UI under `/ui/` from SensApp (config, redirect, Docker stage, docs)
3. [x] Authentication in the frontend (token dialog, 401/403 handling, tests, live check against a JWT server)
4. [x] Code quality pass (error handling in one place, smaller bundle, dead code, README)
5. [x] Live check in a browser, then the next steps: `ideas/frontend-next-steps.md`

## Results

Dependencies: everything is at its latest major (Vite 8, Vitest 5, ESLint 10, jsdom 30) except TypeScript, held
at 5.9 because `typescript-eslint` does not support 7. `echarts-for-react` is gone (unmaintained, imports an
undeclared `tslib`): a 40 line wrapper on echarts core with only the line chart. `npm audit`: 0 findings.

Build output: 51 asset files (1.7 MB) down to 9 (1.1 MB): Latin-only Roboto, echarts loaded only when a chart is
drawn (1 146 kB to 583 kB, 384 to 196 kB gzipped). The app itself is 103 kB gzipped.

Serving: `src/http/ui.rs`, `SENSAPP_UI_ENABLED` / `SENSAPP_UI_DIR`, `/` redirects to `/ui/`. Public files with a
CSP, nosniff and no-cache. Verified in a container built from the Dockerfile (Node stage on the build platform,
files in `/usr/share/sensapp/ui`, runs as the non-root user), and `tests/clickhouse_container_smoke.py` now
checks it in CI.

Authentication: verified against a live SensApp with JWT enabled and in a browser: no token, an invalid token
(401, "Invalid token: InvalidToken"), a write-only token (403, "Read access required"), a read token, a reload
(the token survives in the tab), Sign out (token and data forgotten, dialog again).

Found on the way, fixed: `frontend/openapi.json` had drifted from the server (a test now guards it and the
client is regenerated); raw reads over 100 000 samples are refused by the server, so wide ranges are averaged by
a `step`; the page was unusable on a phone (clipped header and date range, a Metrics panel squeezed to a sliver).

Found on the way, left for later (in `ideas/`): `string-series-aggregation-500.md` (server),
`metrics-tests-need-loaded-config.md` (two unit tests that depend on test order), `frontend-next-steps.md`.

Not checked: the Dockerfile build for linux/arm64 (the Node stage is platform independent), and the complete
`docker-smoke` job with ClickHouse (only its new `check_ui` step, against an image with SQLite).
