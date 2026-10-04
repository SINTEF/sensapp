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

1. [ ] Analysis, dependency refresh
2. [ ] Serve the UI under `/ui/` from SensApp (config, redirect, Docker stage, docs)
3. [ ] Authentication in the frontend (token dialog, 401/403 handling, tests, live check against a JWT server)
4. [ ] Code quality pass (error handling in one place, smaller bundle, dead code, README)
5. [ ] Live check in a browser, then the next steps

## Findings

Filled in as the work goes.
