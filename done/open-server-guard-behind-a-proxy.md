# Open-server guard: usable behind a proxy, and effective on a non-loopback bind

Source: review of pull request 60 on 11 November 2026 (HEAD `7d8b396`). Confirmed by reading; not executed.

## Problem

`src/http/open_server.rs` protects a server that runs with `SENSAPP_AUTH_DISABLED`: on a loopback bind it
refuses a `Host` that is not a loopback name, and everywhere it refuses a request whose `Origin` differs
from its `Host` (`:51-69`).

1. **It breaks the documented setup "authentication in a proxy in front, SensApp on loopback".**
   - A proxy that preserves `Host` (Caddy, Traefik, ingress-nginx; what `docs/JWT_AUTH.md:38` recommends):
     `Host: sensapp.example.com` is not a loopback name, so every request is a 403, reads included.
   - A proxy that rewrites `Host` (plain nginx `proxy_pass`): the loopback check passes, but a browser write
     carries `Origin: https://sensapp.example.com` against `Host: 127.0.0.1:3000`, so it is a 403.
   - There is no setting to name the public host. The doc sentence "which proxies do by default" is wrong
     for nginx.
2. **DNS rebinding is only stopped on a loopback bind.** The Docker image listens on `0.0.0.0`, and
   `compose.prometheus-roleplay.yml:12` runs open that way: `Host: evil.example:3000` with a matching
   `Origin` is accepted (the unit test asserts it).
3. **Its 403s are invisible**: the guard is the outermost layer (`src/http/server.rs:324-328`), outside the
   metrics and trace layers, so they have no request id and are not counted.
4. `frontend/vite.config.ts:20` uses `changeOrigin: true`: in `npm run dev` against an open server, a
   non-GET from the UI would get a 403. Latent, since the UI is read-only.
5. Tests have no case for a missing `Host`, HTTP/2 `:authority`, or the proxy cases.

## Suggested solution

One setting, `SENSAPP_ALLOWED_HOSTS` (a comma-separated list of `host[:port]`):

- When set, a request passes when its `Host` is in the list, and an `Origin` passes when its authority is in
  the list. This serves the proxy setup (`sensapp.example.com`) and closes rebinding on any bind address.
- When unset: keep today's loopback rule on a loopback bind. On a non-loopback bind without the setting,
  log a warning at startup that DNS rebinding is not prevented, and keep the same-origin rule.
- Move the layer inside the metrics and trace layers.
- Fix the docs: the proxy section of `docs/JWT_AUTH.md` names the setting.

## Tests

Proxy preserving `Host` with the setting (pass) and without (403); proxy rewriting `Host` with a browser
`Origin` in the list (pass); rebinding on `0.0.0.0` with the setting (403); missing `Host`; the 403 appears
in `sensapp_http_requests_total`.

## Done when

The tests pass and the documented proxy setup works as written.

## Outcome (11 November 2026)

Items 1 to 3 and 5 were real; the guard worked as the review read it.

- New `SENSAPP_ALLOWED_HOSTS` (`host[:port]`, comma separated, validated at startup: a scheme or a path is
  refused). With it, a `Host` must be listed or a loopback name, on any bind address, and an `Origin` must be
  listed or equal the `Host`: a proxy that keeps the `Host` works, one that rewrites it works for a browser whose
  `Origin` is the public name, and rebinding on `0.0.0.0` is a 403. A missing `Host` is refused with the setting
  (HTTP/2 is judged by its `:authority`); without the setting everything is as before, and a non-loopback bind
  logs a warning at startup.
- `build_router_guarded` puts the guard inside the metrics and trace layers: the 403 is in
  `sensapp_http_requests_total{method="POST",path="/api/v2/write",status="403"}` (integration test).
- Tests: proxy keeping and rewriting the `Host` with and without the setting, rebinding on a non-loopback bind,
  port part of the name, case, missing `Host`, `:authority`, the metrics, and the config validation.
- Item 4 (`frontend/vite.config.ts` `changeOrigin: true`) left as is: the UI is read-only, so no write is
  affected; with `SENSAPP_ALLOWED_HOSTS=localhost:5173` an open dev server accepts it.
- `compose.prometheus-roleplay.yml` runs open on `0.0.0.0` and now gets the startup warning; its host names were
  not changed.

Ran: SQLite and PostgreSQL in full (350 + 388 + 2), clippy with and without all features.
