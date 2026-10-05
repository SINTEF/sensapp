# InfluxDB clients send `Authorization: Token`, SensApp reads `Bearer`

Found while writing the Load Data tab (5 October 2026). Telegraf's `outputs.influxdb_v2` sends its `token` as
`Authorization: Token <token>`, as InfluxDB v2 clients do. `src/http/auth.rs` only strips `Bearer `, so with
`SENSAPP_JWT_SECRET` set a stock Telegraf (or the InfluxDB client libraries) gets `401`.

Today's workaround, in the UI snippet and `docs/FRONTEND.md`: `http_headers = {"Authorization" = "Bearer ${SENSAPP_TOKEN}"}`.

The idea: accept the `Token ` scheme as well on the InfluxDB write route (`/api/v2/write`) only, or everywhere
(one more `strip_prefix`). It makes "update the URL and credentials", the promise of `docs/INFLUX_DB.md`, true with
authentication on. Small change, with a test on each route; `docs/JWT_AUTH.md` and `docs/INFLUX_DB.md` to update,
then the `http_headers` lines of the Load Data snippet (`src/lib/loadSnippets.ts`) to drop.

## Done

Closed with the secure-by-default authentication work (October 2026): `validate_token` accepts `Authorization: Token <jwt>` as well as `Bearer`, on every route (tests in `src/http/auth.rs` and `tests/integration/jwt_auth.rs`, and a live `POST /api/v2/write` with the `Token` scheme). The Load Data snippet now gives Telegraf its own `token = "${SENSAPP_TOKEN}"`, without the `http_headers` workaround. The docs are updated with the rest of that work.
