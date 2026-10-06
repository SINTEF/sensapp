# Signed download links, and bigger or combined downloads

The explorer's **Download** button (`done/download-button.md`) fetches each series with the token in the
`Authorization` header, holds the answer in a `Blob` and saves it with `<a download>`. That is fine while a read
is capped at 100,000 samples (a few MB). This idea is what comes after, if downloads get bigger.

## Why a link

- Authentication reads the token **only from the `Authorization` header** (`src/http/auth.rs`). A plain
  `<a href="/series/…?format=csv&download=true">` cannot send it, so on a server with authentication it answers `401`.
- A browser download of a link has a progress bar, needs no copy in memory and can resume. It matters once the
  sample cap is lifted for exports (a streaming export).
- The request log records the whole URI (`uri = %request.uri()` in the `TraceLayer` of `src/http/server.rs`):
  anything in a query string is logged.
- Tokens are stateless (`ideas/token-registry-and-revocation.md`): one cannot be revoked or made single-use,
  only made short.

## Design

- An endpoint that trades a normal `read` token for a **short-lived link**, e.g.
  `POST /series/{uuid}/download-link` with the export parameters (`format`, `start`, `end`, `step`,
  `aggregation`, `simplify`) in the body, answering `{ "url": "…", "expires_at": "…" }`. Same `read` scope and
  sensor allow list as the read endpoint (a sensor the caller cannot read is a `404`).
- The link carries a token signed with the **same secret** as the API tokens (rotation and
  `SENSAPP_JWT_PREVIOUS_SECRETS` keep working), but:
  - a different **audience** (`sensapp-download`; API tokens are `aud = sensapp`), so it is refused on every
    other endpoint and an API token is refused on the download route;
  - bound to **one series and one set of parameters** (claims), so a leaked link exports that and nothing else;
  - **read only**; `sub` is the subject of the token that made it, for the audit trail;
  - a short `exp` (60 s), `nbf` and `iat` set.
- `GET /download/{token}` (token in the **path**) answers the export with `Content-Disposition: attachment`,
  reusing `get_series_data`. It must **not** read `start`/`end`/`format` from the query string: the claims are
  the only source of the parameters.
- **Logging**: redact the token (a `make_span_with` that logs `/download/<redacted>`), or accept it as
  short-lived and bound to one export. Decide, and test it. `subject`/`token_id` of the span from the claims.
- `Cache-Control: no-store`, `Referrer-Policy: no-referrer`.
- Replayable within its 60 seconds, not beyond its parameters. Say so in `docs/JWT_AUTH.md`, no more.
- Tests: expired link, wrong audience, API token on the download route, download token on `/series/{uuid}`,
  a changed query parameter, a sensor outside the allow list, a rotated secret.
- With `SENSAPP_AUTH_DISABLED` the endpoint can answer a plain link.

The dialog would then ask `download-link` and navigate to the URL instead of fetching.

## Other directions

- **One file for several series.** The dialog saves one file per series. The `*_multi` exporters
  (`src/exporters/`, used by `/api/v1/query`) already write a long CSV with `sensor_name` and label columns, an
  Arrow stream, SenML and JSON Lines for several series. An endpoint taking a list of uuids with
  `start`/`end`/`step`/`aggregation` could use them, with a shared sample budget (as the selector reads).
  A zip made in the browser is the other way (a dependency such as `fflate`).
- **The `curl` snippet of the Code dialog** could use `-OJ` and `download=true`, to save named files.

## Open questions

- Is the 100,000-sample cap going away for exports (streaming)? It decides whether signed links are worth it.
- Log the download token redacted, or accept it?
- Should a link outlive 60 seconds (resumable downloads of big files)?
