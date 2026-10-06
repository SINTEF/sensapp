# Download button in the UI, and download support in the backend

A **Download** button in the explorer that saves what is on screen (the selected series, over the selected
time range, in a chosen format) as a file, with the backend support it needs: an `attachment`
`Content-Disposition`, and a short-lived signed link so that the browser itself can do the download.

## Where we are

- `GET /series/{uuid}` already exports: `?format=csv|jsonl|senml|arrow`, `start`, `end`, `limit`, `step`,
  `aggregation`, `simplify` (`SensorDataQuery`, `src/http/crud.rs`; content types in `ExportFormat`).
- It answers with a `Content-Type` only. There is **no `Content-Disposition`** anywhere, so a browser shows
  the CSV or JSON instead of saving it, and `curl -OJ` has no file name to use.
- A raw read is capped at **100,000 samples** (`docs/HTTP_LIMITS.md`). A larger export needs `step` and
  `aggregation`, or a window, so a download today is at most a few MB.
- Authentication reads the token **only from the `Authorization` header** (`Bearer`, and the InfluxDB
  `Token` scheme), `src/http/auth.rs`. The UI keeps the token in `sessionStorage` and sends it with `fetch`.
- That is the real obstacle: `<a href="/series/…?format=csv" download>` cannot send a header, so on a server
  with authentication a plain link answers `401`.
- The request log records the whole URI (`uri = %request.uri()` in the `TraceLayer` of
  `src/http/server.rs`): **anything put in a query string is logged**.
- Tokens are stateless (no registry, see `ideas/token-registry-and-revocation.md`): a token cannot be revoked
  or made single-use, only made short.

## Backend

### 1. `Content-Disposition` (small, useful on its own)

- `GET /series/{uuid}` gets `Content-Disposition: attachment; filename="…"` when asked: a `download` query
  parameter (`?download=1`, or `?download=my-name`), not by default, so that the existing API behaves as it does.
- The file name: `<series name>_<start>_<end>.<ext>`, the extension from the format (`csv`, `jsonl`, `json`,
  `arrow`). Names and labels hold spaces, dots, slashes and non-ASCII characters (`Sanitæranlegg`), so:
  - send both `filename="<ASCII fallback>"` and `filename*=UTF-8''<percent-encoded>` (RFC 6266 / 8187);
  - the ASCII fallback keeps `[A-Za-z0-9._-]` and replaces the rest with `_`; no path separators, no leading
    dot, a length limit;
  - **header injection**: names come from stored data. Never put a raw name in a header (CR, LF, quotes,
    backslash). Build the value through the `http` header API and test with hostile names.
- Test it for the four formats, with a Unicode name, and with a name full of quotes and newlines.
- Document it in the OpenAPI (`utoipa`) and in `docs/DATA_LIFECYCLE.md` or the API docs, and regenerate the
  frontend client (`npm run openapi-ts`).

### 2. Signed download links (for downloads the browser makes itself)

Needed when the download should be a real browser download (progress bar, no copy in memory, resumable, large
files once the 100,000-sample cap is lifted or streaming exports exist) on a server with authentication.

- An endpoint that trades a normal `read` token for a **short-lived link**, e.g.
  `POST /series/{uuid}/download-link` with the export parameters (`format`, `start`, `end`, `step`,
  `aggregation`, `simplify`) in the body. It answers `{ "url": "…", "expires_at": "…" }`.
  Requires the `read` scope and the same sensor allow list as the read endpoint (a sensor the caller cannot read
  is a `404`, like the other endpoints).
- The link carries a token signed with the **same secret** as the API tokens (so key rotation and
  `SENSAPP_JWT_PREVIOUS_SECRETS` keep working) but built so that it cannot be mistaken for an API token:
  - a different **audience** (`sensapp-download`; the API tokens are `aud = sensapp`, which the validation
    checks, so a download token is refused on every other endpoint, and an API token is refused on the
    download route);
  - bound to **one series and one set of parameters** (claims: the series uuid, and the format, window, step,
    aggregation), so a leaked link exports exactly that and nothing else;
  - **read only**, never any other scope; `sub` is the subject of the token that made it, so the audit trail
    (`subject` in the log span) says who exported what;
  - a short `exp` (60 seconds is enough: the browser starts the request at once); `nbf` and `iat` set.
- The route that honours it, e.g. `GET /download/{token}` (token in the **path**, not the query), that
  answers with the export and `Content-Disposition: attachment`. It reuses the export code of
  `get_series_data` rather than copying it.
- **Logging.** The path is logged like any URI. Either redact the token (a custom `make_span_with` that
  logs `/download/<redacted>`), or accept it because it is short-lived and bound to one export. Decide, and
  test that the token does not reach the logs if redacted. The request's `subject`/`token_id` fields should be
  filled from the claims.
- **Referrer and caching.** The route answers with `Cache-Control: no-store` and `Referrer-Policy: no-referrer`.
- **Not replayable beyond its parameters, but replayable within 60 seconds.** Stateless tokens cannot be
  single-use (see the token registry idea). State that in `docs/JWT_AUTH.md`; do not claim more.
- Tests for the rules: expired link, wrong audience, API token on the download route,
  download token on `/series/{uuid}`, a changed parameter (the signature covers the claims, not the URL, so
  the claims are the only source of the parameters: the route must **not** read `start`/`end`/`format` from
  the query string), a sensor outside the allow list, rotated secret.

With authentication disabled (`SENSAPP_AUTH_DISABLED`) the endpoint can answer a plain link, and the
download route is open like everything else.

## Frontend

- A **Download** button in the explorer, next to the chart (`ExplorerPage.tsx`, `TimeSeriesChart.tsx`; the
  existing `CodeButton`/`CodeDialog` is the model for a small dialog). It exports what is on screen: the
  selected series, the time range of `TimeRangeSelector`, and the `step`/`aggregation` the chart uses.
- A **format** choice (CSV by default, then JSON Lines, SenML, Arrow), and the file name it will get.
- One file per series (the endpoint is per series). With several series selected, either one download per
  series, or a zip made in the browser, or a combined file (CSV with a series column). Open question; start
  with one file per series, listed in the dialog.
- How the click works, in two steps so that the first one ships without the signed links:
  1. **Without signed links**: `fetch` with the `Authorization` header, read the response as a `Blob`, save it
     with an object URL and `<a download>`. Works today, buffers the file in memory (fine below the 100,000
     sample cap), and gets the file name from `Content-Disposition` once part 1 exists.
  2. **With signed links**: ask `download-link`, then navigate to the URL (`window.location` or a hidden
     `<a>`), so the browser downloads natively.
- Show progress and failure: the same `Feedback` component as the other pages; a `401`/`403` re-opens the
  sign-in dialog like the other requests; an export over the sample cap says to narrow the window or use a
  `step` (the server's `400` text), and offers the step.
- Tests (vitest, as for the other components): the button is disabled with nothing selected; it requests the
  right parameters; the file name; the error paths.
- Update `docs/FRONTEND.md` (explorer section) and `docs/JWT_AUTH.md` (download tokens: audience, lifetime, what
  they can and cannot do).

## Order

1. `Content-Disposition` behind `?download=` (backend, tests, OpenAPI, client).
2. The button with the `Blob` download (frontend).
3. Signed links, if the exports get big enough to justify them (they pair naturally with a streaming export
   that lifts the 100,000-sample cap; without that, step 2 is enough).

## Open questions

- Is the 100,000-sample cap going away for exports (streaming)? It decides whether step 3 is worth it.
- One file per series or a combined file for several series?
- Log the download token redacted, or accept it as it is?
- Should the link be allowed to outlive 60 seconds (resumable downloads of big files)?
