# Python SDK sends `True`, the server only accepts `true`

## Status

Fixed on 1 October 2026. `SensAppClient._get` now sends bools as `true` / `false`; unit test `test_get_series_serializes_bools_for_the_server` and a `simplify=True` call in the live test `test_live_polars_upload_helper_and_get_series` cover it. The live tests were run on 1 October 2026 against a release build (`pytest -m integration`, 2 passed), and `get_series(..., simplify=True, simplify_tolerance=0.001)` works from `zeblab_alarm.ipynb`. Server decision: kept strict on `true` / `false`, no change.

## Symptom

`SensAppClient.get_series(uuid, simplify=True, simplify_tolerance=0.001)` fails with:

```
HTTP 400: Failed to deserialize query string: simplify: provided string was not `true` or `false`
```

`client.py` passes the Python `bool` to niquests, which serializes it with `str()`, so the URL carries `simplify=True`. The server deserializes `Option<bool>` query parameters with serde and accepts exactly `true` and `false`. The same applies to `simplify_high_quality` and anything added later.

## Plan

### SDK (required)

- In `SensAppClient._get`, convert bool values to `"true"` / `"false"` next to the existing removal of `None` values, so every current and future bool parameter is covered, not only the simplify ones.
- Add a unit test on the built query string (mocked transport is enough): `simplify=True` must send `simplify=true`, `False` must send `false`, `None` must be omitted.
- Add `get_series` to a live-server SDK test with `simplify=True` once the live test exists (see `arrow-export-timestamp-offset.md`).

### Server (decision needed)

Bool query parameters today: `simplify`, `simplify_high_quality` (`src/http/crud.rs`) and `include_latest_samples` (`src/http/metrics.rs`).

Recommendation: keep the server strict on `true` / `false`. That is what OpenAPI means by `boolean`, and the current error already names the parameter. Do **not** accept YAML-style booleans (`yes`, `no`, `on`, `off`, `y`, `n`): they are a YAML 1.1 quirk, they are ambiguous in a URL, and no HTTP client sends them.

Optional, only if we want to forgive sloppy clients: one shared `deserialize_with` helper accepting case-insensitive `true|false` and `1|0`, used by the three parameters above. Decide when working on this task; the SDK fix does not depend on it.

## Validation

- `cd python/sensapp && uv run ruff check . && uv run pytest`
- Against a running server: `get_series(..., simplify=True, simplify_tolerance=0.001)` returns data.
