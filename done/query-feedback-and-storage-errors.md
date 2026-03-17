# Query Feedback And Storage Errors

## Goal

- keep hyphenated sensor names allowed
- make simple PromQL failures explain why hyphenated bare metric names break
- report storage backend unavailability more clearly during ingestion

## Delivered

- preserved hyphenated sensor names during ingestion and added coverage for that behavior
- improved simple PromQL binary-operation errors with a specific hint for hyphenated metric names
- documented the `__name__` matcher workaround in the Python SDK README
- preserved storage errors when they are wrapped in `anyhow`
- mapped backend-unavailable storage failures to HTTP `503` with a clear `Database unavailable` response

## Validation

- `cargo fmt`
- `cargo check`
- `cargo test --test simple_promql --test ingestion`
- `cargo test internal_server_error_preserves_storage_error_category`
- `cargo clippy --tests -- -D warnings`

## Notes

- existing `dead_code` warnings in `src/http/auth.rs` are still present during `cargo check` and `cargo test`, but `cargo clippy --tests -- -D warnings` passed unchanged
