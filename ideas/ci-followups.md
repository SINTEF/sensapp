# CI follow-ups

- Share the Docker cache scope between `docker-smoke` (amd64) and the publish build, or make
  smoke PR-only, so the release build is not compiled twice on pushes to `main`.
- The `migrate-*` and `setup-dev` cargo-make tasks are no longer used by CI (the app migrates
  itself); drop them if nobody uses them locally.
- If a release ever needs static DuckDB again: keep `bundled` and use sccache for the C++ objects.
