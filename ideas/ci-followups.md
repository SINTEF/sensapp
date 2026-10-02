# CI follow-ups

- Share the Docker cache scope between `docker-smoke` (amd64) and the publish build, or make
  smoke PR-only, so the release build is not compiled twice on pushes to `main`.
- If a release ever needs static DuckDB again: keep `bundled` and use sccache for the C++ objects.
