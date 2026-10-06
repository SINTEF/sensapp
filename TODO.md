# SensApp TODO

This file tracks the main remaining work for SensApp. Detailed task history lives in `current_tasks/`, `ideas/` and `done/`.

- [ ] Add benchmark tooling for storage backend comparison (a first script exists: `tests/perf/scale.sh`, writes and selector reads of thousands of series through the HTTP API)
- [ ] Add research-specific comparison endpoints and reporting helpers
- [ ] Add storage-space and latency comparison reports across backends
- [ ] Data retention (`ideas/data-retention.md`)
- [ ] Download button in the UI, `Content-Disposition` and signed download links in the backend (`ideas/download-button-and-signed-download-links.md`)
- [ ] Cross-series aggregation pushdown, composite sensors, a minimal PromQL `rate()`
