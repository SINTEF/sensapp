# Helm external storage

Make the SensApp chart straightforward to use with an externally managed PostgreSQL, TimescaleDB, or ClickHouse database. Keep SQLite as the simple default, allow a Kubernetes Secret to provide the connection string, and avoid a local data volume for external storage. Document the deployment options without adding database dependencies to the chart.

Completed: added an existing Secret reference, conditional data volume, and deployment examples. Helm lint and rendering checks, `cargo check`, and `cargo clippy` passed. Rust tests were skipped at the user's request.
