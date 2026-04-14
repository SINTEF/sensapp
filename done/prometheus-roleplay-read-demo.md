# Prometheus roleplay read demo

## Goal

Create a small Docker Compose stack that runs SensApp against the existing roleplay DuckDB file and configures Prometheus to read historical series from SensApp through the Prometheus remote read API.

## Constraints

- Use the same effective runtime settings as the local DuckDB command used by the roleplay notebooks.
- Do not depend on an InfluxDB query API because SensApp only implements the InfluxDB write API.
- Keep the setup simple enough for local testing and manual querying.

## Outcome

- Added `compose.prometheus-roleplay.yml` to run a DuckDB-only SensApp service and a Prometheus service together.
- Added `docker/prometheus-roleplay/prometheus.yml` with a `remote_read` target pointing at SensApp.
- Updated the roleplay README with the stack startup command and basic checks.
- Verified the stack locally by querying the known `roleplay_zeb_*` metrics through Prometheus.

## Validation

- `docker compose -f compose.prometheus-roleplay.yml up --build -d`: passed
- `cargo check`: passed when run with `CARGO_TARGET_DIR=/tmp/sensapp-target-validate`
- `cargo clippy --tests`: passed when run with `CARGO_TARGET_DIR=/tmp/sensapp-target-validate`
- `cargo test`: failed due existing PostgreSQL pool timeouts in `http::influxdb::tests::test_publish_influxdb`, `http::influxdb::tests::test_publish_influxdb_with_numeric_enabled`, and `http::server::tests::test_frontpage_handler`
