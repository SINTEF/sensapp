# The PostgreSQL and TimescaleDB test harness leaks a database per run

Decision (2 Oct 2026): **left as it is**, not worth teardown code.

The PostgreSQL and TimescaleDB test types (`tests/integration/common/mod.rs`) give every test run its own
`<database name>-NNNN` database (`sensapp-test-NNNN` with the default URLs) and nothing drops it. In CI the service container is thrown away, so only a
local TimescaleDB container accumulates them (about thirty after a day of runs). Isolation per run is what
makes concurrent runs safe, and a reliable teardown from a synchronous `Drop` is awkward.

To clean a local container, run `cargo make clean-test-databases` (it drops every `sensapp-test-*` database
of the `sensapp-timescaledb` container of `compose.test-services.yml`), or:

```bash
docker exec sensapp-timescaledb psql -U postgres -tAc \
  "SELECT 'DROP DATABASE \"' || datname || '\"' FROM pg_database WHERE datname LIKE 'sensapp-test-%'" \
  | docker exec -i sensapp-timescaledb psql -U postgres
```
