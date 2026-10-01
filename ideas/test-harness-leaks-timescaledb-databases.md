# The TimescaleDB test harness leaks a database per run

`DatabaseType::TimescaleDB` (`tests/integration/common/mod.rs`) uses `isolate_test_database_url` to
give every test run its own `sensapp-test-NNNN` database, created by `ensure_test_database_exists`, and
nothing ever drops them (`TestDb::cleanup` is empty). About thirty runs left about thirty databases
on the local TimescaleDB container (1 Oct 2026); in CI the service container is thrown away, so it
only shows locally.

Fix idea: drop the isolated database when the `TestDb` goes away (a `Drop` that spawns a short
runtime, or an explicit async `cleanup()` that the tests call), or drop the stale ones at the start
of a run.
