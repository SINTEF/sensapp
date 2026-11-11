# DuckDB: `latest` returns nothing while the newest samples are being deleted

Found on 11 November 2026 with `latest_finds_the_newest_remaining_sample_while_the_newest_ones_are_deleted`
(`tests/integration/latest_under_delete.rs`). The newest sample is read in two statements on every backend.
SQLite, PostgreSQL, TimescaleDB and ClickHouse now try again when the samples of the newest time were deleted
between the statements (`LATEST_ATTEMPTS`), and the test passes on them (PostgreSQL 15 runs of 15, one attempt
failed 3 of 3).

On DuckDB the same retry did not help: the test still failed, from read 2 to read 54 of a run, with 86 to 143
samples left. The reads go through `self.readers`, connections of their own, so the cause may be what they see
of the writer's deletes (a stale snapshot, or a checkpoint) rather than the window between the two statements.
Look at that before copying the retry. The test skips DuckDB until then.
