# ClickHouse lost deletes

`data_lifecycle::clickhouse::concurrent_delete_series` failed now and then (1 run in 6 here), and
`concurrent_publish_and_delete` about 1 run in 3. Every `delete_series` answered `true`, yet some series stayed
in `list_series`.

## Cause

Not a SensApp bug. ClickHouse 24.8.14 can lose a lightweight delete (a mutation) while background merges run on
the table: the mutation is `is_done`, there is no error in `system.part_log` or `system.mutations`, and the rows
stay for ever (checked after 5 s and in the merged part). It happened only on `sensors`, the table with a row
per insert and so the most merges, in the two runs inspected.

Reproduced without SensApp: a bare `ReplacingMergeTree`, 25 single-row inserts, 24 parallel
`DELETE FROM t WHERE sensor_id = ?`, then `SELECT count() FROM t FINAL`.

| Variant (100 runs, 30 for heavy)                  | Runs that lost a delete |
|---------------------------------------------------|-------------------------|
| lightweight `DELETE`                              | 8 of 100                |
| heavy `ALTER TABLE .. DELETE .. mutations_sync=2` | 3 of 30 (worse)         |
| lightweight `DELETE`, then check, then again      | 0 of 100 (7 needed a second attempt, never a third) |

## Fix

`ClickHouseStorage::delete_until_gone` (`src/storage/clickhouse/mod.rs`): delete, `SELECT count()` with the same
condition, delete again until it is 0, 5 attempts at most, then an error. Used by `delete_series` (every table)
and `delete_series_samples`. The delete is idempotent, so a repeat is safe, also with several instances.
Cost: one indexed `count()` per table.

## Result

Both tests, 40 runs each, ClickHouse 24.8.14: 40 of 40 pass (80 of 80), before the fix about 1 in 6 and 1 in 3
failed. Full ClickHouse suite: 322 integration and 296 unit tests pass, clippy clean.

Not done: reporting it to ClickHouse, or checking a newer server version. The check is cheap enough to keep
whatever the version.
