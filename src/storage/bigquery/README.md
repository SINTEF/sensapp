# BigQuery backend

User documentation: [docs/BIGQUERY.md](../../../docs/BIGQUERY.md) (connection string, differences, Google Cloud setup,
tests). Notes for whoever changes the code:

| File | What |
|---|---|
| `connection.rs` | The connection string and the validation of the identifiers that end up in backticks |
| `client.rs` | Parameters, `run_query` (waits for the job, reads every page, DML counts), the sorting of errors |
| `rows.rs` | The protobuf rows of the Storage Write API and their descriptors, checked against `migrations/init.sql` by a test |
| `publishers.rs` | Registration of new series and the write of a batch (one append per table, answers checked) |
| `reads.rs` | Sensors, labels and samples, the SQL of the sets that collapse duplicate registrations |
| `matchers.rs` | Label matchers as parameterized SQL |
| `selector.rs` | `BulkSelectorBackend`: a selector with a few queries |
| `mod.rs` | The `StorageInstance` implementation |

Rules the code relies on:

- **Never put a value in a statement**: values go in `@parameters`. Only the project, dataset and table names
  (validated, quoted) and numbers (`LIMIT`) are formatted into SQL.
- **Registration is idempotent**: ids come from the data, so concurrent writers insert identical rows. Reads of
  `sensors`, `units` and `labels` must collapse duplicates (`GROUP BY`, `DISTINCT`), see `Dataset::sensors_set`.
- **A response of the Storage Write API is not a success until its `error` and `row_errors` are empty.**
- **A result is not read until the job is complete and every page is read** (`ResultSet::new_from_query_response`
  returns zero rows for an unfinished job).
- The ids and the types of the columns are an on-disk format, like ClickHouse's.
