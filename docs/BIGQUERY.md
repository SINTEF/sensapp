# BigQuery Storage Backend

SensApp can store its series in [BigQuery](https://cloud.google.com/bigquery). It exists for research and
development (comparing a warehouse with the time series databases, running SQL over sensor data next to other
datasets), not for production: it is **experimental**, kept simple on purpose, and the first thing to know is
that every query is billed. See [BACKENDS.md](BACKENDS.md) for how it compares to the others.

It is tested by hand: CI compiles it and runs its unit tests, but has no Google Cloud project (see
[Testing](#testing)).

## Connection string

```
bigquery://[key.json]?project_id=P&dataset_id=D[&location=europe-north1][&max_bytes_billed=1000000000]
```

| Part | Meaning |
|---|---|
| `key.json` | Path of a service account key (`bigquery:///abs/path/key.json` for an absolute one). Without it SensApp uses the Application Default Credentials: `GOOGLE_APPLICATION_CREDENTIALS`, what `gcloud auth application-default login` stored, or the identity of the Google Cloud machine it runs on |
| `project_id` | The project that runs and pays for the statements, and holds the dataset |
| `dataset_id` | Created at startup if it does not exist (in `location`, `europe-north1` by default); the tables are created with `CREATE TABLE IF NOT EXISTS`. **Use a dataset that is only SensApp's** |
| `location` | Where the dataset is created. Statements run where the dataset is, unless this says otherwise |
| `max_bytes_billed` | A statement that would bill more than this fails and is not charged. A good safety net for experiments: a statement bills at least 10 MB per table it reads, so a few hundred MB (`500000000`) to a few GB is realistic |

Unknown parameters are refused. The build needs the feature: `cargo build --features bigquery` (the container
image can include it, see the `FEATURES` build argument).

## Schema

Created by `src/storage/bigquery/migrations/init.sql`: `units`, `sensors`, `labels`, and one table per sample type
(`integer_values`, `numeric_values`, `float_values`, `string_values`, `boolean_values`, `location_values`,
`json_values`, `blob_values`). Sample tables are partitioned by month and clustered by `sensor_id`.

- **Ids** are derived from the data, like ClickHouse's: the series id is the UUID folded to 64 bits, the unit id a
  hash of its name (`storage::common`). BigQuery has no sequences and no unique keys, so this is what makes two
  writers that register the same series at once insert identical rows instead of two different ids. The reads
  collapse such rows.
- **Labels and strings are stored as text** in their rows, there are no dictionary tables. The datasets of the
  first version of the backend (with `labels_name_dictionary` and friends) are refused at startup with a message:
  use a new dataset.
- **Timestamps** are `TIMESTAMP`, written as microseconds since the epoch. BigQuery has a microsecond
  precision and accepts years 1 to 9999: nanoseconds are rounded down, and a query window is clamped to the range.
- **Values**: floats and locations are `FLOAT64` and exact. `NUMERIC` keeps **9** decimal digits (rounded to even
  when written), ClickHouse keeps 8. JSON is a `JSON` column: BigQuery normalizes it (the order of the keys is not
  kept). Blobs are `BYTES`.

## What is different

| | |
|---|---|
| **Writes** | The Storage Write API, default stream: rows are queryable at once, delivery is *at least once*. A request that timed out and is retried can leave **duplicate samples**, BigQuery cannot refuse them, and SensApp cannot remove them: there is no deduplication at ingestion (`SENSAPP_DEDUPLICATE_ON_INGEST` makes the server refuse to start) and the vacuum does not remove duplicates (`duplicates_removed` is `null`). There is no transaction across tables: a failure can leave a sensor's labels without its samples, never the opposite for a series that is listed |
| **Latency and cost** | Every statement is a job: 0.3 to 1 s even for one row, and a read of a tiny table bills 10 MB. A write looks up the new series (cached for 2 minutes per instance), writes up to 11 tables, one append each. Prefer large batches. Query results are never served from BigQuery's cache, so a read sees what was just written |
| **Deleting** | `DELETE` statements (rows of the Storage Write API can be deleted). BigQuery allows 2 mutating statements at a time per table and queues 20: deleting many series quickly fails with "Too many DML statements". A write to a series that another instance deleted, within 2 minutes, is stored without its sensor and is not seen |
| **Aggregation** | In BigQuery, for numeric series: `step` and `aggregation` are a `GROUP BY` of buckets (`AVG`, `MIN`, `MAX`, `SUM`, `COUNT`, first and last value), so only the buckets come back. The statement still **scans** the window of the series, billed by the bytes of the three columns it reads (about 24 bytes a sample, less once compressed): a 6 hour average over 6 months of one series reads 6 months of that series, and the monthly partitions and the clustering on `sensor_id` keep it to that. Selectors, with or without a `step`, read their series with one statement per numeric type. An average of `NUMERIC` is rounded to 9 digits |
| **Matchers** | `=`, `!=`, `=~`, `!~` on names and labels, with RE2 (`REGEXP_CONTAINS`, anchored). As on ClickHouse, `!=` and `!~` also select the series that do not have the label |
| **Reading a large result** | All the pages of the result are read (10 MB each), and a statement that outlasts its first answer is waited for (up to 5 minutes, then the request fails as unavailable) |
| **Errors** | Overload, quota errors that say "retry" and network failures are `503`; a rejected statement or write is `500` |
| **Not implemented** | Vacuum (nothing to do), removing duplicates, deduplication at ingestion |

## Setting up Google Cloud

You need a project with billing enabled (the BigQuery sandbox does not allow the DML and the Storage Write API
SensApp uses). A throwaway project is the easiest way to keep the cost and the permissions contained.

```bash
# 1. The project. The id is global, pick your own (or use an existing project and skip to 3)
PROJECT_ID=sensapp-test-$RANDOM
gcloud auth login
gcloud projects create "$PROJECT_ID" --name="SensApp test"   # inside an organization: add --organization=ORG_ID or --folder=FOLDER_ID

# 2. Billing (needs the Billing User role on the billing account)
gcloud billing accounts list
gcloud billing projects link "$PROJECT_ID" --billing-account=XXXXXX-XXXXXX-XXXXXX

# 3. The APIs
gcloud config set project "$PROJECT_ID"
gcloud services enable bigquery.googleapis.com bigquerystorage.googleapis.com

# 4. A service account that runs jobs and edits the data, and its key
gcloud iam service-accounts create sensapp-test --display-name="SensApp test"
SA="sensapp-test@$PROJECT_ID.iam.gserviceaccount.com"
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$SA" --role=roles/bigquery.jobUser
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$SA" --role=roles/bigquery.dataEditor
gcloud iam service-accounts keys create key.json --iam-account="$SA"

# 5. The dataset the tests will wipe (SensApp creates it when it is missing, in europe-north1 by default)
bq --project_id="$PROJECT_ID" mk --dataset --location=europe-north1 "$PROJECT_ID:sensapp_test"

# 6. A budget alert at 5 dollars (50%, 90% and 100% of it)
BILLING_ACCOUNT=XXXXXX-XXXXXX-XXXXXX
gcloud billing budgets create --billing-account="$BILLING_ACCOUNT" --display-name="SensApp test" \
  --budget-amount=5USD --filter-projects="projects/$PROJECT_ID" \
  --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0
```

Project-level roles keep the commands short; they are fine in a project that only exists for this. `bq` comes
with the Google Cloud CLI.

If the key command fails with `iam.disableServiceAccountKeyCreation` or `iam.managed.disableServiceAccountKeyCreation`
(a policy of your organization, the default of the ones created since 2024), do not look for a way around it: run as
yourself. Skip the key and leave it out of the connection string:

```bash
gcloud auth application-default login
unset GOOGLE_APPLICATION_CREDENTIALS
# TEST_DATABASE_URL='bigquery://?project_id=P&dataset_id=sensapp_test&max_bytes_billed=2000000000'
```

SensApp then reads `~/.config/gcloud/application_default_credentials.json` (or the file named by
`GOOGLE_APPLICATION_CREDENTIALS`). Only user credentials and service account keys are understood: an
`--impersonate-service-account` file is not. Your account needs `roles/bigquery.jobUser` on the project and
`roles/bigquery.dataEditor` on the dataset. A machine of Google Cloud (Compute Engine, Cloud Run, GKE) with a service
account attached needs neither a key nor a login. If a write fails with a 403 that talks about a quota project, try
`gcloud auth application-default set-quota-project P` and report it: the client does not send one itself.

Limit what a mistake can cost before the first run:

- In the console, **IAM & Admin → Quotas & System limits**, filter on "Query usage per day" (BigQuery API) and set a
  custom limit for the project, for instance 20 GiB. This is a hard stop.
- The budget alert of step 6 only emails, it does not stop anything: the quota above and `max_bytes_billed` do.
- Keep `max_bytes_billed` in the connection string.

Keep `key.json` out of the repository (`key.json` and `*-key.json` are in `.gitignore`). The paths in the
connection string are relative to where `cargo test` or `sensapp` runs, the root of the repository.

Prices change, look at [the BigQuery pricing page](https://cloud.google.com/bigquery/pricing): at the time of
writing the first TiB of queries and 10 GiB of storage per month are free, then queries are billed per TiB
scanned, and the Storage Write API has a free monthly allowance. SensApp's tables are small in tests: a whole
test run scans tens of GB at most, because each statement bills a 10 MB minimum.

## Testing

Unit tests need nothing (the SQL SensApp builds, the rows it sends, the sorting of errors, the connection string):

```bash
cargo make check-bigquery
```

The integration tests need the dataset. **They wipe it**: every test starts by deleting all the rows of all the
tables of the dataset, so use a dataset that holds nothing else, like `sensapp_test`.

```bash
export TEST_DATABASE_URL_BIGQUERY='bigquery://key.json?project_id=my-sensapp-test&dataset_id=sensapp_test&max_bytes_billed=2000000000'

# What is specific to BigQuery first: every type round trip, concurrent instances, paging, selectors, the cost cap
TEST_DATABASE_URL=$TEST_DATABASE_URL_BIGQUERY cargo test --no-default-features --features bigquery \
  --test integration bigquery_integration:: -- --nocapture

# Then the backend-generic suites on BigQuery, a few modules at a time
TEST_DATABASE_URL=$TEST_DATABASE_URL_BIGQUERY cargo test --no-default-features --features bigquery \
  --test integration data_lifecycle:: advanced_backend_queries:: query_sensors_by_labels:: selector_reads::

# Everything (long: every test is serial and starts with a cleanup of 11 statements)
cargo make test-bigquery-live
```

Without a `bigquery:` `TEST_DATABASE_URL` the BigQuery-specific tests print that they are skipped and pass.
The deduplication tests that need a backend that can deduplicate skip themselves, as on ClickHouse.
