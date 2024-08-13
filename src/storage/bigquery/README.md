# BigQuery and SensApp

We are usually not a fan of proprietary solutions, but BigQuery is worth a try.

Infortunately, while it uses some SQL flavour to query data, it's not very standard nor straightforward to use. It has vendor lock-in.

As the summer of 2024, it seems that we have many options to upload data to BigQuery:

- A JSON/REST API. We upload JSON (potentially compressed with Gzip).
- A Storage Write API that requires some protocol buffer binary and schema over gRPC.
- Upload a static file to Google Cloud Storage and then load it into BigQuery as a job.
- Use some big data pipeline tool like Apache Beam or Apache Spark that can then be connected to BigQuery.

The best option for now seems to be the Storage Write API with the `gcp-bigquery-client` crate.

## No denormalisation

BigQuery may benefit from data denormalisation, but we aren't doing it for now. It would be too different when comparing against the other databases. I'm also not convinced about the benefits of denormalisation for SensApp.

## Transactions

As far as I understood, BigQuery does not support transactions when ingesting data.

## No sequential IDs

BigQuery does not support sequential IDs. Querying the database to compute the next ID is not an option, as it's too slow and costly. Therefore, we use [sinteflake](https://crates.io/crates/sinteflake), a distributed unique ID generator inspired by Twitter's Snowflake.
