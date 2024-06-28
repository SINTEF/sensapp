# BigQuery and SensApp.

We are usually not a fan of proprietary solutions, but BigQuery is worth a try.

Infortunately, while it uses some SQL flavour to query data, it's not very standard nor straightforward to use. It smells like vendor lock-in.

As the spring of 2024, it seems that we have many options to upload data to BigQuery, and none uses SQL.

 - A JSON/REST API. We upload JSON (potentially compressed with Gzip).
 - A Storage Write API that requires some protocol buffer binary and schema over gRPC.
 - Upload a static file to Google Cloud Storage and then load it into BigQuery as a job.
 - Use some big data pipeline tool like Apache Beam or Apache Spark that can then be connected to BigQuery.

In our experience, compressed JSON is more compact than non-compressed protocol buffer, and compressed protocol buffer is not worth the hassle compared to compressed JSON.

It's unclear whether the few Rust clients that use gRPC do compress the protocol-buffer exchanges.

Overall, the JSON/REST API seems the best option for now.

We may explore faster options in the future, but we should go a long way with the gzip on the JSON/REST API.
