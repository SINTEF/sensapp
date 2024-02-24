## SensApp and InfluxDB

SensApp can be used instead of InfluxDB or aside it. InfluxDB is a time-series database that is widely used in the IoT and monitoring space. It is the most popular time-series database according to the [DB-Engines Ranking](https://db-engines.com/en/ranking/time+series+dbms).

Data sources can write data to SensApp as if it was InfluxDB. SensApp does **not** support writing data to InfluxDB, and does **not** support the InfluxDB query language or any other advanced InfluxDB features.

## How is the compatibility achieved?

SensApp is compatible with the [InfluxDB line protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/). It actually uses the [InfluxDB line protocol parser](https://crates.io/crates/influxdb-line-protocol) that InfluxDB v3 conveniently provides as a standalone Rust™️ crate/library.

SensApp exposes the same [InfluxDB v2 Writing API](https://docs.influxdata.com/influxdb/v2/api/#operation/PostWrite) as InfluxDB, so if your application is already writing data to InfluxDB, you can easily switch to SensApp by updating the URL and credentials.

The Writing API is the **only** compatible API.

## Using SensApp instead of InfluxDB

For writing data to SensApp, you can use the same API as InfluxDB v2. The only difference is the URL and the credentials.

If you use [Telegraf](https://www.influxdata.com/time-series-platform/telegraf/) for example, you can specify the SensAPP URL and credentials in the `[[outputs.influxdb_v2]]` section of the configuration file.

```toml
[[outputs.influxdb_v2]]
  urls = ["http://sensapp:3000"]
  token = "your-sensapp-token-if-needed"
  organization = "your-sensapp-org"
  bucket = "your-sensapp-bucket"
  content_encoding = "gzip"
  influx_uint_support = true
```

## Using SensApp and InfluxDB

You may prefer to keep using InfluxDB aside SensApp. InfluxDB performs pretty well for data with no long-term retention for example. You may also have applications or data pipelines relying on InfluxDB that you don't want to change.

In this case, you have a few options:

 - Update your data sources to write to both SensApp and InfluxDB.
 - Use an intermediate HTTP(s) server that provides an InfluxDB-compatible API and then writes to both SensApp and InfluxDB. [Telegraf](https://www.influxdata.com/time-series-platform/telegraf/) with its `inputs.influxdb_v2_listener` can be used for this purpose.
 - Use the [InfluxDB replication feature](https://docs.influxdata.com/influxdb/v2/write-data/replication/replicate-data/) to replicate data to SensApp.

### Using Telegraf as a proxy/replicator

```toml
[[inputs.influxdb_v2_listener]]
  service_address = ":8086"

[[outputs.influxdb_v2]]
  urls = ["http://influxdb:8086"]
  token = "your-influxdb-token"
  organization = "your-influxdb-org"
  bucket = "your-influxdb-bucket"
  content_encoding = "gzip"
  influx_uint_support = true

[[outputs.influxdb_v2]]
  urls = ["http://sensapp:3000"]
  token = "your-sensapp-token-if-needed"
  organization = "your-sensapp-org"
  bucket = "your-sensapp-bucket"
  content_encoding = "gzip"
  influx_uint_support = true
```

```bash
telegraf --config telegraf.conf
```

### How to Setup InfluxDB Replication to SensApp

Make sure that the remoteURL is reachable from the InfluxDB instance. If you run InfluxDB in Docker for example, remember that the container network is not the same as the host network by default.

Create the SensApp remote in InfluxDB:
```bash
curl --request POST \
  --url http://influxdb:8086/api/v2/remotes \
  --header 'Authorization: Bearer $YOUR_SECRET_TOKEN' \
  --header 'Content-Type: application/json' \
  --data '{
    "remoteURL": "http://sensapp:3000",
    "name": "sensapp-remote",
    "description": "Example of SensApp Remote",
    "orgID": "your-influxdb-org-id",
    "remoteOrgID": "remote-sensapp-org-id",
    "remoteAPIToken": "if-you-need-it"
}'
```
```json
{
  "id": "created-sensapp-remote-id",
  "orgID": "your-influxdb-org-id",
  "name": "sensapp-remote",
  "description": "Example of SensApp Remote",
  "remoteURL": "http://influxdb:3000",
  "remoteOrgID": "remote-sensapp-org-id",
  "allowInsecureTLS": false
}
```

Create a bucket replication to SensApp:
```bash
curl --request POST \
  --url http://influxdb:8086/api/v2/replications \
  --header 'Authorization: Bearer $YOUR_SECRET_TOKEN' \
  --header 'Content-Type: application/json' \
  --data '{
  "name": "sensapp-replication",
  "description": "Example of SensApp Replication",
  "localBucketID": "influxdb-bucket-id",
  "orgID": "your-influxdb-org-id",
  "remoteBucketName": "sensapp-bucket-name",
  "remoteBucketID": "sensapp-bucket-id",
  "remoteID": "created-sensapp-remote-id",
  "maxAgeSeconds": 604800
}'
```
```json
{
  "id": "created-sensapp-replication-id",
  "orgID": "your-influxdb-org-id",
  "name": "sensapp-replication",
  "description": "Example of SensApp Replication",
  "remoteID":"created-sensapp-remote-id",
  "localBucketID":"influxdb-bucket-id",
  "remoteBucketID":"sensapp-bucket-id",
  "RemoteBucketName": "",
  "maxQueueSizeBytes": 67108860,
  "currentQueueSizeBytes": 0,
  "remainingBytesToBeSynced": 0,
  "dropNonRetryableData": false,
  "maxAgeSeconds": 604800
}
```

Fetch the replication status:
```bash
curl --request GET \
  --url http://localhost:8086/api/v2/replications/created-sensapp-replication-id \
  --header 'Authorization: Bearer $YOUR_SECRET_TOKEN'
```
```json
{
  "id": "created-sensapp-replication-id",
  "orgID": "your-influxdb-org-id",
  "name": "sensapp-replication",
  "description": "Example of SensApp Replication",
  "remoteID":"created-sensapp-remote-id",
  "localBucketID":"influxdb-bucket-id",
  "remoteBucketID":"sensapp-bucket-id",
  "RemoteBucketName": "",
  "maxQueueSizeBytes": 67108860,
  "dropNonRetryableData": false,
  "maxAgeSeconds": 604800,

  // Important status fields:
  "currentQueueSizeBytes": 18894,
  "remainingBytesToBeSynced": 0,
  "latestResponseCode": 204,
  "latestErrorMessage": ""
}
