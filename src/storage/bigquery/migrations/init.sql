-- SensApp schema for BigQuery. `{dataset}` is replaced by `project.dataset`.
--
-- No sequential ids and no unique keys in BigQuery: sensor and unit ids are derived from the UUID and
-- the name (see `storage::common`), so two writers registering the same sensor at once insert
-- identical rows, and the reads collapse them.
--
-- Samples are stored once per write: a retry after a timeout can leave a duplicate. BigQuery cannot
-- refuse it and SensApp does not remove it.

CREATE TABLE IF NOT EXISTS `{dataset}.units` (
    id INT64 NOT NULL,
    name STRING NOT NULL,
    description STRING
);

CREATE TABLE IF NOT EXISTS `{dataset}.sensors` (
    sensor_id INT64 NOT NULL,
    uuid STRING NOT NULL,
    name STRING NOT NULL,
    type STRING NOT NULL,
    unit INT64
)
CLUSTER BY sensor_id;

CREATE TABLE IF NOT EXISTS `{dataset}.labels` (
    sensor_id INT64 NOT NULL,
    name STRING NOT NULL,
    description STRING NOT NULL
)
CLUSTER BY sensor_id;

-- Monthly partitions: BigQuery refuses more than 10 000 partitions in a table, and a daily partition
-- would stop at 27 years of distinct days.
CREATE TABLE IF NOT EXISTS `{dataset}.integer_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value INT64 NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(timestamp, MONTH)
CLUSTER BY sensor_id;

CREATE TABLE IF NOT EXISTS `{dataset}.numeric_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value NUMERIC NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(timestamp, MONTH)
CLUSTER BY sensor_id;

CREATE TABLE IF NOT EXISTS `{dataset}.float_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value FLOAT64 NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(timestamp, MONTH)
CLUSTER BY sensor_id;

CREATE TABLE IF NOT EXISTS `{dataset}.string_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(timestamp, MONTH)
CLUSTER BY sensor_id;

CREATE TABLE IF NOT EXISTS `{dataset}.boolean_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value BOOL NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(timestamp, MONTH)
CLUSTER BY sensor_id;

CREATE TABLE IF NOT EXISTS `{dataset}.location_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    latitude FLOAT64 NOT NULL,
    longitude FLOAT64 NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(timestamp, MONTH)
CLUSTER BY sensor_id;

CREATE TABLE IF NOT EXISTS `{dataset}.json_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value JSON NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(timestamp, MONTH)
CLUSTER BY sensor_id;

CREATE TABLE IF NOT EXISTS `{dataset}.blob_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value BYTES NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(timestamp, MONTH)
CLUSTER BY sensor_id;
