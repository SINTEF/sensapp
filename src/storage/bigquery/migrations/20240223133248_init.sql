-- Create the 'units' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.units` (
    id INT64 NOT NULL,
    name STRING NOT NULL,
    description STRING
);

-- Create the 'sensors' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.sensors` (
    sensor_id INT64 NOT NULL,
    uuid STRING NOT NULL,
    name STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    type STRING NOT NULL,
    unit INT64,
);

-- Create the 'labels_name_dictionary' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.labels_name_dictionary` (
    id INT64 NOT NULL,
    name STRING NOT NULL
);

-- Create the 'labels_description_dictionary' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.labels_description_dictionary` (
    id INT64 NOT NULL,
    description STRING NOT NULL
);

-- Create the 'labels' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.labels` (
    sensor_id INT64 NOT NULL,
    name INT64 NOT NULL,
    description INT64
);

-- Create the 'strings_values_dictionary' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.strings_values_dictionary` (
    id INT64 NOT NULL,
    value STRING NOT NULL
);

-- Create the 'integer_values' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.integer_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value INT64 NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY sensor_id;

-- Create the 'numeric_values' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.numeric_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value NUMERIC NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY sensor_id;

-- Create the 'float_values' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.float_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value FLOAT64 NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY sensor_id;

-- Create the 'string_values' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.string_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value INT64 NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY sensor_id;

-- Create the 'boolean_values' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.boolean_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value BOOL NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY sensor_id;

-- Create the 'location_values' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.location_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    latitude FLOAT64 NOT NULL,
    longitude FLOAT64 NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY sensor_id;

-- Create the 'json_values' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.json_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value JSON NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY sensor_id;

-- Create the 'blob_values' table
CREATE TABLE IF NOT EXISTS `{dataset_id}.blob_values` (
    sensor_id INT64 NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value BYTES NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY sensor_id;



CREATE OR REPLACE VIEW `{dataset_id}.sensor_numeric_values` AS
SELECT
  s.sensor_id,
  s.uuid,
  s.name AS sensor_name,
  s.type AS sensor_type,
  s.unit,
  nv.timestamp,
  nv.value AS numeric_value
FROM
  `{dataset_id}.sensors` s
JOIN
  `{dataset_id}.numeric_values` nv
ON
  s.sensor_id = nv.sensor_id;

CREATE OR REPLACE VIEW `{dataset_id}.sensor_labels_view` AS
SELECT
  s.sensor_id,
  s.uuid,
  s.name AS sensor_name,
  s.type,
  s.unit,
  (
    SELECT JSON_OBJECT(
          ARRAY_AGG(lnd.name),
          ARRAY_AGG(ldd.description)
    )
    FROM `{dataset_id}.labels` l
    LEFT JOIN `{dataset_id}.labels_name_dictionary` lnd ON l.name = lnd.id
    LEFT JOIN `{dataset_id}.labels_description_dictionary` ldd ON l.description = ldd.id
    WHERE l.sensor_id = s.sensor_id
  ) AS labels
FROM
  `{dataset_id}.sensors` s;
