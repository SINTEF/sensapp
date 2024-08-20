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
SELECT sensors.uuid, sensors.created_at, sensors.name, type, units.name as unit, JSON_OBJECT(
	ARRAY_AGG(labels_name_dictionary.name), ARRAY_AGG(labels_description_dictionary.description)
) AS labels
FROM `{dataset_id}.sensors` as sensors
LEFT JOIN `{dataset_id}.units` as units on sensors.unit = units.id
LEFT JOIN `{dataset_id}.labels`  as labels on sensors.sensor_id = labels.sensor_id
LEFT JOIN `{dataset_id}.labels_name_dictionary` as labels_name_dictionary on labels.name = labels_name_dictionary.id
LEFT JOIN `{dataset_id}.labels_description_dictionary` as labels_description_dictionary on labels.description = labels_description_dictionary.id
GROUP BY sensors.sensor_id, sensors.uuid, sensors.created_at, sensors.name, type, units.name
ORDER BY sensors.created_at ASC, sensors.uuid ASC;
