-- Create sequences for tables with numerical primary keys
CREATE SEQUENCE IF NOT EXISTS units_id_seq;
CREATE SEQUENCE IF NOT EXISTS sensors_id_seq;
CREATE SEQUENCE IF NOT EXISTS labels_name_dictionary_id_seq;
CREATE SEQUENCE IF NOT EXISTS labels_description_dictionary_id_seq;
CREATE SEQUENCE IF NOT EXISTS strings_values_dictionary_id_seq;

-- Create the 'units' table
CREATE TABLE IF NOT EXISTS units (
    id BIGINT PRIMARY KEY DEFAULT nextval('units_id_seq'),
    name TEXT NOT NULL UNIQUE,
    description TEXT
);

-- Create the 'sensors' table with UUID and auto-incrementing sensor_id
CREATE TABLE IF NOT EXISTS sensors (
    sensor_id BIGINT PRIMARY KEY DEFAULT nextval('sensors_id_seq'),
    uuid UUID NOT NULL UNIQUE,
    name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    type TEXT NOT NULL,
    unit BIGINT,
    FOREIGN KEY (unit) REFERENCES units(id)
);

-- Create the 'labels_name_dictionary' table
CREATE TABLE IF NOT EXISTS labels_name_dictionary (
    id BIGINT PRIMARY KEY DEFAULT nextval('labels_name_dictionary_id_seq'),
    name TEXT NOT NULL UNIQUE
);

-- Create the 'labels_description_dictionary' table
CREATE TABLE IF NOT EXISTS labels_description_dictionary (
    id BIGINT PRIMARY KEY DEFAULT nextval('labels_description_dictionary_id_seq'),
    description TEXT NOT NULL UNIQUE
);

-- Create the 'labels' table
CREATE TABLE IF NOT EXISTS labels (
    sensor_id BIGINT NOT NULL,
    name BIGINT NOT NULL,
    description BIGINT,
    PRIMARY KEY (sensor_id, name),
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id),
    FOREIGN KEY (name) REFERENCES labels_name_dictionary(id),
    FOREIGN KEY (description) REFERENCES labels_description_dictionary(id),

    -- Unique constraint on (sensor_id, name)
    UNIQUE (sensor_id, name)
);

-- Create the 'strings_values_dictionary' table
CREATE TABLE IF NOT EXISTS strings_values_dictionary (
    id BIGINT PRIMARY KEY DEFAULT nextval('strings_values_dictionary_id_seq'),
    value TEXT NOT NULL UNIQUE
);

-- The rest of the tables remain the same as they don't have numerical primary keys

-- Create the 'integer_values' table
CREATE TABLE IF NOT EXISTS integer_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms TIMESTAMP_MS NOT NULL,
    value BIGINT NOT NULL,
    --FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'numeric_values' table
CREATE TABLE IF NOT EXISTS numeric_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms TIMESTAMP_MS NOT NULL,
    value DECIMAL(18,6) NOT NULL,
    --FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'float_values' table
CREATE TABLE IF NOT EXISTS float_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms TIMESTAMP_MS NOT NULL,
    value DOUBLE NOT NULL,
    --FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'string_values' table
CREATE TABLE IF NOT EXISTS string_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms TIMESTAMP_MS NOT NULL,
    value BIGINT NOT NULL,
    --FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'boolean_values' table
CREATE TABLE IF NOT EXISTS boolean_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms TIMESTAMP_MS NOT NULL,
    value BOOLEAN NOT NULL,
    --FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'location_values' table
CREATE TABLE IF NOT EXISTS location_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms TIMESTAMP_MS NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    --FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'json_values' table
CREATE TABLE IF NOT EXISTS json_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms TIMESTAMP_MS NOT NULL,
    value JSON NOT NULL,
    --FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'blob_values' table
CREATE TABLE IF NOT EXISTS blob_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms TIMESTAMP_MS NOT NULL,
    value BLOB NOT NULL,
    --FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create indexes (DuckDB automatically creates indexes for primary keys and unique constraints)
CREATE INDEX IF NOT EXISTS idx_units_name ON units (name);
CREATE INDEX IF NOT EXISTS idx_labels_name_dictionary_name ON labels_name_dictionary (name);
CREATE INDEX IF NOT EXISTS idx_labels_description_dictionary_description ON labels_description_dictionary (description);
CREATE INDEX IF NOT EXISTS idx_strings_values_dictionary_value ON strings_values_dictionary (value);

CREATE VIEW IF NOT EXISTS sensor_labels_view AS
SELECT sensors.uuid, sensors.created_at, sensors."name", type, units.name as unit, json_group_object(
	labels_name_dictionary."name",labels_description_dictionary."description"
) AS labels
FROM sensors
LEFT JOIN units on sensors.unit = units.id
LEFT JOIN Labels on sensors.sensor_id = labels.sensor_id
LEFT JOIN labels_name_dictionary on labels."name" = labels_name_dictionary."id"
LEFT JOIN labels_description_dictionary on labels.description = labels_description_dictionary.id
GROUP BY sensors."sensor_id", sensors.uuid, sensors.created_at, sensors."name", type, units.name
ORDER BY sensors.created_at ASC, sensors.uuid ASC;
