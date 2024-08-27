-- Create the 'units' table
CREATE TABLE units (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT
);

-- Create the 'sensors' table with UUID and auto-incrementing sensor_id
CREATE TABLE sensors (
    sensor_id BIGSERIAL PRIMARY KEY,
    uuid UUID NOT NULL UNIQUE,
    name TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    type TEXT NOT NULL,
    unit BIGINT,
    FOREIGN KEY (unit) REFERENCES units(id)
);

-- Create the 'labels_name_dictionary' table
CREATE TABLE labels_name_dictionary (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Create the 'labels_description_dictionary' table
CREATE TABLE labels_description_dictionary (
    id BIGSERIAL PRIMARY KEY,
    description TEXT NOT NULL UNIQUE
);


-- Create the 'labels' table
CREATE TABLE labels (
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
CREATE TABLE strings_values_dictionary (
    id BIGSERIAL PRIMARY KEY,
    value TEXT NOT NULL UNIQUE
);

-- Create the 'integer_values' table
CREATE TABLE integer_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    value BIGINT NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'numeric_values' table
CREATE TABLE numeric_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    value NUMERIC NOT NULL, -- Assuming precision and scale are not specified; adjust as needed
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'float_values' table
CREATE TABLE float_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'string_values' table
CREATE TABLE string_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    value BIGINT NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id),
    FOREIGN KEY (value) REFERENCES strings_values_dictionary(id)
);

-- Create the 'boolean_values' table
CREATE TABLE boolean_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    value BOOLEAN NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'location_values' table
CREATE TABLE location_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'json_values' table
CREATE TABLE json_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    value JSONB NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'blob_values' table
CREATE TABLE blob_values (
    sensor_id BIGINT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    value BYTEA NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- B-Tree Indexes for dictionary tables
CREATE INDEX index_units_name ON units USING btree (name);

CREATE INDEX index_labels_name_dictionary_name ON labels_name_dictionary USING btree (name);
CREATE INDEX index_labels_description_dictionary_description ON labels_description_dictionary USING btree (description);
CREATE INDEX index_strings_values_dictionary_value ON strings_values_dictionary USING btree (value);

-- BRIN Indexes for time-series tables (adjust pages_per_range as per your data characteristics)
CREATE INDEX index_integer_values ON integer_values USING brin (sensor_id, timestamp_ms) WITH (pages_per_range = 32);
CREATE INDEX index_numeric_values ON numeric_values USING brin (sensor_id, timestamp_ms) WITH (pages_per_range = 32);
CREATE INDEX index_float_values ON float_values USING brin (sensor_id, timestamp_ms) WITH (pages_per_range = 32);
CREATE INDEX index_string_values ON string_values USING brin (sensor_id, timestamp_ms) WITH (pages_per_range = 32);
CREATE INDEX index_boolean_values ON boolean_values USING brin (sensor_id, timestamp_ms) WITH (pages_per_range = 32);
CREATE INDEX index_location_values ON location_values USING brin (sensor_id, timestamp_ms) WITH (pages_per_range = 32);
CREATE INDEX index_json_values ON json_values USING brin (sensor_id, timestamp_ms) WITH (pages_per_range = 32);
CREATE INDEX index_blob_values ON blob_values USING brin (sensor_id, timestamp_ms) WITH (pages_per_range = 32);

CREATE VIEW sensor_labels_view AS
SELECT sensors.sensor_id, sensors.uuid, sensors.created_at, sensors."name", type, units.name as unit,
CASE WHEN COUNT(labels.sensor_id) = 0 THEN '{}' ELSE jsonb_object_agg(
	COALESCE(labels_name_dictionary."name",'whatever_this_is_a_bug_workaround'),labels_description_dictionary."description")
END AS labels
FROM sensors
LEFT JOIN units on sensors.unit = units.id
LEFT JOIN Labels on sensors.sensor_id = labels.sensor_id
LEFT JOIN labels_name_dictionary on labels."name" = labels_name_dictionary."id"
LEFT JOIN labels_description_dictionary on labels.description = labels_description_dictionary.id
GROUP BY sensors."sensor_id", sensors.uuid, sensors.created_at, sensors."name", type, units.name
ORDER BY sensors.created_at ASC, sensors.uuid ASC;
