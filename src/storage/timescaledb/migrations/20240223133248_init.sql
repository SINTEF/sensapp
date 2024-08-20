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
    time TIMESTAMPTZ NOT NULL,
    value BIGINT NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'numeric_values' table
CREATE TABLE numeric_values (
    sensor_id BIGINT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    value NUMERIC NOT NULL, -- Assuming precision and scale are not specified; adjust as needed
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'float_values' table
CREATE TABLE float_values (
    sensor_id BIGINT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'string_values' table
CREATE TABLE string_values (
    sensor_id BIGINT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    value BIGINT NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
    -- foreign key disabled to allow the hypertable performance boost
    -- FOREIGN KEY (value) REFERENCES strings_values_dictionary(id)
);

-- Create the 'boolean_values' table
CREATE TABLE boolean_values (
    sensor_id BIGINT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    value BOOLEAN NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'location_values' table
CREATE TABLE location_values (
    sensor_id BIGINT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'json_values' table
CREATE TABLE json_values (
    sensor_id BIGINT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    value JSONB NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- Create the 'blob_values' table
CREATE TABLE blob_values (
    sensor_id BIGINT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    value BYTEA NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
);

-- B-Tree Indexes for dictionary tables
CREATE INDEX index_units_name ON units USING btree (name);

CREATE INDEX index_labels_name_dictionary_name ON labels_name_dictionary USING btree (name);
CREATE INDEX index_labels_description_dictionary_description ON labels_description_dictionary USING btree (description);
CREATE INDEX index_strings_values_dictionary_value ON strings_values_dictionary USING btree (value);

-- BRIN Indexes for time-series tables (adjust pages_per_range as per your data characteristics)
--CREATE INDEX index_integer_values ON integer_values USING brin (sensor_id, time) WITH (pages_per_range = 32);
--CREATE INDEX index_numeric_values ON numeric_values USING brin (sensor_id, time) WITH (pages_per_range = 32);
--CREATE INDEX index_float_values ON float_values USING brin (sensor_id, time) WITH (pages_per_range = 32);
--CREATE INDEX index_string_values ON string_values USING brin (sensor_id, time) WITH (pages_per_range = 32);
--CREATE INDEX index_boolean_values ON boolean_values USING brin (sensor_id, time) WITH (pages_per_range = 32);
--CREATE INDEX index_location_values ON location_values USING brin (sensor_id, time) WITH (pages_per_range = 32);
--CREATE INDEX index_json_values ON json_values USING brin (sensor_id, time) WITH (pages_per_range = 32);
--CREATE INDEX index_blob_values ON blob_values USING brin (sensor_id, time) WITH (pages_per_range = 32);

SELECT create_hypertable('integer_values', by_range('time', INTERVAL '7 days'));
ALTER TABLE integer_values SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id'
);
SELECT add_compression_policy('integer_values', INTERVAL '7 days');
SELECT add_dimension('integer_values', by_hash('sensor_id', 2));

SELECT create_hypertable('numeric_values', by_range('time', INTERVAL '7 days'));
ALTER TABLE numeric_values SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id'
);
SELECT add_compression_policy('numeric_values', INTERVAL '7 days');
SELECT add_dimension('numeric_values', by_hash('sensor_id', 2));

SELECT create_hypertable('float_values', by_range('time', INTERVAL '7 days'));
ALTER TABLE float_values SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id'
);
SELECT add_compression_policy('float_values', INTERVAL '7 days');
SELECT add_dimension('float_values', by_hash('sensor_id', 2));

SELECT create_hypertable('string_values', by_range('time', INTERVAL '7 days'));
ALTER TABLE string_values SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id'
);
SELECT add_compression_policy('string_values', INTERVAL '7 days');
SELECT add_dimension('string_values', by_hash('sensor_id', 2));

SELECT create_hypertable('boolean_values', by_range('time', INTERVAL '7 days'));
ALTER TABLE boolean_values SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id'
);
SELECT add_compression_policy('boolean_values', INTERVAL '7 days');
SELECT add_dimension('boolean_values', by_hash('sensor_id', 2));

SELECT create_hypertable('location_values', by_range('time', INTERVAL '7 days'));
ALTER TABLE location_values SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id'
);
SELECT add_compression_policy('location_values', INTERVAL '7 days');
SELECT add_dimension('location_values', by_hash('sensor_id', 2));

SELECT create_hypertable('json_values', by_range('time', INTERVAL '7 days'));
ALTER TABLE json_values SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id'
);
SELECT add_compression_policy('json_values', INTERVAL '7 days');
SELECT add_dimension('json_values', by_hash('sensor_id', 2));

SELECT create_hypertable('blob_values', by_range('time', INTERVAL '7 days'));
ALTER TABLE blob_values SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sensor_id'
);
SELECT add_compression_policy('blob_values', INTERVAL '7 days');
SELECT add_dimension('blob_values', by_hash('sensor_id', 2));

CREATE VIEW sensor_labels_view AS
SELECT sensors.uuid, sensors.created_at, sensors."name", type, units.name as unit, jsonb_object_agg(
	labels_name_dictionary."name",labels_description_dictionary."description"
) AS labels
FROM sensors
LEFT JOIN units on sensors.unit = units.id
LEFT JOIN Labels on sensors.sensor_id = labels.sensor_id
LEFT JOIN labels_name_dictionary on labels."name" = labels_name_dictionary."id"
LEFT JOIN labels_description_dictionary on labels.description = labels_description_dictionary.id
GROUP BY sensors."sensor_id", sensors.uuid, sensors.created_at, sensors."name", type, units.name
ORDER BY sensors.created_at ASC, sensors.uuid ASC;
