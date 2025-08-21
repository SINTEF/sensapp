-- Migrate timestamp columns from milliseconds to microseconds
-- This migration converts all timestamp_ms columns to timestamp_us and multiplies existing data by 1000

-- SQLite doesn't support ALTER COLUMN, so we need to recreate tables
-- We'll do this in steps: rename old tables, create new tables, copy data, drop old tables

-- 1. Rename existing tables to temporary names
ALTER TABLE integer_values RENAME TO integer_values_old;
ALTER TABLE numeric_values RENAME TO numeric_values_old;
ALTER TABLE float_values RENAME TO float_values_old;
ALTER TABLE string_values RENAME TO string_values_old;
ALTER TABLE boolean_values RENAME TO boolean_values_old;
ALTER TABLE location_values RENAME TO location_values_old;
ALTER TABLE json_values RENAME TO json_values_old;
ALTER TABLE blob_values RENAME TO blob_values_old;

-- 2. Create new tables with timestamp_us columns
CREATE TABLE integer_values (
    sensor_id INTEGER NOT NULL,
    timestamp_us INTEGER NOT NULL, -- Unix timestamp in microseconds
    value INTEGER NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
) STRICT;

CREATE TABLE numeric_values (
    sensor_id INTEGER NOT NULL,
    timestamp_us INTEGER NOT NULL, -- Unix timestamp in microseconds
    value TEXT NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
) STRICT;

CREATE TABLE float_values (
    sensor_id INTEGER NOT NULL,
    timestamp_us INTEGER NOT NULL, -- Unix timestamp in microseconds
    value REAL NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
) STRICT;

CREATE TABLE string_values (
    sensor_id INTEGER NOT NULL,
    timestamp_us INTEGER NOT NULL, -- Unix timestamp in microseconds
    value INTEGER NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id),
    FOREIGN KEY (value) REFERENCES strings_values_dictionary(id)
) STRICT;

CREATE TABLE boolean_values (
    sensor_id INTEGER NOT NULL,
    timestamp_us INTEGER NOT NULL, -- Unix timestamp in microseconds
    value INTEGER NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
) STRICT;

CREATE TABLE location_values (
    sensor_id INTEGER NOT NULL,
    timestamp_us INTEGER NOT NULL, -- Unix timestamp in microseconds
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
) STRICT;

CREATE TABLE json_values (
    sensor_id INTEGER NOT NULL,
    timestamp_us INTEGER NOT NULL, -- Unix timestamp in microseconds
    value BLOB NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
) STRICT;

CREATE TABLE blob_values (
    sensor_id INTEGER NOT NULL,
    timestamp_us INTEGER NOT NULL, -- Unix timestamp in microseconds
    value BLOB NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
) STRICT;

-- 3. Copy data from old tables to new tables, converting milliseconds to microseconds
INSERT INTO integer_values (sensor_id, timestamp_us, value)
SELECT sensor_id, timestamp_ms * 1000, value FROM integer_values_old;

INSERT INTO numeric_values (sensor_id, timestamp_us, value)
SELECT sensor_id, timestamp_ms * 1000, value FROM numeric_values_old;

INSERT INTO float_values (sensor_id, timestamp_us, value)
SELECT sensor_id, timestamp_ms * 1000, value FROM float_values_old;

INSERT INTO string_values (sensor_id, timestamp_us, value)
SELECT sensor_id, timestamp_ms * 1000, value FROM string_values_old;

INSERT INTO boolean_values (sensor_id, timestamp_us, value)
SELECT sensor_id, timestamp_ms * 1000, value FROM boolean_values_old;

INSERT INTO location_values (sensor_id, timestamp_us, latitude, longitude)
SELECT sensor_id, timestamp_ms * 1000, latitude, longitude FROM location_values_old;

INSERT INTO json_values (sensor_id, timestamp_us, value)
SELECT sensor_id, timestamp_ms * 1000, value FROM json_values_old;

INSERT INTO blob_values (sensor_id, timestamp_us, value)
SELECT sensor_id, timestamp_ms * 1000, value FROM blob_values_old;

-- 4. Drop old tables
DROP TABLE integer_values_old;
DROP TABLE numeric_values_old;
DROP TABLE float_values_old;
DROP TABLE string_values_old;
DROP TABLE boolean_values_old;
DROP TABLE location_values_old;
DROP TABLE json_values_old;
DROP TABLE blob_values_old;

-- 5. Create indexes on the new timestamp_us columns
CREATE INDEX index_integer_values ON integer_values(sensor_id, timestamp_us);
CREATE INDEX index_numeric_values ON numeric_values(sensor_id, timestamp_us);
CREATE INDEX index_float_values ON float_values(sensor_id, timestamp_us);
CREATE INDEX index_string_values ON string_values(sensor_id, timestamp_us);
CREATE INDEX index_boolean_values ON boolean_values(sensor_id, timestamp_us);
CREATE INDEX index_location_values ON location_values(sensor_id, timestamp_us);
CREATE INDEX index_json_values ON json_values(sensor_id, timestamp_us);
CREATE INDEX index_blob_values ON blob_values(sensor_id, timestamp_us);
