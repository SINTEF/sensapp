-- Migrate timestamp columns from milliseconds to microseconds
-- This migration converts all timestamp_ms columns to timestamp_us and multiplies existing data by 1000

-- Begin transaction to ensure atomicity
BEGIN;

-- 1. Add new timestamp_us columns alongside existing timestamp_ms columns
ALTER TABLE integer_values ADD COLUMN timestamp_us BIGINT;
ALTER TABLE numeric_values ADD COLUMN timestamp_us BIGINT;
ALTER TABLE float_values ADD COLUMN timestamp_us BIGINT;
ALTER TABLE string_values ADD COLUMN timestamp_us BIGINT;
ALTER TABLE boolean_values ADD COLUMN timestamp_us BIGINT;
ALTER TABLE location_values ADD COLUMN timestamp_us BIGINT;
ALTER TABLE json_values ADD COLUMN timestamp_us BIGINT;
ALTER TABLE blob_values ADD COLUMN timestamp_us BIGINT;

-- 2. Copy data from timestamp_ms to timestamp_us, converting milliseconds to microseconds
UPDATE integer_values SET timestamp_us = timestamp_ms * 1000;
UPDATE numeric_values SET timestamp_us = timestamp_ms * 1000;
UPDATE float_values SET timestamp_us = timestamp_ms * 1000;
UPDATE string_values SET timestamp_us = timestamp_ms * 1000;
UPDATE boolean_values SET timestamp_us = timestamp_ms * 1000;
UPDATE location_values SET timestamp_us = timestamp_ms * 1000;
UPDATE json_values SET timestamp_us = timestamp_ms * 1000;
UPDATE blob_values SET timestamp_us = timestamp_ms * 1000;

-- 3. Make the new columns NOT NULL
ALTER TABLE integer_values ALTER COLUMN timestamp_us SET NOT NULL;
ALTER TABLE numeric_values ALTER COLUMN timestamp_us SET NOT NULL;
ALTER TABLE float_values ALTER COLUMN timestamp_us SET NOT NULL;
ALTER TABLE string_values ALTER COLUMN timestamp_us SET NOT NULL;
ALTER TABLE boolean_values ALTER COLUMN timestamp_us SET NOT NULL;
ALTER TABLE location_values ALTER COLUMN timestamp_us SET NOT NULL;
ALTER TABLE json_values ALTER COLUMN timestamp_us SET NOT NULL;
ALTER TABLE blob_values ALTER COLUMN timestamp_us SET NOT NULL;

-- 4. Drop the old BRIN indexes
DROP INDEX IF EXISTS index_integer_values;
DROP INDEX IF EXISTS index_numeric_values;
DROP INDEX IF EXISTS index_float_values;
DROP INDEX IF EXISTS index_string_values;
DROP INDEX IF EXISTS index_boolean_values;
DROP INDEX IF EXISTS index_location_values;
DROP INDEX IF EXISTS index_json_values;
DROP INDEX IF EXISTS index_blob_values;

-- 5. Drop the old timestamp_ms columns
ALTER TABLE integer_values DROP COLUMN timestamp_ms;
ALTER TABLE numeric_values DROP COLUMN timestamp_ms;
ALTER TABLE float_values DROP COLUMN timestamp_ms;
ALTER TABLE string_values DROP COLUMN timestamp_ms;
ALTER TABLE boolean_values DROP COLUMN timestamp_ms;
ALTER TABLE location_values DROP COLUMN timestamp_ms;
ALTER TABLE json_values DROP COLUMN timestamp_ms;
ALTER TABLE blob_values DROP COLUMN timestamp_ms;

-- 6. Create new BRIN indexes on the timestamp_us columns
CREATE INDEX index_integer_values ON integer_values USING brin (sensor_id, timestamp_us) WITH (pages_per_range = 32);
CREATE INDEX index_numeric_values ON numeric_values USING brin (sensor_id, timestamp_us) WITH (pages_per_range = 32);
CREATE INDEX index_float_values ON float_values USING brin (sensor_id, timestamp_us) WITH (pages_per_range = 32);
CREATE INDEX index_string_values ON string_values USING brin (sensor_id, timestamp_us) WITH (pages_per_range = 32);
CREATE INDEX index_boolean_values ON boolean_values USING brin (sensor_id, timestamp_us) WITH (pages_per_range = 32);
CREATE INDEX index_location_values ON location_values USING brin (sensor_id, timestamp_us) WITH (pages_per_range = 32);
CREATE INDEX index_json_values ON json_values USING brin (sensor_id, timestamp_us) WITH (pages_per_range = 32);
CREATE INDEX index_blob_values ON blob_values USING brin (sensor_id, timestamp_us) WITH (pages_per_range = 32);

-- Commit the transaction
COMMIT;
