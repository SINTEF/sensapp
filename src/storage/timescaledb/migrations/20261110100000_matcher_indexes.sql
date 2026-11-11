-- Indexes for the selectors and the reads of a series. A label matcher finds the ids of its name
-- and value in the dictionaries, then the series that have both: `labels` was only indexed by
-- series. A name matcher looks the series up by name: a hash index, which has no limit on the
-- length of the name (a B-tree entry holds about 2.7 kB). The hypertables only had the index on
-- time that TimescaleDB makes, so reading one series in an uncompressed chunk went through the
-- rows of every series. Compressed chunks are segmented by series and do not need it. Measured
-- in `done/postgres-regex-validation-and-statement-timeout.md`.
--
-- No BEGIN or COMMIT here: sqlx runs each migration in a transaction of its own.

CREATE INDEX index_labels_name_description ON labels (name, description);
CREATE INDEX index_sensors_name ON sensors USING hash (name);

CREATE INDEX index_integer_values_sensor_time ON integer_values (sensor_id, time DESC);
CREATE INDEX index_numeric_values_sensor_time ON numeric_values (sensor_id, time DESC);
CREATE INDEX index_float_values_sensor_time ON float_values (sensor_id, time DESC);
CREATE INDEX index_string_values_sensor_time ON string_values (sensor_id, time DESC);
CREATE INDEX index_boolean_values_sensor_time ON boolean_values (sensor_id, time DESC);
CREATE INDEX index_location_values_sensor_time ON location_values (sensor_id, time DESC);
CREATE INDEX index_json_values_sensor_time ON json_values (sensor_id, time DESC);
CREATE INDEX index_blob_values_sensor_time ON blob_values (sensor_id, time DESC);
