-- Indexes for the selectors. A label matcher finds the ids of its name and value in the
-- dictionaries, then the series that have both: `labels` was only indexed by series. A name
-- matcher looks the series up by name: a hash index, which has no limit on the length of the
-- name (a B-tree entry holds about 2.7 kB). Measured in `done/postgres-regex-validation-and-statement-timeout.md`.
--
-- No BEGIN or COMMIT here: sqlx runs each migration in a transaction of its own.

CREATE INDEX index_labels_name_description ON labels (name, description);
CREATE INDEX index_sensors_name ON sensors USING hash (name);
