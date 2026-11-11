-- The label matchers look for a label name and value, and the name matchers for a sensor name:
-- without these indexes every matcher read the whole labels or sensors table
CREATE INDEX IF NOT EXISTS index_labels_name_description ON labels(name, description);
CREATE INDEX IF NOT EXISTS index_sensors_name ON sensors(name);
