-- Create a view that denormalizes sensor labels for easier querying
-- This view makes Prometheus matcher queries much simpler by joining
-- sensors, labels, and dictionaries in one convenient view
CREATE VIEW sensor_labels_view AS
SELECT 
    s.sensor_id,
    s.uuid as sensor_uuid,
    s.name as sensor_name,
    s.type as sensor_type,
    lnd.name as label_name,
    ldd.description as label_value
FROM sensors s
JOIN labels l ON s.sensor_id = l.sensor_id
JOIN labels_name_dictionary lnd ON l.name = lnd.id
LEFT JOIN labels_description_dictionary ldd ON l.description = ldd.id;

-- Add an index to optimize Prometheus label matching queries
-- This will speed up queries by label_name and label_value
CREATE INDEX idx_sensor_labels_view_lookup 
ON labels (name, description);

-- Also add an index specifically for the sensor name (Prometheus __name__ label)
-- This is frequently used in Prometheus queries
CREATE INDEX idx_sensors_name ON sensors (name);