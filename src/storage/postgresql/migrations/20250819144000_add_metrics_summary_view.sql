-- Create a view for metrics summary that aggregates sensors by name
CREATE VIEW metrics_summary AS
SELECT
    s.name as metric_name,
    s.type,
    MIN(u.name) as unit_name,  -- Assuming all series of same metric have same unit
    MIN(u.description) as unit_description,
    COUNT(DISTINCT s.sensor_id) as series_count,
    ARRAY_AGG(DISTINCT lnd.name ORDER BY lnd.name) FILTER (WHERE lnd.name IS NOT NULL) as label_keys
FROM sensors s
LEFT JOIN units u ON s.unit = u.id
LEFT JOIN labels l ON s.sensor_id = l.sensor_id
LEFT JOIN labels_name_dictionary lnd ON l.name = lnd.id
GROUP BY s.name, s.type
ORDER BY s.name;
