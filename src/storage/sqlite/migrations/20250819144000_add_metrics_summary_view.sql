-- Create a view for metrics summary that aggregates sensors by name
-- SQLite version uses GROUP_CONCAT instead of ARRAY_AGG
CREATE VIEW metrics_summary AS
SELECT
    s.name as metric_name,
    s.type,
    MIN(u.name) as unit_name,  -- Assuming all series of same metric have same unit
    MIN(u.description) as unit_description,
    COUNT(DISTINCT s.sensor_id) as series_count,
    GROUP_CONCAT(DISTINCT lnd.name) as label_keys
FROM sensors s
LEFT JOIN units u ON s.unit = u.id
LEFT JOIN labels l ON s.sensor_id = l.sensor_id
LEFT JOIN labels_name_dictionary lnd ON l.name = lnd.id
WHERE lnd.name IS NOT NULL OR l.sensor_id IS NULL
GROUP BY s.name, s.type;
