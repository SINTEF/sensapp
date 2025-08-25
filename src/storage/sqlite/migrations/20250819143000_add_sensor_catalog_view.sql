-- Create a view for sensor catalog that preserves nullable unit fields
CREATE VIEW sensor_catalog_view AS
SELECT
    s.sensor_id,
    s.uuid,
    s.name,
    s.type,
    u.name as unit_name,
    u.description as unit_description
FROM sensors s
LEFT JOIN units u ON s.unit = u.id;
