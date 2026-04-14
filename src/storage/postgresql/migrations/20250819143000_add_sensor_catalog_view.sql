-- Create a view for sensor catalog that preserves nullable unit fields
CREATE VIEW sensor_catalog_view AS
SELECT
    s.sensor_id,
    s.uuid,
    s.name,
    s.type,
    u.name as unit_name,
    u.description as unit_description,
    label_data.labels
FROM sensors s
LEFT JOIN units u ON s.unit = u.id
LEFT JOIN LATERAL (
    SELECT jsonb_object_agg(lnd.name, ldd.description) as labels
    FROM labels l
    JOIN labels_name_dictionary lnd ON l."name" = lnd.id
    JOIN labels_description_dictionary ldd ON l.description = ldd.id
    WHERE l.sensor_id = s.sensor_id
) label_data ON true
ORDER BY s.sensor_id;
