-- Create a view for sensor catalog that preserves nullable unit fields
CREATE VIEW sensor_catalog_view AS
SELECT
    s.sensor_id,
    s.uuid,
    s.name,
    s.type,
    u.name as unit_name,
    u.description as unit_description,
    jsonb_object_agg(lnd.name, ldd.description) as labels
FROM sensors s
LEFT JOIN units u ON s.unit = u.id
left join labels l on s.sensor_id = l.sensor_id
left join labels_name_dictionary lnd on l."name" = lnd.id
left join labels_description_dictionary ldd on l.description = ldd.id
group by s.sensor_id, s.uuid, s.name, s.type, u.name, u.description
order by s.sensor_id
