CREATE VIEW sensor_labels_view AS
SELECT sensors.uuid, sensors.created_at, sensors."name", type, units.name as unit, jsonb_object_agg(
	labels_name_dictionary."name",labels_description_dictionary."description"
) AS labels
FROM sensors
LEFT JOIN units on sensors.unit = units.id
LEFT JOIN Labels on sensors.sensor_id = labels.sensor_id
LEFT JOIN labels_name_dictionary on labels."name" = labels_name_dictionary."id"
LEFT JOIN labels_description_dictionary on labels.description = labels_description_dictionary.id
GROUP BY sensors."sensor_id", sensors.uuid, sensors.created_at, sensors."name", type, units.name
ORDER BY sensors.created_at ASC, sensors.uuid ASC;


set time zone UTC;

select to_timestamp('1723802801');
select to_timestamp('1723802801')::timestamp at time zone 'UTC';
select uuid('36f7a077-4f89-8e94-8ca3-7d7ffaba85be');

select *
from sensor_labels_view
where (created_at, uuid) > (to_timestamp('1723802801'), uuid('36f7a077-4f89-8e94-8ca3-7d7ffaba85be'))
order by created_at ASC, uuid ASC
limit 1;

select to_timestamp('1723802801'),*
from sensor_labels_view
where created_at > to_timestamp('1723802801')
order by created_at ASC, uuid ASC
limit 1;


SELECT sensors.uuid, sensors.created_at, sensors."name", type, units.name as unit, CASE WHEN COUNT(labels.sensor_id) = 0 then '{}' else jsonb_object_agg(
	COALESCE(labels_name_dictionary."name",'whatever_this_is_a_bug_workaround'),labels_description_dictionary."description")
	 end AS labels
FROM sensors
LEFT JOIN units on sensors.unit = units.id
LEFT JOIN Labels on sensors.sensor_id = labels.sensor_id
LEFT JOIN labels_name_dictionary on labels."name" = labels_name_dictionary."id"
LEFT JOIN labels_description_dictionary on labels.description = labels_description_dictionary.id
GROUP BY sensors."sensor_id", sensors.uuid, sensors.created_at, sensors."name", type, units.name
ORDER BY sensors.created_at ASC, sensors.uuid ASC;


explain
SELECT
	*
FROM
	sensors
WHERE
	EXISTS (
		SELECT
			1
		FROM
			labels
		LEFT JOIN labels_name_dictionary ON labels. "name" = labels_name_dictionary.id
		LEFT JOIN labels_description_dictionary ON labels.description = labels_description_dictionary.id
	WHERE
		labels_name_dictionary. "name" = 'instance'
		AND labels_description_dictionary.description ~ '.*host.*'
		AND labels.sensor_id = sensors.sensor_id)
	AND EXISTS (
		SELECT
			1
		FROM
			labels
		LEFT JOIN labels_name_dictionary ON labels. "name" = labels_name_dictionary.id
		LEFT JOIN labels_description_dictionary ON labels.description = labels_description_dictionary.id
	WHERE
		labels_name_dictionary. "name" = 'job'
		AND labels_description_dictionary.description = 'prometheus'
		AND labels.sensor_id = sensors.sensor_id);

explain
SELECT
    sensors.*
FROM
    sensors
JOIN
    labels l1 ON l1.sensor_id = sensors.sensor_id
JOIN
    labels_name_dictionary lnd1 ON l1."name" = lnd1.id
JOIN
    labels_description_dictionary ldd1 ON l1.description = ldd1.id
JOIN
    labels l2 ON l2.sensor_id = sensors.sensor_id
JOIN
    labels_name_dictionary lnd2 ON l2."name" = lnd2.id
JOIN
    labels_description_dictionary ldd2 ON l2.description = ldd2.id
WHERE
    lnd1."name" = 'instance'
    AND ldd1.description ~ '.*host.*'
    AND lnd2."name" = 'job'
    AND ldd2.description = 'prometheus';


SELECT
    *
FROM
    sensors
WHERE
    EXISTS (
        SELECT
            1
        FROM
            labels
        LEFT JOIN
            labels_name_dictionary ON labels.name = labels_name_dictionary.id
        LEFT JOIN
            labels_description_dictionary ON labels.description = labels_description_dictionary.id
        WHERE
            labels.sensor_id = sensors.sensor_id
        GROUP BY
            labels.sensor_id
        HAVING
            SUM(
                CASE
                    WHEN labels_name_dictionary.name = 'instance' AND labels_description_dictionary.description ~ '.*host.*'
                    THEN 1
                    ELSE 0
                END
            ) > 0
        AND
            SUM(
                CASE
                    WHEN labels_name_dictionary.name = 'job' AND labels_description_dictionary.description = 'prometheus'
                    THEN 1
                    ELSE 0
                END
            ) > 0
    );


WITH instance_labels AS (
    SELECT
        sensor_id
    FROM
        labels
    LEFT JOIN
        labels_name_dictionary ON labels.name = labels_name_dictionary.id
    LEFT JOIN
        labels_description_dictionary ON labels.description = labels_description_dictionary.id
    WHERE
        labels_name_dictionary.name = 'instance'
        AND labels_description_dictionary.description ~ '.*host.*'
),
job_labels AS (
    SELECT
        sensor_id
    FROM
        labels
    LEFT JOIN
        labels_name_dictionary ON labels.name = labels_name_dictionary.id
    LEFT JOIN
        labels_description_dictionary ON labels.description = labels_description_dictionary.id
    WHERE
        labels_name_dictionary.name = 'job'
        AND labels_description_dictionary.description = 'prometheus'
)
SELECT
    *
FROM
    sensors
WHERE
    sensor_id IN (SELECT sensor_id FROM instance_labels)
    AND sensor_id IN (SELECT sensor_id FROM job_labels);

explain
WITH filtered_sensors AS (
    SELECT
        sensors.sensor_id
    FROM
        sensors
    WHERE
        not EXISTS (
            SELECT
                1
            FROM
                labels
            LEFT JOIN labels_name_dictionary ON labels."name" = labels_name_dictionary.id
            LEFT JOIN labels_description_dictionary ON labels.description = labels_description_dictionary.id
            WHERE
                labels_name_dictionary."name" = 'instance'
                AND labels_description_dictionary.description ~ '.*host.*'
                AND labels.sensor_id = sensors.sensor_id
        )
        AND not EXISTS (
            SELECT
                1
            FROM
                labels
            LEFT JOIN labels_name_dictionary ON labels."name" = labels_name_dictionary.id
            LEFT JOIN labels_description_dictionary ON labels.description = labels_description_dictionary.id
            WHERE
                labels_name_dictionary."name" = 'influxdb_org'
                AND labels_description_dictionary.description ~ '.*zut.*'
                AND labels.sensor_id = sensors.sensor_id
        )
)
SELECT
	sensors.sensor_id,
    sensors.uuid,
    sensors.created_at,
    sensors."name",
    type,
    units.name as unit,
    CASE
        WHEN COUNT(labels.sensor_id) = 0 THEN '{}'
        ELSE jsonb_object_agg(
            COALESCE(labels_name_dictionary."name", 'whatever_this_is_a_bug_workaround'),
            labels_description_dictionary."description"
        )
    END AS labels
FROM
    sensors
LEFT JOIN units ON sensors.unit = units.id
LEFT JOIN labels ON sensors.sensor_id = labels.sensor_id
LEFT JOIN labels_name_dictionary ON labels."name" = labels_name_dictionary."id"
LEFT JOIN labels_description_dictionary ON labels.description = labels_description_dictionary.id
WHERE
    sensors.sensor_id IN (SELECT sensor_id FROM filtered_sensors)
GROUP BY
    sensors.sensor_id,
    sensors.uuid,
    sensors.created_at,
    sensors."name",
    type,
    units.name
ORDER BY
    sensors.created_at ASC,
    sensors.uuid ASC;


explain
SELECT
    sensors.sensor_id,
    sensors.uuid,
    sensors.created_at,
    sensors."name",
    type,
    units.name AS unit,
    CASE
        WHEN COUNT(labels.sensor_id) = 0 THEN '{}'
        ELSE jsonb_object_agg(
            COALESCE(labels_name_dictionary."name", 'whatever_this_is_a_bug_workaround'),
            labels_description_dictionary."description"
        )
    END AS labels
FROM
    sensors
LEFT JOIN units ON sensors.unit = units.id
LEFT JOIN labels ON sensors.sensor_id = labels.sensor_id
LEFT JOIN labels_name_dictionary ON labels."name" = labels_name_dictionary.id
LEFT JOIN labels_description_dictionary ON labels.description = labels_description_dictionary.id
GROUP BY
    sensors.sensor_id,
    sensors.uuid,
    sensors.created_at,
    sensors."name",
    type,
    units.name
HAVING
    SUM(
        CASE
            WHEN labels_name_dictionary.name = 'instance'
            AND labels_description_dictionary.description ~ '.*host.*'
            THEN 1
            ELSE 0
        END
    ) = 0
AND
    SUM(
        CASE
            WHEN labels_name_dictionary.name = 'influxdb_org'
            AND labels_description_dictionary.description ~ '.*zut.*'
            THEN 1
            ELSE 0
        END
    ) = 0
ORDER BY
    sensors.created_at ASC,
    sensors.uuid ASC;


SELECT
	sensor_id
FROM
	labels
	LEFT JOIN labels_name_dictionary ON labels.name = labels_name_dictionary.id
	LEFT JOIN labels_description_dictionary ON labels.description = labels_description_dictionary.id
WHERE
	labels_name_dictionary.name = 'instance'
	AND labels_description_dictionary.description ~ '.*host.*'
INTERSECT
SELECT
	sensor_id
FROM
	labels
	LEFT JOIN labels_name_dictionary ON labels.name = labels_name_dictionary.id
	LEFT JOIN labels_description_dictionary ON labels.description = labels_description_dictionary.id
WHERE
	labels_name_dictionary.name = 'job'
	AND labels_description_dictionary.description = 'prometheus';


select *
from sensors
where name LIKE 'go%';

CREATE VIEW sensor_labels_view AS
SELECT sensors.sensor_id, sensors.uuid, sensors.created_at, sensors."name", type, units.name as unit,
CASE WHEN COUNT(labels.sensor_id) = 0 THEN '{}' ELSE jsonb_object_agg(
	COALESCE(labels_name_dictionary."name",'whatever_this_is_a_bug_workaround'),labels_description_dictionary."description")
END AS labels
FROM sensors
LEFT JOIN units on sensors.unit = units.id
LEFT JOIN Labels on sensors.sensor_id = labels.sensor_id
LEFT JOIN labels_name_dictionary on labels."name" = labels_name_dictionary."id"
LEFT JOIN labels_description_dictionary on labels.description = labels_description_dictionary.id
GROUP BY sensors."sensor_id", sensors.uuid, sensors.created_at, sensors."name", type, units.name
ORDER BY sensors.created_at ASC, sensors.uuid ASC;
