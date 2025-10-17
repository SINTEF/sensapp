-- ClickHouse initialization schema for SensApp
-- Uses hybrid UUID/UInt64 approach for optimal performance

-- Create the 'units' table
CREATE TABLE IF NOT EXISTS units (
    id UInt64,
    name String,
    description Nullable(String)
) ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 8192;

-- Create the 'sensors' table with UUID and sensor_id (hybrid approach)
CREATE TABLE IF NOT EXISTS sensors (
    sensor_id UInt64,
    uuid UUID,
    name String,
    type String,
    unit Nullable(UInt64)
) ENGINE = MergeTree()
ORDER BY sensor_id
SETTINGS index_granularity = 8192;

-- Create the 'labels' table
CREATE TABLE IF NOT EXISTS labels (
    sensor_id UInt64,
    name String,
    description Nullable(String)
) ENGINE = MergeTree()
ORDER BY (sensor_id, name)
SETTINGS index_granularity = 8192;

-- Create the 'integer_values' table
CREATE TABLE IF NOT EXISTS integer_values (
    sensor_id UInt64,
    timestamp_us Int64 CODEC(DoubleDelta, LZ4),
    value Int64 CODEC(DoubleDelta, ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime64(timestamp_us / 1000000, 6))
ORDER BY (sensor_id, timestamp_us)
SETTINGS index_granularity_bytes = 10485760;

-- Create the 'numeric_values' table
CREATE TABLE IF NOT EXISTS numeric_values (
    sensor_id UInt64,
    timestamp_us Int64 CODEC(DoubleDelta, LZ4),
    value Decimal128(38) CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime64(timestamp_us / 1000000, 6))
ORDER BY (sensor_id, timestamp_us)
SETTINGS index_granularity_bytes = 10485760;

-- Create the 'float_values' table
CREATE TABLE IF NOT EXISTS float_values (
    sensor_id UInt64,
    timestamp_us Int64 CODEC(DoubleDelta, LZ4),
    value Float64 CODEC(DoubleDelta, ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime64(timestamp_us / 1000000, 6))
ORDER BY (sensor_id, timestamp_us)
SETTINGS index_granularity_bytes = 10485760;

-- Create the 'string_values' table
CREATE TABLE IF NOT EXISTS string_values (
    sensor_id UInt64,
    timestamp_us Int64 CODEC(DoubleDelta, LZ4),
    value LowCardinality(String) CODEC(ZSTD(3))  -- Automatic string deduplication
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime64(timestamp_us / 1000000, 6))
ORDER BY (sensor_id, timestamp_us)
SETTINGS index_granularity_bytes = 10485760;

-- Create the 'boolean_values' table
CREATE TABLE IF NOT EXISTS boolean_values (
    sensor_id UInt64,
    timestamp_us Int64 CODEC(DoubleDelta, LZ4),
    value Bool CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime64(timestamp_us / 1000000, 6))
ORDER BY (sensor_id, timestamp_us)
SETTINGS index_granularity_bytes = 10485760;

-- Create the 'location_values' table
CREATE TABLE IF NOT EXISTS location_values (
    sensor_id UInt64,
    timestamp_us Int64 CODEC(DoubleDelta, LZ4),
    latitude Float64 CODEC(DoubleDelta, ZSTD(1)),
    longitude Float64 CODEC(DoubleDelta, ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime64(timestamp_us / 1000000, 6))
ORDER BY (sensor_id, timestamp_us)
SETTINGS index_granularity_bytes = 10485760;

-- Create the 'json_values' table
CREATE TABLE IF NOT EXISTS json_values (
    sensor_id UInt64,
    timestamp_us Int64 CODEC(DoubleDelta, LZ4),
    value JSON  -- Native JSON type (ClickHouse 24.8+) for better performance and compression
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime64(timestamp_us / 1000000, 6))
ORDER BY (sensor_id, timestamp_us)
SETTINGS index_granularity_bytes = 10485760;

-- Create the 'blob_values' table
CREATE TABLE IF NOT EXISTS blob_values (
    sensor_id UInt64,
    timestamp_us Int64 CODEC(DoubleDelta, LZ4),
    value String CODEC(ZSTD(3))  -- Binary data stored as string (base64 encoded)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime64(timestamp_us / 1000000, 6))
ORDER BY (sensor_id, timestamp_us)
SETTINGS index_granularity_bytes = 10485760;

-- Create indexes for units table
CREATE INDEX IF NOT EXISTS idx_units_name ON units (name) TYPE bloom_filter GRANULARITY 1;

-- Create materialized view for sensor catalog (similar to PostgreSQL view)
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_catalog_view
ENGINE = MergeTree()
ORDER BY sensor_id AS
SELECT
    s.sensor_id,
    s.uuid,
    s.name,
    s.type,
    u.name AS unit_name,
    u.description AS unit_description
FROM sensors s
LEFT JOIN units u ON s.unit = u.id;

-- Create materialized view for metrics summary (similar to PostgreSQL view)
CREATE MATERIALIZED VIEW IF NOT EXISTS metrics_summary_view
ENGINE = AggregatingMergeTree()
ORDER BY (name, type) AS
SELECT
    name,
    type,
    count() AS sensor_count,
    min(sensor_id) AS min_sensor_id,
    max(sensor_id) AS max_sensor_id
FROM sensors
GROUP BY name, type;
