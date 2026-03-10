# RRDCached Storage Backend

## Overview

RRDCached is an experimental storage backend for SensApp that uses [RRDtool's](https://oss.oetiker.ch/rrdtool/) caching daemon for time-series data storage. RRDtool is a mature, high-performance system originally designed for network monitoring that excels at storing and graphing time-series data with fixed-size storage requirements.

## How it Works

SensApp integrates with RRDCached through a TCP connection, allowing it to:
- Create Round Robin Database (RRD) files for each sensor
- Publish numeric sensor data points  
- Query historical data using RRD's consolidation functions
- List available sensors based on RRD files

### Connection String

```
rrdcached://host:port?preset=<preset_name>
```

Example:
```
rrdcached://127.0.0.1:42217?preset=hoarder
```

### Available Presets

- **munin**: Traditional Munin-style data retention (5-minute resolution)
- **hoarder**: High-resolution data retention for detailed analysis

## Configuration

To use RRDCached as a storage backend:

1. Start an RRDCached daemon:
```bash
rrdcached -l 127.0.0.1:42217 -b /var/lib/rrdcached/db -j /var/lib/rrdcached/journal
```

2. Configure SensApp with the RRDCached connection string in `settings.toml`:
```toml
[database]
connection_string = "rrdcached://127.0.0.1:42217?preset=hoarder"
```

## Current Implementation Status

### Supported Features

✅ **Publishing numeric data** - Integer and Float sensor values  
✅ **Creating RRD files** - Automatic RRD creation with configurable presets  
✅ **Querying sensor data** - Fetch historical data with time ranges  
✅ **Listing sensors** - Basic sensor enumeration based on RRD files  

### Limitations

The RRDCached backend has significant limitations due to the fundamental design of RRDtool:

#### Metadata Storage
RRDtool only stores numeric time-series data. It cannot store:
- **Sensor names** - Only UUIDs are used as RRD filenames
- **Sensor types** - All data is treated as numeric (Float)
- **Units** - No unit information is preserved
- **Labels/Tags** - Not supported by RRD format
- **Non-numeric types** - String, Boolean, Location, and JSON types cannot be stored

#### Feature Limitations
- **Arrow format export/import** - Not supported due to missing metadata
- **Sensor search by name** - Names are not stored, only UUIDs
- **Type preservation** - Integer values become Floats in RRD
- **Round-trip data integrity** - Original sensor metadata cannot be reconstructed

#### Query Limitations
- **Recent data** - RRD consolidation may return NaN for very recent data points
- **Exact timestamps** - RRD uses fixed time intervals, timestamps are rounded
- **Raw data access** - Only consolidated data is available after initial retention period

## Testing

Integration tests are available but **not enabled by default** due to the limitations described above. To run RRDCached-specific tests:

```bash
# Ensure RRDCached is running on port 42217
TEST_DATABASE_URL="rrdcached://127.0.0.1:42217?preset=hoarder" \
  cargo test --no-default-features --features rrdcached --test rrdcached_integration
```

Note: Generic storage tests that depend on full metadata support will fail. This includes:
- Arrow format import/export tests
- Tests requiring sensor name lookups
- Tests using non-numeric data types
- Round-trip data integrity tests

## Use Cases

RRDCached is suitable for:
- **Fixed-size storage** - RRD files don't grow over time
- **Network monitoring** - Traditional RRDtool use case
- **Simple numeric metrics** - Temperature, CPU usage, network traffic
- **Long-term trending** - Automatic data consolidation over time

RRDCached is **not** suitable for:
- **Rich sensor metadata** - When names, units, or labels are important
- **Non-numeric data** - String, Boolean, or complex data types
- **Data export/interchange** - When Arrow or other formats are needed
- **Exact data retrieval** - When original resolution must be preserved

## Future Plans

The RRDCached backend is currently experimental. Future enhancements being considered include:

- **External metadata store** - SQLite or JSON sidecar for sensor metadata
- **Hybrid storage** - RRD for numeric data, separate store for metadata
- **Improved query support** - Better handling of consolidation functions
- **Grafana integration** - Direct RRD graph generation

These enhancements would address the current limitations but require significant architectural changes. Implementation timeline is to be determined based on user needs and project priorities.

## Technical Details

### RRD File Structure

Each sensor creates an RRD file named `{uuid}.rrd` with:
- **Data Source (DS)**: Single gauge-type data source named "value"
- **Round Robin Archives (RRA)**: Multiple archives with different resolutions
  - Munin preset: 5-minute, 30-minute, 2-hour, daily consolidations
  - Hoarder preset: 1-second resolution with longer retention

### Consolidation Functions

When querying data, RRDCached uses consolidation functions:
- **AVERAGE**: Mean value over the consolidation period
- **MIN**: Minimum value
- **MAX**: Maximum value  
- **LAST**: Most recent value

### Implementation Notes

The implementation uses:
- `rrdcached_client` crate for protocol communication
- Async/await pattern with Tokio runtime
- UUID v7 for time-ordered sensor identifiers
- In-memory tracking of created sensors per session