use crate::{
    datamodel::{
        Sample, SensAppDateTime, Sensor, SensorType, TypedSamples, batch_builder::BatchBuilder,
        unit::Unit,
    },
    infer::{
        columns::{InferedColumn, infer_column},
        datagrid::StringDataGrid,
        datetime_guesser::likely_datetime_column,
        parsing::InferedValue,
    },
    storage::StorageInstance,
};
use anyhow::{Result, anyhow};
use crate::ingestors::http::server::ParseMode;
use csv_async::AsyncReader;
use futures::{StreamExt, io};
use std::{collections::HashMap, sync::Arc};

// Type alias to simplify complex HashMap type for clippy
type SensorDataMap = HashMap<String, (Arc<Sensor>, Vec<(SensAppDateTime, InferedValue)>)>;

pub async fn publish_csv_async<R: io::AsyncRead + Unpin + Send>(
    mut csv_reader: AsyncReader<R>,
    _batch_size: usize,
    storage: Arc<dyn StorageInstance>,
) -> Result<()> {
    // Read all CSV data into a StringDataGrid
    let headers = csv_reader.headers().await?.clone();
    let column_names = headers.iter().map(|s| s.to_string()).collect::<Vec<_>>();

    let mut rows = Vec::new();
    let mut records = csv_reader.records();

    while let Some(record) = records.next().await {
        let record = record?;
        let row = record.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        rows.push(row);
    }

    let data_grid = StringDataGrid::new(column_names.clone(), rows)?;

    // Parse the CSV data
    let parsed_data = parse_csv_data_grid(data_grid)?;

    // Use BatchBuilder to publish the data
    let mut batch_builder = BatchBuilder::new()?;

    for (_sensor_name, (sensor, samples)) in parsed_data {
        batch_builder.add(sensor, samples).await?;
    }

    // Send all batches to storage
    batch_builder.send_what_is_left(storage).await?;

    Ok(())
}

/// Enhanced CSV parser with strict and inference modes
pub async fn publish_csv_async_with_mode<R: io::AsyncRead + Unpin + Send>(
    mut csv_reader: AsyncReader<R>,
    _batch_size: usize,
    mode: ParseMode,
    storage: Arc<dyn StorageInstance>,
) -> Result<()> {
    // Read all CSV data into a StringDataGrid
    let headers = csv_reader.headers().await?.clone();
    let column_names = headers.iter().map(|s| s.to_string()).collect::<Vec<_>>();

    let mut rows = Vec::new();
    let mut records = csv_reader.records();

    while let Some(record) = records.next().await {
        let record = record?;
        let row = record.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        rows.push(row);
    }

    let data_grid = StringDataGrid::new(column_names.clone(), rows)?;

    // Parse the CSV data based on mode
    let parsed_data = match mode {
        ParseMode::Strict => parse_csv_strict(data_grid)?,
        ParseMode::Infer => parse_csv_with_inference(data_grid)?,
    };

    // Use BatchBuilder to publish the data
    let mut batch_builder = BatchBuilder::new()?;

    for (_sensor_name, (sensor, samples)) in parsed_data {
        batch_builder.add(sensor, samples).await?;
    }

    // Send all batches to storage
    batch_builder.send_what_is_left(storage).await?;

    Ok(())
}

/// Parse CSV data in strict mode with exact column name matching
fn parse_csv_strict(data_grid: StringDataGrid) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    let column_names = &data_grid.column_names;

    if data_grid.rows.is_empty() {
        return Err(anyhow!("CSV contains no data rows"));
    }

    // Check for exact column formats
    if column_names == &["datetime", "sensor_name", "value"] {
        return parse_long_format_with_names(data_grid);
    }
    
    if column_names == &["datetime", "sensor_uuid", "value"] {
        return parse_long_format_with_uuids(data_grid);
    }
    
    if column_names.len() == 4 && column_names == &["datetime", "sensor_name", "value", "unit"] {
        return parse_long_format_with_names_and_units(data_grid);
    }
    
    if column_names.len() == 4 && column_names == &["datetime", "sensor_uuid", "value", "unit"] {
        return parse_long_format_with_uuids_and_units(data_grid);
    }

    // Check for wide format (datetime + sensor columns)
    if column_names.len() >= 2 && column_names[0] == "datetime" {
        return parse_wide_format(data_grid);
    }

    Err(anyhow!(
        "Strict mode: CSV columns don't match expected format.\n\
        Expected formats:\n\
        - Long format: datetime,sensor_name,value[,unit]\n\
        - Long format with UUIDs: datetime,sensor_uuid,value[,unit]\n\
        - Wide format: datetime,sensor1,sensor2,sensor3...\n\
        \n\
        Received columns: {}\n\
        Hint: Use ?mode=infer for automatic column detection",
        column_names.join(", ")
    ))
}

/// Parse CSV data with intelligent inference
fn parse_csv_with_inference(data_grid: StringDataGrid) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    // Use the existing smart inference logic but with better column matching
    parse_csv_data_grid_enhanced(data_grid)
}

/// Parse CSV data grid into sensors and their samples
fn parse_csv_data_grid(
    data_grid: StringDataGrid,
) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    let column_names = &data_grid.column_names;
    let rows = &data_grid.rows;

    if rows.is_empty() {
        return Err(anyhow!("CSV contains no data rows"));
    }

    if column_names.len() < 2 {
        return Err(anyhow!(
            "CSV must have at least 2 columns (datetime and values)"
        ));
    }

    // Convert rows to columns for type inference
    let mut columns = vec![Vec::new(); column_names.len()];
    for row in rows {
        for (col_idx, value) in row.iter().enumerate() {
            if col_idx < columns.len() {
                columns[col_idx].push(value.clone());
            }
        }
    }

    // Infer column types
    let inferred_columns: Vec<InferedColumn> = columns
        .iter()
        .map(|col| infer_column(col.clone(), true, false))
        .collect();

    // Try to identify datetime column
    let datetime_column = likely_datetime_column(column_names, &inferred_columns);

    // Find sensor name and value columns
    let sensor_name_idx =
        find_column_index(column_names, &["sensor_name", "metric", "name", "sensor"]);
    let value_idx = find_column_index(column_names, &["value", "reading", "measurement"]);
    let unit_idx = find_column_index(column_names, &["unit", "units"]);
    let datetime_idx = datetime_column
        .as_ref()
        .and_then(|name| column_names.iter().position(|col| col == name));

    let mut sensors_data: SensorDataMap = HashMap::new();

    // Process each row
    for (row_idx, row) in rows.iter().enumerate() {
        let datetime = if let Some(dt_idx) = datetime_idx {
            parse_datetime_from_inferred(&inferred_columns[dt_idx], row_idx)?
        } else {
            // Use row index as timestamp if no datetime column
            SensAppDateTime::from_unix_seconds(row_idx as f64)
        };

        if let (Some(sensor_name_idx), Some(value_idx)) = (sensor_name_idx, value_idx) {
            // Long format: one row per sample
            let sensor_name = row[sensor_name_idx].clone();
            if sensor_name.trim().is_empty() {
                return Err(anyhow!("Empty sensor name found in row {}", row_idx + 1));
            }

            let value = parse_value_from_inferred(&inferred_columns[value_idx], row_idx)?;
            let unit_name = unit_idx
                .map(|idx| row[idx].clone())
                .filter(|s| !s.is_empty());

            sensors_data
                .entry(sensor_name.clone())
                .or_insert_with(|| {
                    let sensor_type = inferred_value_to_sensor_type(&value);
                    let unit = unit_name.map(|name| Unit::new(name, None));
                    let sensor = Arc::new(
                        Sensor::new_without_uuid(sensor_name, sensor_type, unit, None).unwrap(),
                    );
                    (sensor, Vec::new())
                })
                .1
                .push((datetime, value));
        } else if datetime_idx.is_some() {
            // Wide format: each column (except datetime) is a sensor
            let mut sensor_count = 0;
            for (col_idx, col_name) in column_names.iter().enumerate() {
                if Some(col_idx) == datetime_idx {
                    continue; // Skip datetime column
                }

                let value = parse_value_from_inferred(&inferred_columns[col_idx], row_idx)?;
                let sensor_name = col_name.clone();

                sensors_data
                    .entry(sensor_name.clone())
                    .or_insert_with(|| {
                        let sensor_type = inferred_value_to_sensor_type(&value);
                        let sensor = Arc::new(
                            Sensor::new_without_uuid(sensor_name, sensor_type, None, None).unwrap(),
                        );
                        (sensor, Vec::new())
                    })
                    .1
                    .push((datetime, value));
                sensor_count += 1;
            }

            if sensor_count == 0 {
                return Err(anyhow!("No sensor columns found - CSV format unclear"));
            }
        } else {
            return Err(anyhow!(
                "Unable to parse CSV: no clear datetime column and no sensor_name/value columns found. \
                Expected either (datetime, sensor_name, value) format or (datetime, sensor1, sensor2, ...) format"
            ));
        }
    }

    // Convert to final format
    if sensors_data.is_empty() {
        return Err(anyhow!("No sensors or data found in CSV"));
    }

    let mut result = HashMap::new();
    for (sensor_name, (sensor, samples)) in sensors_data {
        if samples.is_empty() {
            return Err(anyhow!("Sensor '{}' has no data samples", sensor_name));
        }

        let typed_samples = convert_samples_to_typed_samples(samples, &sensor.sensor_type)?;
        result.insert(sensor_name, (sensor, typed_samples));
    }

    Ok(result)
}

/// Parse long format CSV with sensor names: datetime,sensor_name,value
fn parse_long_format_with_names(data_grid: StringDataGrid) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    parse_long_format_generic(data_grid, 0, 1, 2, None, false)
}

/// Parse long format CSV with UUIDs: datetime,sensor_uuid,value
fn parse_long_format_with_uuids(data_grid: StringDataGrid) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    parse_long_format_generic(data_grid, 0, 1, 2, None, true)
}

/// Parse long format CSV with sensor names and units: datetime,sensor_name,value,unit
fn parse_long_format_with_names_and_units(data_grid: StringDataGrid) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    parse_long_format_generic(data_grid, 0, 1, 2, Some(3), false)
}

/// Parse long format CSV with UUIDs and units: datetime,sensor_uuid,value,unit
fn parse_long_format_with_uuids_and_units(data_grid: StringDataGrid) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    parse_long_format_generic(data_grid, 0, 1, 2, Some(3), true)
}

/// Generic parser for long format CSV
fn parse_long_format_generic(
    data_grid: StringDataGrid,
    datetime_idx: usize,
    sensor_idx: usize,
    value_idx: usize,
    unit_idx: Option<usize>,
    is_uuid: bool,
) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    let column_names = &data_grid.column_names;
    let rows = &data_grid.rows;

    if rows.is_empty() {
        return Err(anyhow!("CSV contains no data rows"));
    }

    // Convert rows to columns for type inference
    let mut columns = vec![Vec::new(); column_names.len()];
    for row in rows {
        for (col_idx, value) in row.iter().enumerate() {
            if col_idx < columns.len() {
                columns[col_idx].push(value.clone());
            }
        }
    }

    // Infer column types
    let inferred_columns: Vec<InferedColumn> = columns
        .iter()
        .map(|col| infer_column(col.clone(), true, false))
        .collect();

    let mut sensors_data: SensorDataMap = HashMap::new();

    // Process each row
    for (row_idx, row) in rows.iter().enumerate() {
        let datetime = parse_datetime_from_inferred(&inferred_columns[datetime_idx], row_idx)?;
        
        let sensor_identifier = row[sensor_idx].clone();
        if sensor_identifier.trim().is_empty() {
            return Err(anyhow!("Empty sensor identifier found in row {}", row_idx + 1));
        }

        // Validate UUID if expected
        if is_uuid && !is_valid_uuid(&sensor_identifier) {
            return Err(anyhow!(
                "Invalid UUID format in sensor_uuid column at row {}: '{}'", 
                row_idx + 1, 
                sensor_identifier
            ));
        }

        let value = parse_value_from_inferred(&inferred_columns[value_idx], row_idx)?;
        let unit_name = unit_idx
            .map(|idx| row[idx].clone())
            .filter(|s| !s.is_empty());

        sensors_data
            .entry(sensor_identifier.clone())
            .or_insert_with(|| {
                let sensor_type = inferred_value_to_sensor_type(&value);
                let unit = unit_name.map(|name| Unit::new(name, None));
                let sensor = Arc::new(
                    if is_uuid {
                        // Parse UUID and use it
                        let uuid = uuid::Uuid::parse_str(&sensor_identifier).unwrap();
                        Sensor::new(uuid, sensor_identifier.clone(), sensor_type, unit, None)
                    } else {
                        Sensor::new_without_uuid(sensor_identifier.clone(), sensor_type, unit, None).unwrap()
                    }
                );
                (sensor, Vec::new())
            })
            .1
            .push((datetime, value));
    }

    // Convert to final format
    if sensors_data.is_empty() {
        return Err(anyhow!("No sensors or data found in CSV"));
    }

    let mut result = HashMap::new();
    for (sensor_name, (sensor, samples)) in sensors_data {
        if samples.is_empty() {
            return Err(anyhow!("Sensor '{}' has no data samples", sensor_name));
        }

        let typed_samples = convert_samples_to_typed_samples(samples, &sensor.sensor_type)?;
        result.insert(sensor_name, (sensor, typed_samples));
    }

    Ok(result)
}

/// Parse wide format CSV: datetime,sensor1,sensor2,sensor3...
fn parse_wide_format(data_grid: StringDataGrid) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    let column_names = &data_grid.column_names;
    let rows = &data_grid.rows;

    if rows.is_empty() {
        return Err(anyhow!("CSV contains no data rows"));
    }

    let datetime_idx = 0; // First column is datetime

    // Convert rows to columns for type inference
    let mut columns = vec![Vec::new(); column_names.len()];
    for row in rows {
        for (col_idx, value) in row.iter().enumerate() {
            if col_idx < columns.len() {
                columns[col_idx].push(value.clone());
            }
        }
    }

    // Infer column types
    let inferred_columns: Vec<InferedColumn> = columns
        .iter()
        .map(|col| infer_column(col.clone(), true, false))
        .collect();

    let mut result = HashMap::new();

    // Process each sensor column (skip datetime)
    for (col_idx, col_name) in column_names.iter().enumerate() {
        if col_idx == datetime_idx {
            continue; // Skip datetime column
        }

        let mut samples = Vec::new();
        for (row_idx, _row) in rows.iter().enumerate() {
            let datetime = parse_datetime_from_inferred(&inferred_columns[datetime_idx], row_idx)?;
            let value = parse_value_from_inferred(&inferred_columns[col_idx], row_idx)?;
            samples.push((datetime, value));
        }

        if samples.is_empty() {
            continue;
        }

        let sensor_type = inferred_value_to_sensor_type(&samples[0].1);
        let sensor = Arc::new(
            Sensor::new_without_uuid(col_name.clone(), sensor_type, None, None)?
        );

        let typed_samples = convert_samples_to_typed_samples(samples, &sensor.sensor_type)?;
        result.insert(col_name.clone(), (sensor, typed_samples));
    }

    if result.is_empty() {
        return Err(anyhow!("No sensor data found in wide format CSV"));
    }

    Ok(result)
}

/// Enhanced CSV parsing with flexible inference
fn parse_csv_data_grid_enhanced(data_grid: StringDataGrid) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    // This is an enhanced version of the original parse_csv_data_grid with better column detection
    // For now, delegate to the original implementation and enhance later
    parse_csv_data_grid(data_grid)
}

/// Check if a string is a valid UUID
fn is_valid_uuid(s: &str) -> bool {
    uuid::Uuid::parse_str(s).is_ok()
}

fn find_column_index(column_names: &[String], candidates: &[&str]) -> Option<usize> {
    for candidate in candidates {
        if let Some(idx) = column_names
            .iter()
            .position(|name| name.to_lowercase() == candidate.to_lowercase())
        {
            return Some(idx);
        }
    }
    None
}

fn parse_datetime_from_inferred(column: &InferedColumn, row_idx: usize) -> Result<SensAppDateTime> {
    match column {
        InferedColumn::DateTime(values) => {
            if row_idx < values.len() {
                Ok(values[row_idx])
            } else {
                Err(anyhow!(
                    "Row index {} out of bounds for datetime column",
                    row_idx
                ))
            }
        }
        InferedColumn::Integer(values) => {
            if row_idx < values.len() {
                Ok(SensAppDateTime::from_unix_seconds(values[row_idx] as f64))
            } else {
                Err(anyhow!(
                    "Row index {} out of bounds for integer datetime column",
                    row_idx
                ))
            }
        }
        _ => Err(anyhow!(
            "Cannot parse datetime from column type: {:?}",
            column
        )),
    }
}

fn parse_value_from_inferred(column: &InferedColumn, row_idx: usize) -> Result<InferedValue> {
    match column {
        InferedColumn::Integer(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::Integer(values[row_idx]))
            } else {
                Err(anyhow!("Row index {} out of bounds", row_idx))
            }
        }
        InferedColumn::Float(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::Float(values[row_idx]))
            } else {
                Err(anyhow!("Row index {} out of bounds", row_idx))
            }
        }
        InferedColumn::String(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::String(values[row_idx].clone()))
            } else {
                Err(anyhow!("Row index {} out of bounds", row_idx))
            }
        }
        InferedColumn::Boolean(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::Boolean(values[row_idx]))
            } else {
                Err(anyhow!("Row index {} out of bounds", row_idx))
            }
        }
        InferedColumn::DateTime(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::DateTime(values[row_idx]))
            } else {
                Err(anyhow!("Row index {} out of bounds", row_idx))
            }
        }
        InferedColumn::Json(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::Json(values[row_idx].clone()))
            } else {
                Err(anyhow!("Row index {} out of bounds", row_idx))
            }
        }
        InferedColumn::Numeric(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::Numeric(values[row_idx]))
            } else {
                Err(anyhow!("Row index {} out of bounds", row_idx))
            }
        }
    }
}

fn inferred_value_to_sensor_type(value: &InferedValue) -> SensorType {
    match value {
        InferedValue::Integer(_) => SensorType::Integer,
        InferedValue::Float(_) => SensorType::Float,
        InferedValue::Numeric(_) => SensorType::Numeric,
        InferedValue::String(_) => SensorType::String,
        InferedValue::Boolean(_) => SensorType::Boolean,
        InferedValue::DateTime(_) => SensorType::String, // Store datetime as string
        InferedValue::Json(_) => SensorType::Json,
    }
}

fn convert_samples_to_typed_samples(
    samples: Vec<(SensAppDateTime, InferedValue)>,
    sensor_type: &SensorType,
) -> Result<TypedSamples> {
    match sensor_type {
        SensorType::Integer => {
            let typed_samples = samples
                .into_iter()
                .map(|(datetime, value)| match value {
                    InferedValue::Integer(val) => Sample {
                        datetime,
                        value: val,
                    },
                    _ => unreachable!("Sensor type mismatch"),
                })
                .collect();
            Ok(TypedSamples::Integer(typed_samples))
        }
        SensorType::Float => {
            let typed_samples = samples
                .into_iter()
                .map(|(datetime, value)| match value {
                    InferedValue::Float(val) => Sample {
                        datetime,
                        value: val,
                    },
                    _ => unreachable!("Sensor type mismatch"),
                })
                .collect();
            Ok(TypedSamples::Float(typed_samples))
        }
        SensorType::Numeric => {
            let typed_samples = samples
                .into_iter()
                .map(|(datetime, value)| match value {
                    InferedValue::Numeric(val) => Sample {
                        datetime,
                        value: val,
                    },
                    _ => unreachable!("Sensor type mismatch"),
                })
                .collect();
            Ok(TypedSamples::Numeric(typed_samples))
        }
        SensorType::String => {
            let typed_samples = samples
                .into_iter()
                .map(|(datetime, value)| {
                    let string_value = match value {
                        InferedValue::String(val) => val,
                        InferedValue::DateTime(val) => val.to_rfc3339(),
                        _ => unreachable!("Sensor type mismatch"),
                    };
                    Sample {
                        datetime,
                        value: string_value,
                    }
                })
                .collect();
            Ok(TypedSamples::String(typed_samples))
        }
        SensorType::Boolean => {
            let typed_samples = samples
                .into_iter()
                .map(|(datetime, value)| match value {
                    InferedValue::Boolean(val) => Sample {
                        datetime,
                        value: val,
                    },
                    _ => unreachable!("Sensor type mismatch"),
                })
                .collect();
            Ok(TypedSamples::Boolean(typed_samples))
        }
        SensorType::Json => {
            let typed_samples = samples
                .into_iter()
                .map(|(datetime, value)| match value {
                    InferedValue::Json(val) => Sample {
                        datetime,
                        value: (*val).clone(),
                    },
                    _ => unreachable!("Sensor type mismatch"),
                })
                .collect();
            Ok(TypedSamples::Json(typed_samples))
        }
        _ => Err(anyhow!("Unsupported sensor type: {:?}", sensor_type)),
    }
}

#[cfg(test)]
#[cfg(all(feature = "test-utils", feature = "sqlite"))]
mod strict_mode_tests {
    use super::*;
    use crate::datamodel::SensorType;

    #[tokio::test]
    async fn test_strict_mode_long_format_with_names() {
        let csv_data = "datetime,sensor_name,value\n2024-01-01T00:00:00Z,temp1,22.5\n2024-01-01T01:00:00Z,temp2,23.1";
        let reader = std::io::Cursor::new(csv_data.as_bytes());
        let csv_reader = csv_async::AsyncReaderBuilder::new()
            .has_headers(true)
            .create_reader(reader);

        let storage = std::sync::Arc::new(
            crate::storage::sqlite::SqliteStorage::connect("sqlite::memory:")
                .await
                .unwrap(),
        );

        let result = publish_csv_async_with_mode(csv_reader, 1000, ParseMode::Strict, storage).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_strict_mode_long_format_with_uuids() {
        let uuid1 = uuid::Uuid::new_v4();
        let uuid2 = uuid::Uuid::new_v4();
        let csv_data = format!(
            "datetime,sensor_uuid,value\n2024-01-01T00:00:00Z,{},22.5\n2024-01-01T01:00:00Z,{},23.1",
            uuid1, uuid2
        );
        let reader = std::io::Cursor::new(csv_data.as_bytes());
        let csv_reader = csv_async::AsyncReaderBuilder::new()
            .has_headers(true)
            .create_reader(reader);

        let storage = std::sync::Arc::new(
            crate::storage::sqlite::SqliteStorage::connect("sqlite::memory:")
                .await
                .unwrap(),
        );

        let result = publish_csv_async_with_mode(csv_reader, 1000, ParseMode::Strict, storage).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_strict_mode_wide_format() {
        let csv_data = "datetime,temperature,humidity,pressure\n2024-01-01T00:00:00Z,22.5,65.2,1013.25\n2024-01-01T01:00:00Z,23.1,64.8,1013.12";
        let reader = std::io::Cursor::new(csv_data.as_bytes());
        let csv_reader = csv_async::AsyncReaderBuilder::new()
            .has_headers(true)
            .create_reader(reader);

        let storage = std::sync::Arc::new(
            crate::storage::sqlite::SqliteStorage::connect("sqlite::memory:")
                .await
                .unwrap(),
        );

        let result = publish_csv_async_with_mode(csv_reader, 1000, ParseMode::Strict, storage).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_strict_mode_invalid_format_error() {
        let csv_data = "timestamp,device_name,reading\n2024-01-01T00:00:00Z,dev1,22.5";
        let reader = std::io::Cursor::new(csv_data.as_bytes());
        let csv_reader = csv_async::AsyncReaderBuilder::new()
            .has_headers(true)
            .create_reader(reader);

        let storage = std::sync::Arc::new(
            crate::storage::sqlite::SqliteStorage::connect("sqlite::memory:")
                .await
                .unwrap(),
        );

        let result = publish_csv_async_with_mode(csv_reader, 1000, ParseMode::Strict, storage).await;
        assert!(result.is_err());
        let error_msg = result.err().unwrap().to_string();
        assert!(error_msg.contains("Strict mode: CSV columns don't match expected format"));
        assert!(error_msg.contains("Hint: Use ?mode=infer"));
    }

    #[tokio::test]
    async fn test_strict_mode_invalid_uuid_error() {
        let csv_data = "datetime,sensor_uuid,value\n2024-01-01T00:00:00Z,not-a-uuid,22.5";
        let reader = std::io::Cursor::new(csv_data.as_bytes());
        let csv_reader = csv_async::AsyncReaderBuilder::new()
            .has_headers(true)
            .create_reader(reader);

        let storage = std::sync::Arc::new(
            crate::storage::sqlite::SqliteStorage::connect("sqlite::memory:")
                .await
                .unwrap(),
        );

        let result = publish_csv_async_with_mode(csv_reader, 1000, ParseMode::Strict, storage).await;
        assert!(result.is_err());
        let error_msg = result.err().unwrap().to_string();
        assert!(error_msg.contains("Invalid UUID format"));
    }

    #[test]
    fn test_parse_strict_formats() {
        // Test long format with names
        let data_grid = StringDataGrid::new(
            vec!["datetime".to_string(), "sensor_name".to_string(), "value".to_string()],
            vec![
                vec!["2024-01-01T00:00:00Z".to_string(), "temp1".to_string(), "22.5".to_string()],
                vec!["2024-01-01T01:00:00Z".to_string(), "temp2".to_string(), "23.1".to_string()],
            ],
        ).unwrap();

        let result = parse_csv_strict(data_grid);
        assert!(result.is_ok());
        let sensors = result.unwrap();
        assert_eq!(sensors.len(), 2);
        assert!(sensors.contains_key("temp1"));
        assert!(sensors.contains_key("temp2"));
    }

    #[test]
    fn test_parse_strict_wide_format() {
        let data_grid = StringDataGrid::new(
            vec!["datetime".to_string(), "temperature".to_string(), "humidity".to_string()],
            vec![
                vec!["2024-01-01T00:00:00Z".to_string(), "22.5".to_string(), "65.2".to_string()],
                vec!["2024-01-01T01:00:00Z".to_string(), "23.1".to_string(), "64.8".to_string()],
            ],
        ).unwrap();

        let result = parse_csv_strict(data_grid);
        assert!(result.is_ok());
        let sensors = result.unwrap();
        assert_eq!(sensors.len(), 2);
        assert!(sensors.contains_key("temperature"));
        assert!(sensors.contains_key("humidity"));
    }

    #[test]
    fn test_uuid_validation() {
        assert!(is_valid_uuid("550e8400-e29b-41d4-a716-446655440000"));
        assert!(!is_valid_uuid("not-a-uuid"));
        assert!(!is_valid_uuid(""));
    }

    #[test]
    fn test_strict_format_validation_errors() {
        let data_grid = StringDataGrid::new(
            vec!["timestamp".to_string(), "device".to_string(), "reading".to_string()],
            vec![
                vec!["2024-01-01T00:00:00Z".to_string(), "dev1".to_string(), "22.5".to_string()],
            ],
        ).unwrap();

        let result = parse_csv_strict(data_grid);
        assert!(result.is_err());
        let error_msg = result.err().unwrap().to_string();
        assert!(error_msg.contains("Strict mode"));
        assert!(error_msg.contains("Expected formats"));
        assert!(error_msg.contains("mode=infer"));
    }

    #[tokio::test]
    async fn test_inference_mode_flexible_column_names() {
        let csv_data = "timestamp,metric,reading\n2024-01-01T00:00:00Z,temperature,22.5\n2024-01-01T01:00:00Z,humidity,65.2";
        let reader = std::io::Cursor::new(csv_data.as_bytes());
        let csv_reader = csv_async::AsyncReaderBuilder::new()
            .has_headers(true)
            .create_reader(reader);

        let storage = std::sync::Arc::new(
            crate::storage::sqlite::SqliteStorage::connect("sqlite::memory:")
                .await
                .unwrap(),
        );

        let result = publish_csv_async_with_mode(csv_reader, 1000, ParseMode::Infer, storage).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_empty_csv_error() {
        let csv_data = "datetime,sensor_name,value\n";
        let reader = std::io::Cursor::new(csv_data.as_bytes());
        let csv_reader = csv_async::AsyncReaderBuilder::new()
            .has_headers(true)
            .create_reader(reader);

        let storage = std::sync::Arc::new(
            crate::storage::sqlite::SqliteStorage::connect("sqlite::memory:")
                .await
                .unwrap(),
        );

        let result = publish_csv_async_with_mode(csv_reader, 1000, ParseMode::Strict, storage).await;
        assert!(result.is_err());
        let error_msg = result.err().unwrap().to_string();
        assert!(error_msg.contains("no data rows"));
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_parse_strict_formats() {
        // Initialize configuration
        let _ = crate::config::load_configuration();
        
        // Test long format with names
        let data_grid = StringDataGrid::new(
            vec!["datetime".to_string(), "sensor_name".to_string(), "value".to_string()],
            vec![
                vec!["2024-01-01T00:00:00Z".to_string(), "temp1".to_string(), "22.5".to_string()],
                vec!["2024-01-01T01:00:00Z".to_string(), "temp2".to_string(), "23.1".to_string()],
            ],
        ).unwrap();

        let result = parse_csv_strict(data_grid);
        assert!(result.is_ok());
        let sensors = result.unwrap();
        assert_eq!(sensors.len(), 2);
        assert!(sensors.contains_key("temp1"));
        assert!(sensors.contains_key("temp2"));
    }

    #[test]
    fn test_is_valid_uuid() {
        let valid_uuid = uuid::Uuid::new_v4().to_string();
        assert!(is_valid_uuid(&valid_uuid));
        assert!(!is_valid_uuid("not-a-uuid"));
        assert!(!is_valid_uuid(""));
    }
}
