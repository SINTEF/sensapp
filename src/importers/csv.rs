use crate::{
    datamodel::{
        Sample, SensAppDateTime, Sensor, SensorType, TypedSamples, batch_builder::BatchBuilder,
        unit::Unit,
    },
    infer::{
        columns::{InferedColumn, infer_column},
        csv_analysis::{CsvAnalysis, CsvStructure, HeaderInfo},
        datagrid::StringDataGrid,
        geo_guesser::likely_geo_columns,
        parsing::InferedValue,
        sensor_id::{detect_sensor_id, is_valid_uuid, SensorId},
    },
    storage::StorageInstance,
};
use num_traits::ToPrimitive;
use anyhow::{Result, anyhow};
use crate::ingestors::http::server::ParseMode;
use csv_async::AsyncReader;
use futures::{StreamExt, io};
use std::{collections::HashMap, sync::Arc};

// Type alias to simplify complex HashMap type for clippy
type SensorDataMap = HashMap<String, (Arc<Sensor>, Vec<(SensAppDateTime, InferedValue)>)>;

/// Enhanced CSV parser with strict and inference modes
pub async fn publish_csv<R: io::AsyncRead + Unpin + Send>(
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

    // Check for exact column formats (with or without unit column)
    if column_names == &["datetime", "sensor_name", "value"] {
        return parse_long_format_with_names(data_grid);
    }
    
    if column_names == &["datetime", "sensor_name", "value", "unit"] {
        return parse_long_format_with_names_and_units(data_grid);
    }
    
    if column_names == &["datetime", "sensor_uuid", "value"] {
        return parse_long_format_with_uuids(data_grid);
    }
    
    if column_names == &["datetime", "sensor_uuid", "value", "unit"] {
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
    // Use the new CsvAnalysis for comprehensive inference
    // Note: Headers are already extracted by the CSV reader and set in data_grid.column_names
    // Create a custom analysis that uses the existing headers instead of trying to detect them
    let analysis = create_analysis_with_existing_headers(&data_grid)?;
    
    // Debug output
    println!("DEBUG: CSV inference mode");
    println!("DEBUG: Column names: {:?}", data_grid.column_names);
    println!("DEBUG: Row count: {}", data_grid.rows.len());
    println!("DEBUG: First few rows: {:?}", data_grid.rows.iter().take(3).collect::<Vec<_>>());
    println!("DEBUG: Detected structure: {:?}", analysis.structure);
    println!("DEBUG: DateTime column: {:?}", analysis.datetime_column);
    println!("DEBUG: Sensor ID column: {:?}", analysis.sensor_id_column);
    println!("DEBUG: Value column: {:?}", analysis.value_column);
    
    // Use the original data_grid since headers are already properly set by CSV reader
    let processed_grid = data_grid;

    // Parse based on detected structure
    match analysis.structure {
        CsvStructure::Long => {
            if let (Some(sensor_id_col), Some(value_col)) = (analysis.sensor_id_column, analysis.value_column) {
                let datetime_col = analysis.datetime_column.unwrap_or(0);
                parse_long_format_with_inference(processed_grid, datetime_col, sensor_id_col, value_col, analysis.unit_column, &analysis)
            } else {
                // Fallback to generic parsing
                parse_csv_data_grid_with_analysis(processed_grid, &analysis)
            }
        }
        CsvStructure::Wide => {
            parse_wide_format_with_inference(processed_grid, &analysis)
        }
        CsvStructure::SingleSensor => {
            parse_single_sensor_format(processed_grid, &analysis)
        }
    }
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

        let value = parse_value_from_analysis(&inferred_columns[value_idx], row_idx)?;
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
            let value = parse_value_from_analysis(&inferred_columns[col_idx], row_idx)?;
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



/// Parse long format CSV with inference analysis
fn parse_long_format_with_inference(
    data_grid: StringDataGrid,
    datetime_idx: usize,
    sensor_idx: usize,
    value_idx: usize,
    unit_idx: Option<usize>,
    analysis: &CsvAnalysis,
) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    let rows = &data_grid.rows;

    if rows.is_empty() {
        return Err(anyhow!("CSV contains no data rows"));
    }

    let mut sensors_data: SensorDataMap = HashMap::new();

    // Process each row
    for (row_idx, row) in rows.iter().enumerate() {
        let datetime = parse_datetime_from_inferred(&analysis.inferred_columns[datetime_idx], row_idx)?;
        
        let sensor_identifier = row[sensor_idx].clone();
        if sensor_identifier.trim().is_empty() {
            return Err(anyhow!("Empty sensor identifier found in row {}", row_idx + 1));
        }

        // Use our new sensor ID detection
        let sensor_id = detect_sensor_id(&sensor_identifier);
        
        // Create sensor key using the original identifier
        let sensor_key = sensor_identifier.clone();
        
        // Parse value from inferred column type
        let inferred_value = parse_value_from_analysis(&analysis.inferred_columns[value_idx], row_idx)?;
        
        // Get or create sensor
        if !sensors_data.contains_key(&sensor_key) {
            // Create new sensor based on detected ID type
            let sensor = match sensor_id {
                SensorId::Uuid(uuid) => Arc::new(Sensor::new(
                    uuid,
                    sensor_identifier.clone(),
                    SensorType::Float,
                    unit_idx.map(|idx| Unit::new(row[idx].to_string(), None)),
                    None,
                )),
                SensorId::Name(name) => Arc::new(Sensor::new_without_uuid(
                    name,
                    SensorType::Float,
                    unit_idx.map(|idx| Unit::new(row[idx].to_string(), None)),
                    None,
                )?),
            };
            sensors_data.insert(sensor_key.clone(), (sensor, Vec::new()));
        }
        
        // Add sample to sensor
        sensors_data.get_mut(&sensor_key).unwrap().1.push((datetime, inferred_value));
    }

    // Convert to final format
    convert_sensor_data_map_to_typed_samples(sensors_data)
}

/// Parse wide format CSV with inference analysis
fn parse_wide_format_with_inference(
    data_grid: StringDataGrid,
    analysis: &CsvAnalysis,
) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    let column_names = &data_grid.column_names;
    let rows = &data_grid.rows;

    if rows.is_empty() {
        return Err(anyhow!("CSV contains no data rows"));
    }

    let datetime_col = analysis.datetime_column.unwrap_or(0);
    let mut sensors_data: SensorDataMap = HashMap::new();

    // Process each row
    for (row_idx, row) in rows.iter().enumerate() {
        let datetime = parse_datetime_from_inferred(&analysis.inferred_columns[datetime_col], row_idx)?;
        
        // Process each value column (skip datetime column)
        for (col_idx, column_name) in column_names.iter().enumerate() {
            if col_idx == datetime_col {
                continue; // Skip datetime column
            }

            // Skip geo coordinate columns from being treated as sensors
            if let Some(ref geo) = analysis.geo_columns {
                if column_name == &geo.lat || column_name == &geo.lon {
                    continue;
                }
            }

            let value_str = &row[col_idx];
            if value_str.trim().is_empty() {
                continue; // Skip empty values
            }

            // Parse value from inferred column type
            let inferred_value = parse_value_from_analysis(&analysis.inferred_columns[col_idx], row_idx)?;
            
            // Get or create sensor for this column
            if !sensors_data.contains_key(column_name) {
                let sensor = Arc::new(Sensor::new_without_uuid(
                    column_name.clone(),
                    SensorType::Float,
                    None, // No unit info in wide format
                    None,
                )?);
                sensors_data.insert(column_name.clone(), (sensor, Vec::new()));
            }
            
            // Add sample to sensor
            sensors_data.get_mut(column_name).unwrap().1.push((datetime, inferred_value));
        }
    }

    // Convert to final format
    convert_sensor_data_map_to_typed_samples(sensors_data)
}

/// Parse single sensor format CSV
fn parse_single_sensor_format(
    data_grid: StringDataGrid,
    analysis: &CsvAnalysis,
) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    let column_names = &data_grid.column_names;
    let rows = &data_grid.rows;

    if rows.is_empty() {
        return Err(anyhow!("CSV contains no data rows"));
    }

    if column_names.len() != 2 {
        return Err(anyhow!("Single sensor format requires exactly 2 columns"));
    }

    let datetime_col = analysis.datetime_column.unwrap_or(0);
    let value_col = if datetime_col == 0 { 1 } else { 0 };
    
    let sensor_name = column_names[value_col].clone();
    let sensor = Arc::new(Sensor::new_without_uuid(sensor_name.clone(), SensorType::Float, None, None)?);
    
    let mut samples = Vec::new();
    
    // Process each row
    for (row_idx, _row) in rows.iter().enumerate() {
        let datetime = parse_datetime_from_inferred(&analysis.inferred_columns[datetime_col], row_idx)?;
        let inferred_value = parse_value_from_analysis(&analysis.inferred_columns[value_col], row_idx)?;
        samples.push((datetime, inferred_value));
    }

    let typed_samples = convert_samples_to_typed_samples(samples, &SensorType::String)?;
    let mut result = HashMap::new();
    result.insert(sensor_name, (sensor, typed_samples));
    Ok(result)
}

/// Fallback parser using analysis but with generic structure detection
fn parse_csv_data_grid_with_analysis(
    data_grid: StringDataGrid,
    analysis: &CsvAnalysis,
) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    // Use analysis information to improve parsing
    println!("DEBUG: Using fallback parser with analysis");
    let column_names = &data_grid.column_names;
    let rows = &data_grid.rows;

    if rows.is_empty() {
        return Err(anyhow!("CSV contains no data rows"));
    }

    let datetime_idx = analysis.datetime_column;
    let sensor_name_idx = analysis.sensor_id_column;
    let value_idx = analysis.value_column;
    // For unit, we need to detect it manually since analysis doesn't track it
    let unit_idx = column_names.iter().position(|name| name.to_lowercase() == "unit");

    let mut sensors_data: SensorDataMap = HashMap::new();

    // Process each row
    for (row_idx, row) in rows.iter().enumerate() {
        let datetime = if let Some(dt_idx) = datetime_idx {
            parse_datetime_from_inferred(&analysis.inferred_columns[dt_idx], row_idx)?
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

            let value = parse_value_from_analysis(&analysis.inferred_columns[value_idx], row_idx)?;
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

                let value = parse_value_from_analysis(&analysis.inferred_columns[col_idx], row_idx)?;
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
    convert_sensor_data_map_to_typed_samples(sensors_data)
}

/// Convert SensorDataMap to HashMap with TypedSamples
fn convert_sensor_data_map_to_typed_samples(
    sensors_data: SensorDataMap,
) -> Result<HashMap<String, (Arc<Sensor>, TypedSamples)>> {
    let mut result = HashMap::new();
    
    for (sensor_name, (sensor, samples)) in sensors_data {
        let typed_samples = convert_analysis_samples_to_typed_samples(samples)?;
        result.insert(sensor_name, (sensor, typed_samples));
    }
    
    Ok(result)
}

/// Convert samples with inferred values to TypedSamples
fn convert_analysis_samples_to_typed_samples(
    samples: Vec<(SensAppDateTime, InferedValue)>,
) -> Result<TypedSamples> {
    if samples.is_empty() {
        return Ok(TypedSamples::Integer(smallvec::smallvec![]));
    }

    // Build SmallVec for each sample type
    let mut integer_samples = smallvec::SmallVec::new();
    let mut float_samples = smallvec::SmallVec::new();
    let mut string_samples = smallvec::SmallVec::new();
    let mut boolean_samples = smallvec::SmallVec::new();
    let mut json_samples = smallvec::SmallVec::new();

    // Determine the dominant type from the first sample
    let first_type = &samples[0].1;
    match first_type {
        InferedValue::Integer(_) => {
            for (datetime, value) in samples {
                if let InferedValue::Integer(val) = value {
                    integer_samples.push(Sample { datetime, value: val });
                } else {
                    return Err(anyhow!("Mixed data types in samples - expected integer"));
                }
            }
            Ok(TypedSamples::Integer(integer_samples))
        }
        InferedValue::Float(_) | InferedValue::Numeric(_) => {
            for (datetime, value) in samples {
                match value {
                    InferedValue::Float(val) => {
                        float_samples.push(Sample { datetime, value: val });
                    }
                    InferedValue::Integer(val) => {
                        float_samples.push(Sample { datetime, value: val as f64 });
                    }
                    InferedValue::Numeric(val) => {
                        let float_val = val.to_f64().unwrap_or(0.0);
                        float_samples.push(Sample { datetime, value: float_val });
                    }
                    _ => {
                        return Err(anyhow!("Mixed data types in samples - expected numeric"));
                    }
                }
            }
            Ok(TypedSamples::Float(float_samples))
        }
        InferedValue::String(_) | InferedValue::DateTime(_) => {
            for (datetime, value) in samples {
                match value {
                    InferedValue::String(val) => {
                        string_samples.push(Sample { datetime, value: val });
                    }
                    InferedValue::DateTime(val) => {
                        string_samples.push(Sample { datetime, value: val.to_rfc3339() });
                    }
                    _ => {
                        return Err(anyhow!("Mixed data types in samples - expected string"));
                    }
                }
            }
            Ok(TypedSamples::String(string_samples))
        }
        InferedValue::Boolean(_) => {
            for (datetime, value) in samples {
                if let InferedValue::Boolean(val) = value {
                    boolean_samples.push(Sample { datetime, value: val });
                } else {
                    return Err(anyhow!("Mixed data types in samples - expected boolean"));
                }
            }
            Ok(TypedSamples::Boolean(boolean_samples))
        }
        InferedValue::Json(_) => {
            for (datetime, value) in samples {
                if let InferedValue::Json(val) = value {
                    json_samples.push(Sample { datetime, value: val.as_ref().clone() });
                } else {
                    return Err(anyhow!("Mixed data types in samples - expected JSON"));
                }
            }
            Ok(TypedSamples::Json(json_samples))
        }
    }
}

/// Parse a value from an inferred column at a specific row index
fn parse_value_from_analysis(column: &InferedColumn, row_idx: usize) -> Result<InferedValue> {
    match column {
        InferedColumn::Integer(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::Integer(values[row_idx]))
            } else {
                Err(anyhow!("Row index {} out of bounds for integer column", row_idx))
            }
        }
        InferedColumn::Float(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::Float(values[row_idx]))
            } else {
                Err(anyhow!("Row index {} out of bounds for float column", row_idx))
            }
        }
        InferedColumn::String(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::String(values[row_idx].clone()))
            } else {
                Err(anyhow!("Row index {} out of bounds for string column", row_idx))
            }
        }
        InferedColumn::Boolean(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::Boolean(values[row_idx]))
            } else {
                Err(anyhow!("Row index {} out of bounds for boolean column", row_idx))
            }
        }
        InferedColumn::DateTime(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::DateTime(values[row_idx]))
            } else {
                Err(anyhow!("Row index {} out of bounds for datetime column", row_idx))
            }
        }
        InferedColumn::Numeric(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::Numeric(values[row_idx]))
            } else {
                Err(anyhow!("Row index {} out of bounds for numeric column", row_idx))
            }
        }
        InferedColumn::Json(values) => {
            if row_idx < values.len() {
                Ok(InferedValue::Json(values[row_idx].clone()))
            } else {
                Err(anyhow!("Row index {} out of bounds for JSON column", row_idx))
            }
        }
    }
}


/// Create a CsvAnalysis using existing headers instead of trying to detect them
/// This is needed because the CSV reader already extracted the headers
fn create_analysis_with_existing_headers(data_grid: &StringDataGrid) -> Result<CsvAnalysis> {
    if data_grid.is_empty() {
        return Err(anyhow!("Cannot analyze empty CSV data"));
    }

    // Use the existing column names from the data_grid (already set by CSV reader)
    let column_names = data_grid.column_names.clone();
    
    // Create header info indicating we already have headers
    let header_info = HeaderInfo {
        has_headers: true,
        headers: column_names.clone(),
        confidence: 1.0, // We're certain since they came from CSV reader
    };

    // Infer column types by building columns from data
    let mut columns = vec![Vec::new(); column_names.len()];
    for row in &data_grid.rows {
        for (col_idx, value) in row.iter().enumerate() {
            if col_idx < columns.len() {
                columns[col_idx].push(value.clone());
            }
        }
    }

    let inferred_columns: Vec<InferedColumn> = columns
        .iter()
        .map(|col| infer_column(col.clone(), false, false))
        .collect();

    // Detect geo columns
    let geo_columns = likely_geo_columns(&column_names, &inferred_columns);

    // Find datetime column using smart detection
    use crate::infer::datetime_guesser::likely_datetime_column;
    let datetime_column = if let Some(best_column_name) = likely_datetime_column(&column_names, &inferred_columns) {
        column_names.iter().position(|name| name == &best_column_name)
    } else {
        None
    };

    // Detect structure using the correct column names
    let structure = detect_csv_structure_simple(&column_names, &inferred_columns);

    // For long format, detect sensor ID and value columns
    let (sensor_id_column, value_column) = match structure {
        CsvStructure::Long => detect_long_format_columns_simple(&column_names, &inferred_columns),
        _ => (None, None),
    };

    // Detect unit column for long format using smart detection
    let unit_column = match structure {
        CsvStructure::Long => {
            // Use the same smart unit detection from CsvAnalysis
            column_names.iter().enumerate().find_map(|(idx, name)| {
                let name_lower = name.to_lowercase();
                if name_lower == "unit" || name_lower == "units" {
                    Some(idx)
                } else {
                    None
                }
            })
        },
        _ => None,
    };

    Ok(CsvAnalysis {
        header_info,
        structure,
        inferred_columns,
        geo_columns,
        datetime_column,
        sensor_id_column,
        value_column,
        unit_column,
    })
}


/// Detect the CSV structure type
fn detect_csv_structure_simple(
    column_names: &[String],
    inferred_columns: &[InferedColumn],
) -> CsvStructure {
    let column_count = column_names.len();
    
    // Single sensor: exactly 2 columns (timestamp, value)
    if column_count == 2 {
        return CsvStructure::SingleSensor;
    }

    // Look for sensor ID column indicators
    let has_sensor_id_column = column_names.iter().enumerate().any(|(idx, name)| {
        let name_lower = name.to_lowercase();
        let matches = (name_lower.contains("sensor") && (name_lower.contains("id") || name_lower.contains("name"))) ||
        name_lower == "sensor" ||
        name_lower == "id" ||
        name_lower == "name" ||
        // Check if column contains mixed sensor identifiers
        column_contains_sensor_identifiers_simple(inferred_columns.get(idx));
        
        println!("DEBUG: Column '{}' -> sensor_id check: {}", name, matches);
        matches
    });

    // Look for value column
    let has_value_column = column_names.iter().any(|name| {
        let name_lower = name.to_lowercase();
        let matches = name_lower.contains("value") || name_lower == "val";
        println!("DEBUG: Column '{}' -> value check: {}", name, matches);
        matches
    });

    println!("DEBUG: has_sensor_id_column: {}, has_value_column: {}", has_sensor_id_column, has_value_column);

    // Long format: has sensor ID column and value column
    if has_sensor_id_column && has_value_column {
        println!("DEBUG: Detected as Long format");
        CsvStructure::Long
    } else {
        // Default to wide format
        println!("DEBUG: Detected as Wide format");
        CsvStructure::Wide
    }
}

/// Detect long format columns (sensor ID and value)
fn detect_long_format_columns_simple(
    column_names: &[String],
    inferred_columns: &[InferedColumn],
) -> (Option<usize>, Option<usize>) {
    // Find sensor ID column
    let sensor_id_column = column_names.iter().enumerate().find_map(|(idx, name)| {
        let name_lower = name.to_lowercase();
        if (name_lower.contains("sensor") && (name_lower.contains("id") || name_lower.contains("name"))) ||
           name_lower == "sensor" ||
           name_lower == "id" ||
           name_lower == "name" ||
           column_contains_sensor_identifiers_simple(inferred_columns.get(idx)) {
            Some(idx)
        } else {
            None
        }
    });

    // Find value column
    let value_column = column_names.iter().enumerate().find_map(|(idx, name)| {
        let name_lower = name.to_lowercase();
        if name_lower.contains("value") || name_lower == "val" {
            Some(idx)
        } else {
            None
        }
    });

    (sensor_id_column, value_column)
}

/// Check if a column contains sensor identifier values
fn column_contains_sensor_identifiers_simple(column: Option<&InferedColumn>) -> bool {
    match column {
        Some(InferedColumn::String(values)) => {
            // Check if values look like sensor names or UUIDs
            values.iter().any(|value| {
                // Check for UUID pattern
                if value.len() == 36 && value.chars().filter(|&c| c == '-').count() == 4 {
                    return true;
                }
                // Check for sensor name patterns
                let lower = value.to_lowercase();
                lower.contains("sensor") || lower.contains("temp") || lower.contains("humid")
            })
        }
        _ => false,
    }
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

        let result = publish_csv(csv_reader, 1000, ParseMode::Strict, storage).await;
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

        let result = publish_csv(csv_reader, 1000, ParseMode::Strict, storage).await;
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

        let result = publish_csv(csv_reader, 1000, ParseMode::Strict, storage).await;
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

        let result = publish_csv(csv_reader, 1000, ParseMode::Strict, storage).await;
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

        let result = publish_csv(csv_reader, 1000, ParseMode::Strict, storage).await;
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

        let result = publish_csv(csv_reader, 1000, ParseMode::Infer, storage).await;
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

        let result = publish_csv(csv_reader, 1000, ParseMode::Strict, storage).await;
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
