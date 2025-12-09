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
use csv_async::AsyncReader;
use futures::{StreamExt, io};
use std::{collections::HashMap, sync::Arc};

// Type alias to simplify complex HashMap type for clippy
type SensorDataMap = HashMap<String, (Arc<Sensor>, Vec<(SensAppDateTime, InferedValue)>)>;

pub async fn publish_csv_async<R: io::AsyncRead + Unpin + Send>(
    mut csv_reader: AsyncReader<R>,
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
