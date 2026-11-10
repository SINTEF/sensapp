use crate::{
    datamodel::{
        Sample, SensAppDateTime, Sensor, SensorType, TypedSamples, batch_builder::BatchBuilder,
        unit::Unit,
    },
    importers::IngestionStats,
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

/// Reads, parses and publishes a CSV stream.
pub async fn publish_csv_async<R: io::AsyncRead + Unpin + Send>(
    csv_reader: AsyncReader<R>,
    storage: Arc<dyn StorageInstance>,
) -> Result<IngestionStats> {
    let parsed = parse_csv_data_grid(read_csv_data_grid(csv_reader).await?)?;
    publish_parsed_csv(parsed, storage).await
}

/// Reads all the CSV records.
pub async fn read_csv_data_grid<R: io::AsyncRead + Unpin + Send>(
    mut csv_reader: AsyncReader<R>,
) -> Result<StringDataGrid> {
    let headers = csv_reader.headers().await?.clone();
    let column_names = headers.iter().map(|s| s.to_string()).collect::<Vec<_>>();

    let mut rows = Vec::new();
    let mut records = csv_reader.records();

    while let Some(record) = records.next().await {
        let record = record?;
        let row = record.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        rows.push(row);
    }

    StringDataGrid::new(column_names, rows)
}

/// Publishes the series parsed from a CSV.
pub async fn publish_parsed_csv(
    parsed_data: ParsedCsv,
    storage: Arc<dyn StorageInstance>,
) -> Result<IngestionStats> {
    let series = parsed_data.len();
    let samples = parsed_data.values().map(|(_, values)| values.len()).sum();

    let mut batch_builder = BatchBuilder::new()?;
    for (_sensor_name, (sensor, samples)) in parsed_data {
        batch_builder.add(sensor, samples).await?;
    }
    batch_builder.send_what_is_left(storage).await?;

    Ok(IngestionStats::new(series, samples))
}

/// The series of a CSV, by name.
pub type ParsedCsv = HashMap<String, (Arc<Sensor>, TypedSamples)>;

/// Parse CSV data grid into sensors and their samples
/// Errors here come from the payload.
pub fn parse_csv_data_grid(data_grid: StringDataGrid) -> Result<ParsedCsv> {
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

    // Wide format: each column is a series named after its header, so a header given twice would
    // put two columns, possibly of two types, in one series
    let long = sensor_name_idx.is_some() && value_idx.is_some();
    if !long {
        let mut seen = std::collections::HashSet::new();
        if let Some(duplicate) = column_names.iter().find(|name| !seen.insert(name.as_str())) {
            return Err(anyhow!("The column '{duplicate}' is given twice"));
        }
    }

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

            let unit = unit_name.map(|name| Unit::new(name, None));
            series_entry(&mut sensors_data, sensor_name, &value, unit)?.push((datetime, value));
        } else if datetime_idx.is_some() {
            // Wide format: each column (except datetime) is a sensor
            let mut sensor_count = 0;
            for (col_idx, col_name) in column_names.iter().enumerate() {
                if Some(col_idx) == datetime_idx {
                    continue; // Skip datetime column
                }

                let value = parse_value_from_inferred(&inferred_columns[col_idx], row_idx)?;
                let sensor_name = col_name.clone();

                series_entry(&mut sensors_data, sensor_name, &value, None)?.push((datetime, value));
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

/// The samples of the series `name`, created with the type of `value` when it is new. A name the
/// series identity refuses (a control character) is an error of the payload.
fn series_entry<'a>(
    sensors_data: &'a mut SensorDataMap,
    name: String,
    value: &InferedValue,
    unit: Option<Unit>,
) -> Result<&'a mut Vec<(SensAppDateTime, InferedValue)>> {
    let entry = match sensors_data.entry(name) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let sensor_type = inferred_value_to_sensor_type(value);
            let sensor = Sensor::new_without_uuid(entry.key().clone(), sensor_type, unit, None)?;
            entry.insert((Arc::new(sensor), Vec::new()))
        }
    };
    Ok(&mut entry.1)
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
                    InferedValue::Integer(val) => Ok(Sample {
                        datetime,
                        value: val,
                    }),
                    other => Err(type_mismatch(sensor_type, &other)),
                })
                .collect::<Result<_>>()?;
            Ok(TypedSamples::Integer(typed_samples))
        }
        SensorType::Float => {
            let typed_samples = samples
                .into_iter()
                .map(|(datetime, value)| match value {
                    InferedValue::Float(val) => Ok(Sample {
                        datetime,
                        value: val,
                    }),
                    other => Err(type_mismatch(sensor_type, &other)),
                })
                .collect::<Result<_>>()?;
            Ok(TypedSamples::Float(typed_samples))
        }
        SensorType::Numeric => {
            let typed_samples = samples
                .into_iter()
                .map(|(datetime, value)| match value {
                    InferedValue::Numeric(val) => Ok(Sample {
                        datetime,
                        value: val,
                    }),
                    other => Err(type_mismatch(sensor_type, &other)),
                })
                .collect::<Result<_>>()?;
            Ok(TypedSamples::Numeric(typed_samples))
        }
        SensorType::String => {
            let typed_samples = samples
                .into_iter()
                .map(|(datetime, value)| {
                    let string_value = match value {
                        InferedValue::String(val) => val,
                        InferedValue::DateTime(val) => val.to_rfc3339(),
                        other => return Err(type_mismatch(sensor_type, &other)),
                    };
                    Ok(Sample {
                        datetime,
                        value: string_value,
                    })
                })
                .collect::<Result<_>>()?;
            Ok(TypedSamples::String(typed_samples))
        }
        SensorType::Boolean => {
            let typed_samples = samples
                .into_iter()
                .map(|(datetime, value)| match value {
                    InferedValue::Boolean(val) => Ok(Sample {
                        datetime,
                        value: val,
                    }),
                    other => Err(type_mismatch(sensor_type, &other)),
                })
                .collect::<Result<_>>()?;
            Ok(TypedSamples::Boolean(typed_samples))
        }
        SensorType::Json => {
            let typed_samples = samples
                .into_iter()
                .map(|(datetime, value)| match value {
                    InferedValue::Json(val) => Ok(Sample {
                        datetime,
                        value: (*val).clone(),
                    }),
                    other => Err(type_mismatch(sensor_type, &other)),
                })
                .collect::<Result<_>>()?;
            Ok(TypedSamples::Json(typed_samples))
        }
        _ => Err(anyhow!("Unsupported sensor type: {:?}", sensor_type)),
    }
}

fn type_mismatch(sensor_type: &SensorType, value: &InferedValue) -> anyhow::Error {
    anyhow!("A {sensor_type} series cannot hold the value {value:?}")
}
