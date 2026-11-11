use crate::{
    datamodel::{
        Sample, SensAppDateTime, Sensor, SensorType, TypedSamples, batch_builder::BatchBuilder,
        sensapp_datetime::SensAppDateTimeExt, unit::Unit,
    },
    importers::IngestionStats,
    infer::{
        columns::{InferedColumn, infer_typed_column},
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
    let (parsed, skipped) = parse_csv_data_grid(read_csv_data_grid(csv_reader).await?)?;
    publish_parsed_csv(parsed, skipped, storage).await
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
    skipped: usize,
    storage: Arc<dyn StorageInstance>,
) -> Result<IngestionStats> {
    let series = parsed_data.len();
    let samples = parsed_data.values().map(|(_, values)| values.len()).sum();

    let mut batch_builder = BatchBuilder::new()?;
    for (_sensor_name, (sensor, samples)) in parsed_data {
        batch_builder.add(sensor, samples).await?;
    }
    batch_builder.send_what_is_left(storage).await?;

    Ok(IngestionStats::new(series, samples).with_skipped(skipped))
}

/// The series of a CSV, by name.
pub type ParsedCsv = HashMap<String, (Arc<Sensor>, TypedSamples)>;

/// Whether a cell says that there is no reading: empty, or `NaN`, `null`, `NA`, `N/A` in any case.
fn is_missing_marker(cell: &str) -> bool {
    let cell = cell.trim();
    cell.is_empty()
        || ["nan", "null", "na", "n/a"]
            .iter()
            .any(|marker| cell.eq_ignore_ascii_case(marker))
}

/// Parse CSV data grid into sensors and their samples, and count the cells that were skipped.
/// Errors here come from the payload.
///
/// A cell that says there is no reading (empty, `NaN`, `null`, `NA`, `N/A`) in a column that is
/// otherwise numeric is no sample: it is skipped and counted, and the column stays numeric. In any
/// other column those cells are text, as they were.
///
/// The cells are moved, never copied: out of the rows into the columns, then out of the columns
/// into the samples. A column keeps its text only when it is read as text.
pub fn parse_csv_data_grid(data_grid: StringDataGrid) -> Result<(ParsedCsv, usize)> {
    let StringDataGrid { column_names, rows } = data_grid;
    let column_names = &column_names;

    if rows.is_empty() {
        return Err(anyhow!("CSV contains no data rows"));
    }

    if column_names.len() < 2 {
        return Err(anyhow!(
            "CSV must have at least 2 columns (datetime and values)"
        ));
    }

    // Find sensor name and value columns
    let sensor_name_idx =
        find_column_index(column_names, &["sensor_name", "metric", "name", "sensor"]);
    let value_idx = find_column_index(column_names, &["value", "reading", "measurement"]);
    let unit_idx = find_column_index(column_names, &["unit", "units"]);

    // Wide format: each column is a series named after its header, so a header given twice would
    // put two columns, possibly of two types, in one series
    let long = sensor_name_idx.is_some() && value_idx.is_some();
    if !long {
        let mut seen = std::collections::HashSet::new();
        if let Some(duplicate) = column_names.iter().find(|name| !seen.insert(name.as_str())) {
            return Err(anyhow!("The column '{duplicate}' is given twice"));
        }
    }

    // Rows to columns, for type inference
    let row_count = rows.len();
    let mut cells: Vec<Vec<String>> = (0..column_names.len())
        .map(|_| Vec::with_capacity(row_count))
        .collect();
    for row in rows {
        for (column, value) in cells.iter_mut().zip(row) {
            column.push(value);
        }
    }

    // Infer column types. The text of a typed column is kept when it is read as text: the sensor
    // names and units, and the times of a value column.
    let mut inferred_columns: Vec<InferedColumn> = Vec::with_capacity(cells.len());
    let mut texts: Vec<Option<Vec<String>>> = Vec::with_capacity(cells.len());
    // The cells of each numeric column that held no reading
    let mut gaps: Vec<Option<Vec<bool>>> = Vec::with_capacity(cells.len());
    for (col_idx, mut column) in cells.into_iter().enumerate() {
        let gap_cells: Vec<usize> = if long && [sensor_name_idx, unit_idx].contains(&Some(col_idx))
        {
            Vec::new()
        } else {
            (0..column.len())
                .filter(|&row| is_missing_marker(&column[row]))
                .collect()
        };
        // Some cells, not all: a gap stands in as `0` while the column is typed, and the column
        // is numeric only if what is left is
        let mut column_gaps = None;
        let mut typed_with_gaps = None;
        if !gap_cells.is_empty() && gap_cells.len() < column.len() {
            let saved: Vec<String> = gap_cells
                .iter()
                .map(|&row| std::mem::replace(&mut column[row], "0".to_string()))
                .collect();
            match infer_typed_column(&column, true, false) {
                Some(
                    typed @ (InferedColumn::Integer(_)
                    | InferedColumn::Float(_)
                    | InferedColumn::Numeric(_)),
                ) => {
                    let mut mask = vec![false; column.len()];
                    for &row in &gap_cells {
                        mask[row] = true;
                    }
                    column_gaps = Some(mask);
                    typed_with_gaps = Some(typed);
                }
                _ => {
                    for (&row, cell) in gap_cells.iter().zip(saved) {
                        column[row] = cell;
                    }
                }
            }
        }
        gaps.push(column_gaps);
        let typed = typed_with_gaps.or_else(|| infer_typed_column(&column, true, false));
        match typed {
            None => {
                inferred_columns.push(InferedColumn::String(column));
                texts.push(None);
            }
            Some(typed) => {
                let read_as_text = (long && [sensor_name_idx, unit_idx].contains(&Some(col_idx)))
                    || matches!(typed, InferedColumn::DateTime(_));
                inferred_columns.push(typed);
                texts.push(read_as_text.then_some(column));
            }
        }
    }

    // Try to identify datetime column
    let datetime_column = likely_datetime_column(column_names, &inferred_columns);
    let datetime_idx = datetime_column
        .as_ref()
        .and_then(|name| column_names.iter().position(|col| col == name));

    let Some(datetime_idx) = datetime_idx else {
        return Err(anyhow!(
            "No time column found among the columns {column_names:?}. Name it datetime or \
             timestamp, with ISO 8601 times (2024-06-01T12:00:00Z) or Unix epoch numbers"
        ));
    };
    if let Some(row) = gaps[datetime_idx]
        .as_ref()
        .and_then(|mask| mask.iter().position(|&gap| gap))
    {
        return Err(anyhow!(
            "The time column '{}' has no time in row {}",
            column_names[datetime_idx],
            row + 1
        ));
    }
    let times = column_times(&inferred_columns[datetime_idx], &column_names[datetime_idx])?;

    let mut sensors_data: SensorDataMap = HashMap::new();
    let mut skipped = 0_usize;
    let is_gap = |gaps: &[Option<Vec<bool>>], column: usize, row: usize| {
        gaps[column].as_ref().is_some_and(|mask| mask[row])
    };

    // Process each row
    for (row_idx, &datetime) in times.iter().enumerate() {
        if let (Some(sensor_name_idx), Some(value_idx)) = (sensor_name_idx, value_idx) {
            // Long format: one row per sample
            if is_gap(&gaps, value_idx, row_idx) {
                skipped += 1;
                continue;
            }
            let value = take_value(
                &mut inferred_columns[value_idx],
                &mut texts[value_idx],
                row_idx,
            )?;
            let sensor_name = cell_text(
                &inferred_columns[sensor_name_idx],
                &texts[sensor_name_idx],
                row_idx,
            )?;
            if sensor_name.trim().is_empty() {
                return Err(anyhow!("Empty sensor name found in row {}", row_idx + 1));
            }
            // The unit is read only for a new series
            let unit = || -> Result<Option<Unit>> {
                let Some(unit_idx) = unit_idx else {
                    return Ok(None);
                };
                let unit_name = cell_text(&inferred_columns[unit_idx], &texts[unit_idx], row_idx)?;
                Ok((!unit_name.is_empty()).then(|| Unit::new(unit_name.to_string(), None)))
            };
            series_entry(&mut sensors_data, sensor_name, &value, unit)?.push((datetime, value));
        } else {
            // Wide format: each column (except datetime) is a sensor
            for (col_idx, col_name) in column_names.iter().enumerate() {
                if col_idx == datetime_idx {
                    continue; // Skip datetime column
                }

                if is_gap(&gaps, col_idx, row_idx) {
                    skipped += 1;
                    continue;
                }
                let value =
                    take_value(&mut inferred_columns[col_idx], &mut texts[col_idx], row_idx)?;
                series_entry(&mut sensors_data, col_name, &value, || Ok(None))?
                    .push((datetime, value));
            }
        }
    }
    drop(inferred_columns);
    drop(texts);

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

    Ok((result, skipped))
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

/// The times of the time column. Epoch numbers are seconds, milliseconds, microseconds or
/// nanoseconds, after the largest of the column: under 10^11 (year 5138 in seconds) seconds, under
/// 10^14 milliseconds, under 10^17 microseconds, nanoseconds above.
fn column_times(column: &InferedColumn, name: &str) -> Result<Vec<SensAppDateTime>> {
    let micros_per_unit = |largest: f64| -> i64 {
        match largest {
            x if x < 1e11 => 1_000_000,
            x if x < 1e14 => 1_000,
            x if x < 1e17 => 1,
            _ => 0, // nanoseconds: divided below
        }
    };
    let out_of_range = || anyhow!("The time column '{name}' holds a time outside years 1 to 9999");
    match column {
        InferedColumn::DateTime(values) => Ok(values.clone()),
        InferedColumn::Integer(values) => {
            let largest = values.iter().map(|v| v.unsigned_abs()).max().unwrap_or(0);
            let factor = micros_per_unit(largest as f64);
            values
                .iter()
                .map(|&value| {
                    let micros = if factor == 0 {
                        Some(value.div_euclid(1000))
                    } else {
                        value.checked_mul(factor)
                    };
                    micros
                        .map(SensAppDateTime::from_unix_microseconds_i64)
                        .ok_or_else(out_of_range)
                })
                .collect()
        }
        InferedColumn::Float(values) => {
            let largest = values.iter().fold(0.0_f64, |max, v| max.max(v.abs()));
            let factor = match micros_per_unit(largest) {
                0 => 1e-3,
                factor => factor as f64,
            };
            values
                .iter()
                .map(|&value| {
                    // Rounded: 1717243200.123456 × 10^6 is 1717243200123455.9 in floating point
                    let micros = (value * factor).round();
                    // The range check of the samples refuses what is out of years 1 to 9999
                    if micros.is_finite() && micros.abs() < 9.2e18 {
                        Ok(SensAppDateTime::from_unix_microseconds_i64(micros as i64))
                    } else {
                        Err(out_of_range())
                    }
                })
                .collect()
        }
        _ => Err(anyhow!(
            "The time column '{name}' holds neither ISO 8601 times nor epoch numbers"
        )),
    }
}

/// The value of a cell, moved out of its column: each cell is read once. Times in a value column
/// are text: they are kept as written.
fn take_value(
    column: &mut InferedColumn,
    text: &mut Option<Vec<String>>,
    row_idx: usize,
) -> Result<InferedValue> {
    let out_of_bounds = || anyhow!("Row index {} out of bounds", row_idx);
    Ok(match column {
        InferedColumn::Integer(values) => {
            InferedValue::Integer(*values.get(row_idx).ok_or_else(out_of_bounds)?)
        }
        InferedColumn::Float(values) => {
            InferedValue::Float(*values.get(row_idx).ok_or_else(out_of_bounds)?)
        }
        InferedColumn::Numeric(values) => {
            InferedValue::Numeric(*values.get(row_idx).ok_or_else(out_of_bounds)?)
        }
        InferedColumn::Boolean(values) => {
            InferedValue::Boolean(*values.get(row_idx).ok_or_else(out_of_bounds)?)
        }
        InferedColumn::Json(values) => {
            InferedValue::Json(values.get(row_idx).ok_or_else(out_of_bounds)?.clone())
        }
        InferedColumn::String(values) => InferedValue::String(std::mem::take(
            values.get_mut(row_idx).ok_or_else(out_of_bounds)?,
        )),
        InferedColumn::DateTime(_) => InferedValue::String(std::mem::take(
            text.as_mut()
                .and_then(|text| text.get_mut(row_idx))
                .ok_or_else(out_of_bounds)?,
        )),
    })
}

/// The text of a cell of a column read as text.
fn cell_text<'a>(
    column: &'a InferedColumn,
    text: &'a Option<Vec<String>>,
    row_idx: usize,
) -> Result<&'a str> {
    let cells = match (text, column) {
        (Some(text), _) => text,
        (None, InferedColumn::String(values)) => values,
        (None, _) => return Err(anyhow!("The column of row {} has no text", row_idx + 1)),
    };
    cells
        .get(row_idx)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("Row index {} out of bounds", row_idx))
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

/// The samples of the series `name`, created with the type of `value` and the unit of `unit` when
/// it is new. A name the series identity refuses (a control character) is an error of the payload.
fn series_entry<'a>(
    sensors_data: &'a mut SensorDataMap,
    name: &str,
    value: &InferedValue,
    unit: impl FnOnce() -> Result<Option<Unit>>,
) -> Result<&'a mut Vec<(SensAppDateTime, InferedValue)>> {
    if !sensors_data.contains_key(name) {
        let sensor_type = inferred_value_to_sensor_type(value);
        let sensor = Sensor::new_without_uuid(name.to_string(), sensor_type, unit()?, None)?;
        sensors_data.insert(name.to_string(), (Arc::new(sensor), Vec::new()));
    }
    let (_, samples) = sensors_data
        .get_mut(name)
        .expect("the series was inserted above");
    Ok(samples)
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
    // The start of the value only: a cell can be as long as the body
    let value: String = format!("{value:?}").chars().take(80).collect();
    anyhow!("A {sensor_type} series cannot hold the value {value}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grid(columns: &[&str], rows: &[&[&str]]) -> StringDataGrid {
        // The UUID of a series is derived with the configured salt
        _ = crate::config::load_configuration_for_tests();
        StringDataGrid::new(
            columns.iter().map(|name| name.to_string()).collect(),
            rows.iter()
                .map(|row| row.iter().map(|cell| cell.to_string()).collect())
                .collect(),
        )
        .unwrap()
    }

    fn series<'a>(parsed: &'a ParsedCsv, name: &str) -> &'a TypedSamples {
        &parsed.get(name).unwrap_or_else(|| panic!("{name}")).1
    }

    /// A gap is no reading: pandas writes it as an empty cell or `NaN`, others as `NA`, `N/A` or
    /// `null`. One of them used to turn the whole column into text, and with it into a String
    /// series of numbers as text.
    #[test]
    fn gaps_in_a_numeric_column_are_skipped_and_counted() {
        let (parsed, skipped) = parse_csv_data_grid(grid(
            &["datetime", "temperature", "count"],
            &[
                &["2024-06-01T00:00:00Z", "1.5", "1"],
                &["2024-06-01T00:00:01Z", "", "NA"],
                &["2024-06-01T00:00:02Z", "NaN", "N/A"],
                &["2024-06-01T00:00:03Z", "nan", "null"],
                &["2024-06-01T00:00:04Z", "NULL", "5"],
                &["2024-06-01T00:00:05Z", "2.5", "6"],
            ],
        ))
        .unwrap();
        // 4 gaps in `temperature`, 3 in `count`
        assert_eq!(skipped, 7);
        match series(&parsed, "temperature") {
            TypedSamples::Float(samples) => {
                let values: Vec<f64> = samples.iter().map(|sample| sample.value).collect();
                assert_eq!(values, [1.5, 2.5]);
            }
            other => panic!("temperature is not Float: {other:?}"),
        }
        // The gaps do not change the type of a column of integers
        match series(&parsed, "count") {
            TypedSamples::Integer(samples) => {
                let values: Vec<i64> = samples.iter().map(|sample| sample.value).collect();
                assert_eq!(values, [1, 5, 6]);
            }
            other => panic!("count is not Integer: {other:?}"),
        }
    }

    #[test]
    fn gaps_in_the_value_column_of_a_long_csv_are_skipped() {
        let (parsed, skipped) = parse_csv_data_grid(grid(
            &["datetime", "sensor_name", "value"],
            &[
                &["2024-06-01T00:00:00Z", "a", "1"],
                &["2024-06-01T00:00:01Z", "a", ""],
                &["2024-06-01T00:00:02Z", "b", "NaN"],
                &["2024-06-01T00:00:03Z", "b", "4"],
            ],
        ))
        .unwrap();
        assert_eq!(skipped, 2);
        assert_eq!(series(&parsed, "a").len(), 1);
        assert_eq!(series(&parsed, "b").len(), 1);
    }

    /// The infinities are values.
    #[test]
    fn infinities_are_stored() {
        let (parsed, skipped) = parse_csv_data_grid(grid(
            &["datetime", "level"],
            &[
                &["2024-06-01T00:00:00Z", "inf"],
                &["2024-06-01T00:00:01Z", "-inf"],
                &["2024-06-01T00:00:02Z", "+Infinity"],
                &["2024-06-01T00:00:03Z", "1.5"],
            ],
        ))
        .unwrap();
        assert_eq!(skipped, 0);
        match series(&parsed, "level") {
            TypedSamples::Float(samples) => {
                let values: Vec<f64> = samples.iter().map(|sample| sample.value).collect();
                assert_eq!(
                    values,
                    [f64::INFINITY, f64::NEG_INFINITY, f64::INFINITY, 1.5]
                );
            }
            other => panic!("not Float: {other:?}"),
        }
    }

    /// In a column that is not numeric the same words are text, as they were.
    #[test]
    fn markers_in_a_text_column_stay_text() {
        let (parsed, skipped) = parse_csv_data_grid(grid(
            &["datetime", "status"],
            &[
                &["2024-06-01T00:00:00Z", "ok"],
                &["2024-06-01T00:00:01Z", "NA"],
                &["2024-06-01T00:00:02Z", ""],
                &["2024-06-01T00:00:03Z", "fault"],
            ],
        ))
        .unwrap();
        assert_eq!(skipped, 0);
        match series(&parsed, "status") {
            TypedSamples::String(samples) => assert_eq!(samples.len(), 4),
            other => panic!("not String: {other:?}"),
        }
    }

    #[test]
    fn a_missing_time_is_an_error_that_names_the_row() {
        let error = parse_csv_data_grid(grid(
            &["timestamp", "value_a"],
            &[&["1717243200", "1"], &["", "2"], &["1717243202", "3"]],
        ))
        .unwrap_err();
        assert!(error.to_string().contains("row 2"), "{error}");
    }
}
