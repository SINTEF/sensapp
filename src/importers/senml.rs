use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::sensapp_vec::SensAppLabels;
use crate::datamodel::unit::Unit;
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples};
use anyhow::{Result, anyhow, bail};
use rust_decimal::Decimal;
use serde_json::Value;
use sindit_senml::{SenMLResolvedRecord, SenMLValueField, parse_json};
use std::collections::HashMap;
use std::str::FromStr;
use uuid::Uuid;

/// Parser for SenML JSON format (RFC 8428)
/// Uses the sindit-senml crate for RFC-compliant parsing
pub struct SenMLImporter;

/// The records of one series, with the SensApp extension fields its records carry.
struct Series<'a> {
    name: String,
    unit: Option<String>,
    /// `_name`: the name of a series whose base name is its UUID
    display_name: Option<String>,
    /// `_type`: the type of the series, when the value field does not tell it (Integer, Numeric,
    /// JSON)
    declared_type: Option<SensorType>,
    /// `_labels`
    labels: Option<SensAppLabels>,
    /// The records that hold a value, with their raw JSON
    records: Vec<(&'a SenMLResolvedRecord, &'a Value)>,
}

/// The value fields of a SenML record
const VALUE_FIELDS: [&str; 4] = ["v", "vs", "vb", "vd"];

/// Integers are exact in an `f64` up to 2^53
const EXACT_F64_INTEGER: f64 = 9_007_199_254_740_992.0;

impl SenMLImporter {
    /// Parse SenML JSON and create sensor data grouped by sensor name, in the order the series
    /// first appear.
    pub fn from_senml_json(json_str: &str) -> Result<Vec<(String, SensorData)>> {
        // The raw records, read before the parser for what it does not give or check
        let raw_records = match serde_json::from_str(json_str) {
            Ok(Value::Array(records)) => records,
            _ => Vec::new(), // the parser reports what is wrong
        };
        Self::check_times(&raw_records)?;
        let records = parse_json(json_str, None)?;
        // The parser resolves each record in order, one for one
        if records.len() != raw_records.len() {
            bail!("The SenML records could not be read one by one");
        }
        // A base value (`bv`) is added to the values: the raw `v` is not the value then
        let raw_values_are_values = !raw_records.iter().any(|raw| raw.get("bv").is_some());

        let mut all_series: Vec<Series> = Vec::new();
        let mut positions: HashMap<String, usize> = HashMap::new();
        for (index, (record, raw)) in records.iter().zip(&raw_records).enumerate() {
            let position = *positions.entry(record.name.clone()).or_insert_with(|| {
                all_series.push(Series {
                    name: record.name.clone(),
                    unit: record.unit.clone(),
                    display_name: None,
                    declared_type: None,
                    labels: None,
                    records: Vec::new(),
                });
                all_series.len() - 1
            });
            let series = &mut all_series[position];
            Self::read_extensions(series, raw)?;

            if VALUE_FIELDS.iter().any(|field| raw.get(field).is_some()) {
                series.records.push((record, raw));
            } else if record.sum.is_some() {
                bail!(
                    "The SenML record {index} ('{}') has a sum and no value: sums are not supported",
                    record.name
                );
            }
            // Otherwise the record only carries base fields: it is no sample (the parser gives
            // it the value 0)
        }

        all_series
            .into_iter()
            .filter(|series| !series.records.is_empty())
            .map(|series| Self::series_data(series, raw_values_are_values))
            .collect()
    }

    /// Read the SensApp extension fields of a record into its series.
    fn read_extensions(series: &mut Series, raw: &Value) -> Result<()> {
        if let Some(name) = raw.get("_name").and_then(Value::as_str) {
            series.display_name = Some(name.to_string());
        }
        if let Some(declared) = raw.get("_type") {
            let sensor_type = declared
                .as_str()
                .and_then(|name| SensorType::from_str(name).ok())
                .ok_or_else(|| anyhow!("Unknown SenML _type {declared} for '{}'", series.name))?;
            if sensor_type == SensorType::Location {
                bail!(
                    "'{}' is a Location series, which SenML cannot import",
                    series.name
                );
            }
            series.declared_type = Some(sensor_type);
        }
        if let Some(labels) = raw.get("_labels") {
            let labels = labels
                .as_object()
                .ok_or_else(|| anyhow!("The SenML _labels of '{}' is not an object", series.name))?
                .iter()
                .map(|(key, value)| {
                    let value = value.as_str().ok_or_else(|| {
                        anyhow!("The SenML label {key} of '{}' is not text", series.name)
                    })?;
                    Ok((key.clone(), value.to_string()))
                })
                .collect::<Result<SensAppLabels>>()?;
            series.labels = Some(labels);
        }
        Ok(())
    }

    /// The sensor and the samples of a series. Every record holds a value of the type of the
    /// series: the `_type` field, or else the value field of its first record.
    fn series_data(series: Series, raw_values_are_values: bool) -> Result<(String, SensorData)> {
        let (first, _) = series.records[0];
        let sensor_type = match series.declared_type {
            Some(sensor_type) => sensor_type,
            None => match &first.value {
                Some(SenMLValueField::StringValue(_)) => SensorType::String,
                Some(SenMLValueField::BooleanValue(_)) => SensorType::Boolean,
                Some(SenMLValueField::DataValue(_)) => SensorType::Blob,
                _ => SensorType::Float,
            },
        };
        let records = &series.records;
        let mismatch = |record: &SenMLResolvedRecord, what: &str| {
            anyhow!(
                "The SenML record of '{}' at {} has no {what} value: the series holds {} values",
                record.name,
                record.time.to_rfc3339(),
                sensor_type
            )
        };
        let float = |record: &SenMLResolvedRecord| match record.value {
            Some(SenMLValueField::FloatingPoint(value)) => Ok(value),
            _ => Err(mismatch(record, "number")),
        };
        let text = |record: &SenMLResolvedRecord| match &record.value {
            Some(SenMLValueField::StringValue(value)) => Ok(value.clone()),
            _ => Err(mismatch(record, "string")),
        };
        // The number as written, when no base value was added to it
        let raw_number = |raw: &Value| {
            raw_values_are_values
                .then(|| raw.get("v").and_then(Value::as_number).cloned())
                .flatten()
        };
        let time = Self::record_time_to_sensapp_datetime;

        macro_rules! samples {
            ($variant:ident, $value:expr) => {
                TypedSamples::$variant(
                    records
                        .iter()
                        .map(|&(record, raw)| {
                            Ok(Sample {
                                datetime: time(record),
                                value: ($value)(record, raw)?,
                            })
                        })
                        .collect::<Result<_>>()?,
                )
            };
        }

        let samples = match sensor_type {
            SensorType::Float => samples!(Float, |record, _| float(record)),
            SensorType::Integer => samples!(Integer, |record, raw| {
                let value = float(record)?;
                raw_number(raw)
                    .and_then(|number| number.as_i64())
                    .or_else(|| {
                        (value.fract() == 0.0 && value.abs() <= EXACT_F64_INTEGER)
                            .then_some(value as i64)
                    })
                    .ok_or_else(|| mismatch(record, "integer"))
            }),
            SensorType::Numeric => samples!(Numeric, |record, raw| {
                let value = float(record)?;
                let written = raw_number(raw).map_or_else(|| value.to_string(), |n| n.to_string());
                Decimal::from_str(&written)
                    .or_else(|_| Decimal::from_scientific(&written))
                    .map_err(|_| mismatch(record, "numeric"))
            }),
            SensorType::String => samples!(String, |record, _| text(record)),
            SensorType::Json => samples!(Json, |record, _| {
                serde_json::from_str::<Value>(&text(record)?).map_err(|_| mismatch(record, "JSON"))
            }),
            SensorType::Boolean => samples!(Boolean, |record: &SenMLResolvedRecord, _| {
                match record.value {
                    Some(SenMLValueField::BooleanValue(value)) => Ok(value),
                    _ => Err(mismatch(record, "boolean")),
                }
            }),
            SensorType::Blob => samples!(Blob, |record: &SenMLResolvedRecord, _| {
                match &record.value {
                    Some(SenMLValueField::DataValue(value)) => Ok(value.clone()),
                    _ => Err(mismatch(record, "data")),
                }
            }),
            SensorType::Location => unreachable!("refused when the extensions are read"),
        };

        // The exporter writes an empty `bu` for a series without a unit
        let unit = series
            .unit
            .filter(|unit| !unit.is_empty())
            .map(|unit| Unit::new(unit, None));

        // A UUID as the name is the identity written by the SenML exporter, so importing an
        // export goes back to the original series. Any other name derives a reproducible
        // UUID, so publishing the same sensor again appends to the same series.
        let sensor = match canonical_uuid(&series.name) {
            Some(uuid) => {
                let sensor = Sensor::new(
                    uuid,
                    series.display_name.unwrap_or_else(|| series.name.clone()),
                    sensor_type,
                    unit,
                    series.labels,
                );
                // `Sensor::new` checks nothing: a NUL in the name, the unit or a label would reach
                // the database as an error of the database
                sensor.validate()?;
                sensor
            }
            None => {
                Sensor::new_without_uuid(series.name.clone(), sensor_type, unit, series.labels)?
            }
        };
        Ok((series.name, SensorData::new(sensor, samples)))
    }

    /// Refuses a `t` or `bt` too far in the past. `sindit-senml` reads a time under 2^28 as
    /// relative to now and adds it to the current date without a check, which panics when the
    /// result leaves chrono's range (about 262,000 years). Times after that are absolute and
    /// already refused by the parser when out of range.
    fn check_times(records: &[serde_json::Value]) -> Result<()> {
        /// About 3,000 years: `bt + t` stays far within chrono's range
        const EARLIEST_SECONDS: f64 = -1e11;
        for record in records {
            for field in ["t", "bt"] {
                if let Some(seconds) = record.get(field).and_then(|value| value.as_f64())
                    && seconds < EARLIEST_SECONDS
                {
                    return Err(anyhow::anyhow!(
                        "The SenML time {field}={seconds} is too far in the past"
                    ));
                }
            }
        }
        Ok(())
    }

    /// The time, rounded to the microsecond. SenML times are `f64` seconds and the parser truncates
    /// their fraction to nanoseconds: `.123` arrives as `.122999999`.
    fn record_time_to_sensapp_datetime(record: &SenMLResolvedRecord) -> SensAppDateTime {
        let seconds = record.time.timestamp();
        let micros = (i64::from(record.time.timestamp_subsec_nanos()) + 500) / 1000;
        SensAppDateTime::from_unix_microseconds_i64(
            seconds.saturating_mul(1_000_000).saturating_add(micros),
        )
    }
}

/// The UUID of a name written as a hyphenated UUID, as the exporter writes it. `Uuid::parse_str`
/// also reads 32 hex digits, braces and `urn:uuid:`, which are device ids more than series UUIDs.
fn canonical_uuid(name: &str) -> Option<Uuid> {
    (name.len() == 36)
        .then(|| Uuid::try_parse(name).ok())
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
    use crate::exporters::senml::SenMLConverter;
    use smallvec::smallvec;
    use uuid::Uuid;

    fn import(json_str: &str) -> Vec<(String, SensorData)> {
        _ = crate::config::load_configuration();
        SenMLImporter::from_senml_json(json_str).unwrap()
    }

    #[test]
    fn test_same_name_gives_same_uuid() {
        let first = import(r#"[{"bn": "temp", "bt": 1609459200.0, "v": 1.0}]"#);
        let second = import(r#"[{"bn": "temp", "bt": 1609459260.0, "v": 2.0}]"#);
        assert_eq!(first[0].1.sensor.uuid, second[0].1.sensor.uuid);
        assert_eq!(first[0].1.sensor.name, "temp");
    }

    #[test]
    fn test_different_names_or_units_give_different_uuids() {
        let temp = import(r#"[{"n": "temp", "t": 1609459200.0, "v": 1.0}]"#);
        let humidity = import(r#"[{"n": "humidity", "t": 1609459200.0, "v": 1.0}]"#);
        let temp_cel = import(r#"[{"n": "temp", "u": "Cel", "t": 1609459200.0, "v": 1.0}]"#);
        assert_ne!(temp[0].1.sensor.uuid, humidity[0].1.sensor.uuid);
        assert_ne!(temp[0].1.sensor.uuid, temp_cel[0].1.sensor.uuid);
    }

    #[test]
    fn test_uuid_base_name_is_the_series_uuid() {
        let uuid = Uuid::new_v4();
        let json = format!(r#"[{{"bn": "{uuid}", "bt": 1609459200.0, "v": 1.0}}]"#);
        let imported = import(&json);
        assert_eq!(imported[0].1.sensor.uuid, uuid);
        // Without `_name`, the name is the UUID itself
        assert_eq!(imported[0].1.sensor.name, uuid.to_string());
    }

    #[test]
    fn test_exported_name_is_used_with_uuid_base_name() {
        let uuid = Uuid::new_v4();
        let json = format!(
            r#"[{{"bn": "{uuid}", "_name": "kitchen_temp", "bt": 1609459200.0, "v": 1.0}},
                {{"t": 60.0, "v": 2.0}}]"#
        );
        let imported = import(&json);
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].1.sensor.uuid, uuid);
        assert_eq!(imported[0].1.sensor.name, "kitchen_temp");
        assert_eq!(imported[0].1.samples.len(), 2);
    }

    #[test]
    fn test_blob_samples_senml_roundtrip() {
        let uuid = Uuid::new_v4();
        let sensor = Sensor::new(
            uuid,
            "blob_sensor".to_string(),
            SensorType::Blob,
            None,
            None,
        );

        let data = vec![1u8, 2, 3, 4, 255];
        let samples = TypedSamples::Blob(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
            value: data.clone(),
        }]);

        let sensor_data = SensorData::new(sensor, samples);
        let senml_json = SenMLConverter::to_senml_json(&sensor_data).unwrap();

        // Verify JSON structure
        let json_str = senml_json.to_string();
        assert!(json_str.contains("vd"));
        assert!(json_str.contains("_name")); // Sensor name is in _name field
        assert!(json_str.contains("blob_sensor"));

        // Verify roundtrip: bn is the UUID, so the import goes back to the original series
        let imported = SenMLImporter::from_senml_json(&json_str).unwrap();
        assert_eq!(imported.len(), 1);
        let (name, imported_data) = &imported[0];
        assert_eq!(name, &uuid.to_string());
        assert_eq!(imported_data.sensor.uuid, uuid);
        assert_eq!(imported_data.sensor.name, "blob_sensor");

        if let TypedSamples::Blob(samples) = &imported_data.samples {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, data);
        } else {
            panic!("Wrong sample type");
        }
    }

    /// The SenML export added the millisecond part twice (`.250` came out at `.500`) and kept only
    /// milliseconds; the import went through `f64` seconds. Export then import keeps every
    /// microsecond and the order.
    #[test]
    fn test_export_then_import_keeps_the_microseconds() {
        use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
        _ = crate::config::load_configuration();

        let micros: [i64; 6] = [
            1_700_000_000_250_000,
            1_700_000_000_900_000,
            1_700_000_001_100_000,
            1_700_000_001_999_000,
            1_700_000_002_123_456,
            1_073_741_824_001_000,
        ];
        let samples = micros
            .iter()
            .map(|&us| Sample {
                datetime: SensAppDateTime::from_unix_microseconds_i64(us),
                value: 1.5,
            })
            .collect();
        let sensor = Sensor::new(
            Uuid::new_v4(),
            "times".to_string(),
            SensorType::Float,
            None,
            None,
        );
        let exported =
            SenMLConverter::to_senml_json(&SensorData::new(sensor, TypedSamples::Float(samples)))
                .unwrap()
                .to_string();
        let imported = SenMLImporter::from_senml_json(&exported).unwrap();
        let TypedSamples::Float(samples) = &imported[0].1.samples else {
            panic!("float samples expected");
        };
        let back: Vec<i64> = samples
            .iter()
            .map(|sample| sample.datetime.to_unix_microseconds_i64())
            .collect();
        assert_eq!(back, micros, "{exported}");
    }

    #[test]
    fn test_float_import() {
        // Note: sindit-senml treats all numeric values as floats (per RFC 8428)
        let json_str = r#"[
            {"bn": "temp_sensor", "bt": 1609459200.0, "bver": 10, "v": 23, "t": 0},
            {"v": 24, "t": 60.0}
        ]"#;

        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert_eq!(imported.len(), 1);
        let (name, data) = &imported[0];
        assert_eq!(name, "temp_sensor");
        assert_eq!(data.sensor.sensor_type, SensorType::Float);

        if let TypedSamples::Float(samples) = &data.samples {
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].value, 23.0);
            assert_eq!(samples[1].value, 24.0);
        } else {
            panic!("Wrong sample type");
        }
    }

    #[test]
    fn test_string_import() {
        let json_str = r#"[
            {"bn": "status_sensor", "bt": 1609459200.0, "vs": "active", "t": 0}
        ]"#;

        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert_eq!(imported.len(), 1);
        let (name, data) = &imported[0];
        assert_eq!(name, "status_sensor");
        assert_eq!(data.sensor.sensor_type, SensorType::String);

        if let TypedSamples::String(samples) = &data.samples {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, "active");
        } else {
            panic!("Wrong sample type");
        }
    }

    #[test]
    fn test_boolean_import() {
        let json_str = r#"[
            {"bn": "door_sensor", "bt": 1609459200.0, "vb": true, "t": 0}
        ]"#;

        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert_eq!(imported.len(), 1);
        let (name, data) = &imported[0];
        assert_eq!(name, "door_sensor");
        assert_eq!(data.sensor.sensor_type, SensorType::Boolean);

        if let TypedSamples::Boolean(samples) = &data.samples {
            assert_eq!(samples.len(), 1);
            assert!(samples[0].value);
        } else {
            panic!("Wrong sample type");
        }
    }

    #[test]
    fn test_empty_array_import() {
        let json_str = "[]";
        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert!(imported.is_empty());
    }

    #[test]
    fn test_invalid_json() {
        let json_str = "not valid json";
        let result = SenMLImporter::from_senml_json(json_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_unit_extraction() {
        let json_str = r#"[
            {"n": "temperature", "u": "Cel", "v": 25.5, "t": 1609459200}
        ]"#;

        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert_eq!(imported.len(), 1);
        let (name, data) = &imported[0];
        assert_eq!(name, "temperature");
        assert!(data.sensor.unit.is_some());
        assert_eq!(data.sensor.unit.as_ref().unwrap().name, "Cel");
    }

    fn import_error(json_str: &str) -> String {
        _ = crate::config::load_configuration();
        SenMLImporter::from_senml_json(json_str)
            .map(|imported| format!("imported {} series", imported.len()))
            .unwrap_or_else(|error| error.to_string())
    }

    /// The boolean record was stored as 0.0 in the float series of its first record.
    #[test]
    fn test_records_of_another_value_type_are_refused() {
        let error = import_error(
            r#"[{"n": "door", "t": 1609459200, "v": 1}, {"n": "door", "t": 1609459260, "vb": true}]"#,
        );
        assert!(
            error.contains("'door'") && error.contains("number"),
            "{error}"
        );
    }

    /// A record with only a sum was stored as 0.0.
    #[test]
    fn test_a_sum_without_a_value_is_refused() {
        let error = import_error(r#"[{"n": "energy", "t": 1609459200, "s": 42.0}]"#);
        assert!(error.contains("sums are not supported"), "{error}");
    }

    /// A record with only base fields was stored as a sample 0.0.
    #[test]
    fn test_a_record_without_a_value_is_no_sample() {
        let imported = import(r#"[{"bn": "temp", "bt": 1609459200}, {"t": 60, "v": 21.5}]"#);
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].1.samples.len(), 1);
    }

    /// `Uuid::parse_str` also reads 32 hex digits, braces and `urn:uuid:`: a device id became the
    /// series UUID.
    #[test]
    fn test_only_a_hyphenated_uuid_is_a_uuid() {
        let device = "0123456789abcdef0123456789abcdef";
        let imported = import(&format!(
            r#"[{{"n": "{device}", "t": 1609459200, "v": 1}}]"#
        ));
        assert_eq!(imported[0].1.sensor.name, device);
        assert_ne!(imported[0].1.sensor.uuid, Uuid::parse_str(device).unwrap());
    }

    fn export_then_import(series: &[SensorData]) -> Vec<SensorData> {
        _ = crate::config::load_configuration();
        let exported = SenMLConverter::to_senml_json_multi(series)
            .unwrap()
            .to_string();
        SenMLImporter::from_senml_json(&exported)
            .unwrap_or_else(|error| panic!("{error}: {exported}"))
            .into_iter()
            .map(|(_, data)| data)
            .collect()
    }

    fn one_sample<T>(value: T) -> smallvec::SmallVec<[Sample<T>; 4]> {
        smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
            value,
        }]
    }

    /// Every type but Location comes back with its type, its values, its unit and its labels.
    /// Integers came back as floats (refused as another type by the stored series), a unit leaked
    /// to the next series (`bu` is sticky), and labels were dropped.
    #[test]
    fn test_export_then_import_keeps_types_units_and_labels() {
        let labels: crate::datamodel::sensapp_vec::SensAppLabels =
            smallvec![("room".to_string(), "lab".to_string())];
        let sensor = |name: &str, sensor_type, unit: Option<&str>| {
            Sensor::new(
                Uuid::new_v4(),
                name.to_string(),
                sensor_type,
                unit.map(|unit| crate::datamodel::unit::Unit::new(unit.to_string(), None)),
                Some(labels.clone()),
            )
        };
        let series = vec![
            SensorData::new(
                sensor("count", SensorType::Integer, Some("Cel")),
                TypedSamples::Integer(one_sample((1 << 60) + 1)),
            ),
            SensorData::new(
                sensor("price", SensorType::Numeric, None),
                TypedSamples::Numeric(one_sample(rust_decimal::Decimal::new(1234, 2))),
            ),
            SensorData::new(
                sensor("state", SensorType::Json, None),
                TypedSamples::Json(one_sample(serde_json::json!({"on": true}))),
            ),
            SensorData::new(
                sensor("temp", SensorType::Float, None),
                TypedSamples::Float(one_sample(21.5)),
            ),
            SensorData::new(
                sensor("door", SensorType::Boolean, None),
                TypedSamples::Boolean(one_sample(true)),
            ),
        ];
        let imported = export_then_import(&series);
        assert_eq!(imported.len(), series.len());
        for (exported, imported) in series.iter().zip(&imported) {
            let (a, b) = (&imported.sensor, &exported.sensor);
            assert_eq!(
                (a.uuid, &a.name, a.sensor_type, &a.labels),
                (b.uuid, &b.name, b.sensor_type, &b.labels)
            );
            assert_eq!(
                a.unit.as_ref().map(|unit| &unit.name),
                b.unit.as_ref().map(|unit| &unit.name),
                "{}",
                b.name
            );
            assert_eq!(imported.samples, exported.samples);
        }
    }

    /// An empty series was exported as a record without a value, imported as a sample 0.0 at 1970.
    #[test]
    fn test_an_empty_series_exports_nothing() {
        let empty = SensorData::new(
            Sensor::new(
                Uuid::new_v4(),
                "empty".to_string(),
                SensorType::Float,
                None,
                None,
            ),
            TypedSamples::Float(smallvec![]),
        );
        assert_eq!(
            SenMLConverter::to_senml_json(&empty).unwrap(),
            serde_json::json!([])
        );
    }

    #[test]
    fn test_multiple_sensors() {
        let json_str = r#"[
            {"n": "sensor1", "v": 10.0, "t": 1609459200},
            {"n": "sensor2", "v": 20.0, "t": 1609459200},
            {"n": "sensor1", "v": 11.0, "t": 1609459260}
        ]"#;

        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert_eq!(imported.len(), 2);

        // Find sensor1 and sensor2
        let sensor1 = imported.iter().find(|(n, _)| n == "sensor1");
        let sensor2 = imported.iter().find(|(n, _)| n == "sensor2");

        assert!(sensor1.is_some());
        assert!(sensor2.is_some());

        if let TypedSamples::Float(samples) = &sensor1.unwrap().1.samples {
            assert_eq!(samples.len(), 2);
        } else {
            panic!("Wrong sample type for sensor1");
        }

        if let TypedSamples::Float(samples) = &sensor2.unwrap().1.samples {
            assert_eq!(samples.len(), 1);
        } else {
            panic!("Wrong sample type for sensor2");
        }
    }
}
