use crate::{
    datamodel::{
        Sample, SensAppDateTime, Sensor, SensorType, TypedSamples, batch_builder::BatchBuilder,
        unit::Unit,
    },
    importers::IngestionStats,
    storage::StorageInstance,
};
use anyhow::{Result, anyhow};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float64Array, Int64Array,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::{FileReader, StreamReader};
use futures::io::{AsyncRead, AsyncReadExt};
use geo::Point;
use rust_decimal::Decimal;
use smallvec::SmallVec;
use std::{collections::HashMap, io::Cursor, sync::Arc};

/// Type alias for complex sensor data map
type SensorDataMap = HashMap<String, (Arc<Sensor>, Vec<(SensAppDateTime, TypedSamples)>)>;
use uuid::Uuid;

/// Publish Arrow data asynchronously to storage
pub async fn publish_arrow_async<R: AsyncRead + Unpin + Send>(
    mut arrow_reader: R,
    storage: Arc<dyn StorageInstance>,
) -> Result<IngestionStats> {
    // Read all data into a buffer first
    let mut buffer = Vec::new();
    arrow_reader.read_to_end(&mut buffer).await?;

    // Parse the Arrow data
    let record_batches = parse_arrow_file(&buffer)?;
    let mut stats = IngestionStats::default();

    // Convert Arrow data to SensApp format and publish
    let mut batch_builder = BatchBuilder::new()?;

    for record_batch in record_batches {
        let sensor_data_map = convert_record_batch_to_sensors(&record_batch)?;
        stats.series += sensor_data_map.len();

        for (_sensor_key, (sensor, sample_entries)) in sensor_data_map {
            for (_datetime, typed_samples) in sample_entries {
                stats.samples += typed_samples.len();
                batch_builder.add(sensor.clone(), typed_samples).await?;
            }
        }
    }

    batch_builder.send_what_is_left(storage).await?;
    Ok(stats)
}

/// Parse Arrow IPC stream or file format from bytes.
fn parse_arrow_file(buffer: &[u8]) -> Result<Vec<RecordBatch>> {
    let stream_batches = StreamReader::try_new(Cursor::new(buffer), None)
        .ok()
        .map(|reader| {
            reader
                .into_iter()
                .map(|batch| {
                    batch.map_err(|e| anyhow!("Failed to read Arrow batch from stream: {}", e))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;

    let batches = if let Some(batches) = stream_batches {
        batches
    } else {
        let cursor = Cursor::new(buffer);
        let reader = FileReader::try_new(cursor, None)
            .map_err(|e| anyhow!("Failed to create Arrow file reader: {}", e))?;

        let mut batches = Vec::new();
        for batch_result in reader {
            let batch =
                batch_result.map_err(|e| anyhow!("Failed to read Arrow batch from file: {}", e))?;
            batches.push(batch);
        }
        batches
    };

    if batches.is_empty() {
        return Err(anyhow!("Arrow IPC payload contains no data batches"));
    }

    Ok(batches)
}

/// Convert Arrow RecordBatch to SensApp sensors and samples
fn convert_record_batch_to_sensors(batch: &RecordBatch) -> Result<SensorDataMap> {
    let schema = batch.schema();

    if batch.num_rows() == 0 {
        return Err(anyhow!("Arrow IPC payload contains an empty batch"));
    }

    // Find required columns
    let timestamp_idx = find_column_index(&schema, "timestamp")
        .ok_or_else(|| anyhow!("Arrow data must contain 'timestamp' column"))?;
    let value_idx = find_column_index(&schema, "value")
        .ok_or_else(|| anyhow!("Arrow data must contain 'value' column"))?;

    // Optional metadata columns
    let sensor_id_idx = find_column_index(&schema, "sensor_id");
    let sensor_name_idx = find_column_index(&schema, "sensor_name");

    // Get arrays
    let timestamp_array = batch.column(timestamp_idx);
    let value_array = batch.column(value_idx);

    // Validate timestamp column
    let timestamp_data = match timestamp_array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => timestamp_array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| anyhow!("Failed to downcast timestamp array"))?,
        _ => {
            return Err(anyhow!(
                "Timestamp column must be Timestamp(Microsecond, _) type"
            ));
        }
    };

    // Convert timestamps
    let timestamps: Result<Vec<SensAppDateTime>> = (0..timestamp_data.len())
        .map(|i| {
            let ts_micros = timestamp_data.value(i);
            microseconds_to_sensapp_datetime(ts_micros)
                .ok_or_else(|| anyhow!("Invalid timestamp value: {}", ts_micros))
        })
        .collect();
    let timestamps = timestamps?;

    // Convert values based on data type
    let (derived_sensor_type, typed_samples) =
        convert_arrow_array_to_typed_samples(value_array, &timestamps)?;

    // Extract sensor metadata
    let sensor_id = extract_sensor_id(batch, sensor_id_idx)?;
    let sensor_name = extract_sensor_name(batch, sensor_name_idx);
    let sensor_type = extract_sensor_type(batch).unwrap_or(derived_sensor_type);
    let unit = extract_sensor_unit(batch);
    let labels = extract_sensor_labels(batch)?;

    let sensor_name = sensor_name.unwrap_or_else(|| sensor_id.to_string());
    let sensor = Arc::new(Sensor {
        uuid: sensor_id,
        name: sensor_name.clone(),
        sensor_type,
        unit,
        labels,
    });

    let mut result = HashMap::new();
    let sensor_key = sensor_name;

    // Create a single sample entry with all the data
    let sample_entry = vec![(timestamps[0], typed_samples)];
    result.insert(sensor_key, (sensor, sample_entry));

    Ok(result)
}

/// Convert Arrow array to TypedSamples
fn convert_arrow_array_to_typed_samples(
    array: &ArrayRef,
    timestamps: &[SensAppDateTime],
) -> Result<(SensorType, TypedSamples)> {
    match array.data_type() {
        DataType::Int64 => {
            let int_array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("Failed to downcast Int64 array"))?;

            let samples: SmallVec<[Sample<i64>; 4]> = (0..int_array.len())
                .map(|i| Sample {
                    datetime: timestamps[i],
                    value: int_array.value(i),
                })
                .collect();

            Ok((SensorType::Integer, TypedSamples::Integer(samples)))
        }

        DataType::Float64 => {
            let float_array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("Failed to downcast Float64 array"))?;

            let samples: SmallVec<[Sample<f64>; 4]> = (0..float_array.len())
                .map(|i| Sample {
                    datetime: timestamps[i],
                    value: float_array.value(i),
                })
                .collect();

            Ok((SensorType::Float, TypedSamples::Float(samples)))
        }

        DataType::Decimal128(_, scale) => {
            let decimal_array = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| anyhow!("Failed to downcast Decimal128 array"))?;

            let samples: Result<SmallVec<[Sample<Decimal>; 4]>> = (0..decimal_array.len())
                .map(|i| {
                    let raw_value = decimal_array.value(i);
                    // Convert i128 back to Decimal with proper scale
                    let decimal = Decimal::from_i128_with_scale(raw_value, *scale as u32);
                    Ok(Sample {
                        datetime: timestamps[i],
                        value: decimal,
                    })
                })
                .collect();

            Ok((SensorType::Numeric, TypedSamples::Numeric(samples?)))
        }

        DataType::Utf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("Failed to downcast String array"))?;

            let samples: SmallVec<[Sample<String>; 4]> = (0..string_array.len())
                .map(|i| Sample {
                    datetime: timestamps[i],
                    value: string_array.value(i).to_string(),
                })
                .collect();

            Ok((SensorType::String, TypedSamples::String(samples)))
        }

        DataType::Boolean => {
            let bool_array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("Failed to downcast Boolean array"))?;

            let samples: SmallVec<[Sample<bool>; 4]> = (0..bool_array.len())
                .map(|i| Sample {
                    datetime: timestamps[i],
                    value: bool_array.value(i),
                })
                .collect();

            Ok((SensorType::Boolean, TypedSamples::Boolean(samples)))
        }

        DataType::Struct(fields) => {
            // Handle location data (lat/lon struct)
            if fields.len() == 2
                && fields[0].name() == "latitude"
                && fields[1].name() == "longitude"
            {
                let struct_array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| anyhow!("Failed to downcast Struct array"))?;

                let lat_array = struct_array
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| anyhow!("Latitude must be Float64"))?;

                let lon_array = struct_array
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| anyhow!("Longitude must be Float64"))?;

                let samples: SmallVec<[Sample<Point>; 4]> = (0..struct_array.len())
                    .map(|i| {
                        let lat = lat_array.value(i);
                        let lon = lon_array.value(i);
                        Sample {
                            datetime: timestamps[i],
                            value: Point::new(lon, lat), // Note: Point::new(x, y) where x=lon, y=lat
                        }
                    })
                    .collect();

                Ok((SensorType::Location, TypedSamples::Location(samples)))
            } else {
                Err(anyhow!("Unsupported struct format for location data"))
            }
        }

        DataType::Binary => {
            let binary_array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow!("Failed to downcast Binary array"))?;

            let samples: SmallVec<[Sample<Vec<u8>>; 4]> = (0..binary_array.len())
                .map(|i| Sample {
                    datetime: timestamps[i],
                    value: binary_array.value(i).to_vec(),
                })
                .collect();

            Ok((SensorType::Blob, TypedSamples::Blob(samples)))
        }

        _ => Err(anyhow!(
            "Unsupported Arrow data type: {:?}",
            array.data_type()
        )),
    }
}

/// Helper functions
fn find_column_index(schema: &arrow::datatypes::Schema, column_name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == column_name)
}

fn extract_sensor_id(batch: &RecordBatch, sensor_id_idx: Option<usize>) -> Result<Uuid> {
    if let Some(uuid_str) = batch.schema().metadata().get("sensapp.sensor.uuid") {
        return Uuid::parse_str(uuid_str)
            .map_err(|e| anyhow!("Invalid UUID in sensapp.sensor.uuid metadata: {}", e));
    }

    if let Some(idx) = sensor_id_idx {
        let array = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("sensor_id column must be string type"))?;

        if array.len() > 0 {
            let uuid_str = array.value(0);
            return Uuid::parse_str(uuid_str)
                .map_err(|e| anyhow!("Invalid UUID in sensor_id: {}", e));
        }
    }

    // Generate a new UUID if no sensor_id provided
    Ok(Uuid::new_v4())
}

fn extract_sensor_name(batch: &RecordBatch, sensor_name_idx: Option<usize>) -> Option<String> {
    if let Some(name) = batch.schema().metadata().get("sensapp.sensor.name") {
        return Some(name.clone());
    }

    sensor_name_idx.and_then(|idx| {
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .and_then(|array| {
                if array.len() > 0 {
                    Some(array.value(0).to_string())
                } else {
                    None
                }
            })
    })
}

fn extract_sensor_type(batch: &RecordBatch) -> Option<SensorType> {
    batch
        .schema()
        .metadata()
        .get("sensapp.sensor.type")
        .and_then(|value| value.parse().ok())
}

fn extract_sensor_unit(batch: &RecordBatch) -> Option<Unit> {
    batch
        .schema()
        .metadata()
        .get("sensapp.sensor.unit")
        .map(|value| Unit::new(value.clone(), None))
}

fn extract_sensor_labels(batch: &RecordBatch) -> Result<SmallVec<[(String, String); 8]>> {
    let schema = batch.schema();

    let Some(labels_json) = schema.metadata().get("sensapp.sensor.labels") else {
        return Ok(SmallVec::new());
    };

    let labels_map =
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(labels_json)
            .map_err(|e| anyhow!("Invalid sensapp.sensor.labels metadata: {}", e))?;

    let mut labels: Vec<(String, String)> = labels_map
        .into_iter()
        .map(|(key, value)| {
            let value = value.as_str().ok_or_else(|| {
                anyhow!(
                    "Invalid sensapp.sensor.labels metadata value for key '{}'",
                    key
                )
            })?;
            Ok((key, value.to_string()))
        })
        .collect::<Result<_>>()?;

    labels.sort_by(|(left_key, left_value), (right_key, right_value)| {
        left_key
            .cmp(right_key)
            .then_with(|| left_value.cmp(right_value))
    });

    Ok(SmallVec::from_vec(labels))
}

/// Helper function to convert microseconds since epoch to SensAppDateTime
fn microseconds_to_sensapp_datetime(micros: i64) -> Option<SensAppDateTime> {
    // Convert microseconds since Unix epoch to SensAppDateTime
    // Use SensAppDateTime::from_unix_seconds which accepts seconds as f64
    let seconds = micros as f64 / 1_000_000.0;
    Some(SensAppDateTime::from_unix_seconds(seconds))
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use crate::datamodel::unit::Unit;
    use crate::datamodel::*;
    use crate::exporters::arrow::ArrowConverter;
    use smallvec::smallvec;
    use uuid::Uuid;

    pub fn create_test_arrow_stream_data() -> Vec<u8> {
        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_sensor".to_string(),
            sensor_type: SensorType::Integer,
            unit: Some(Unit::new("Celsius".to_string(), None)),
            labels: smallvec![("room".to_string(), "lab".to_string())],
        };

        let datetime1 = SensAppDateTime::now().unwrap();
        let datetime2 = datetime1 + hifitime::Duration::from_seconds(1.0);

        let samples = TypedSamples::Integer(smallvec![
            Sample {
                datetime: datetime1,
                value: 42
            },
            Sample {
                datetime: datetime2,
                value: 84
            },
        ]);

        let sensor_data = SensorData::new(sensor, samples);
        ArrowConverter::to_arrow_stream(&sensor_data).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::test_utils::*;
    use super::*;

    #[tokio::test]
    async fn test_parse_arrow_file() {
        let arrow_data = create_test_arrow_stream_data();
        let batches = parse_arrow_file(&arrow_data).unwrap();

        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn test_convert_record_batch_to_sensors() {
        let arrow_data = create_test_arrow_stream_data();
        let batches = parse_arrow_file(&arrow_data).unwrap();
        let sensor_map = convert_record_batch_to_sensors(&batches[0]).unwrap();

        assert_eq!(sensor_map.len(), 1);

        let (sensor, sample_entries) = sensor_map.values().next().unwrap();
        assert_eq!(sensor.name, "test_sensor");
        assert_eq!(sensor.sensor_type, SensorType::Integer);
        assert_eq!(
            sensor.unit.as_ref().map(|unit| unit.name.as_str()),
            Some("Celsius")
        );
        assert_eq!(sensor.labels.len(), 1);
        assert_eq!(sensor.labels[0], ("room".to_string(), "lab".to_string()));
        assert_eq!(sample_entries.len(), 1);

        match &sample_entries[0].1 {
            TypedSamples::Integer(samples) => {
                assert_eq!(samples.len(), 2);
                assert_eq!(samples[0].value, 42);
                assert_eq!(samples[1].value, 84);
            }
            other => panic!("expected integer samples, got {:?}", other),
        }
    }

    #[test]
    fn test_datetime_conversion() {
        let now_micros = 1640995200000000i64; // 2022-01-01T00:00:00Z in microseconds
        let datetime = microseconds_to_sensapp_datetime(now_micros);
        assert!(datetime.is_some());
    }

    #[test]
    fn test_find_column_index() {
        // This would require creating a test schema
        // Implementation depends on actual Arrow schema setup
    }
}
