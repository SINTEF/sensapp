use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
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
use arrow::compute::{and, cast, filter_record_batch, is_not_null};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_ipc::{Message, root_as_footer, root_as_message};
use geo::Point;
use rust_decimal::Decimal;
use smallvec::SmallVec;
use std::{collections::HashMap, io::Cursor, sync::Arc};

/// Type alias for complex sensor data map
pub type SensorDataMap = HashMap<String, (Arc<Sensor>, Vec<(SensAppDateTime, TypedSamples)>)>;
use uuid::Uuid;

/// Read an Arrow IPC payload and convert it to sensors and samples.
///
/// Every error returned here is caused by the payload itself.
pub fn parse_arrow_sensors(
    buffer: &[u8],
    max_decompressed_bytes: usize,
) -> Result<Vec<SensorDataMap>> {
    parse_arrow_file(buffer, max_decompressed_bytes)?
        .iter()
        .map(convert_record_batch_to_sensors)
        .collect()
}

/// Publish parsed Arrow sensors to storage
pub async fn publish_arrow_sensors(
    sensor_data_maps: Vec<SensorDataMap>,
    storage: Arc<dyn StorageInstance>,
) -> Result<IngestionStats> {
    let mut stats = IngestionStats::default();
    let mut batch_builder = BatchBuilder::new()?;

    for sensor_data_map in sensor_data_maps {
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

fn parse_arrow_file(buffer: &[u8], max_decompressed_bytes: usize) -> Result<Vec<RecordBatch>> {
    check_decompressed_size(buffer, max_decompressed_bytes)?;

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

/// The buffers of an Arrow IPC payload may be compressed (the Python SDK sends LZ4), each with
/// its uncompressed length in front, and `arrow-ipc` allocates that length before it
/// decompresses. The declared lengths of the whole payload are added up before anything is
/// decoded, and may not exceed `max_bytes`, the limit of gzip and snappy bodies.
fn check_decompressed_size(buffer: &[u8], max_bytes: usize) -> Result<()> {
    let mut total = 0_u64;
    let mut count = |message: Message<'_>, body: &[u8]| -> Result<()> {
        total = total.saturating_add(declared_decompressed_size(message, body)?);
        if total > max_bytes as u64 {
            return Err(anyhow!(
                "The Arrow IPC payload exceeds {max_bytes} bytes once decompressed"
            ));
        }
        Ok(())
    };

    if buffer.starts_with(ARROW_FILE_MAGIC) {
        // A file: its messages are where the footer says, as `FileReader` reads them
        for (offset, metadata_length, body_length) in file_blocks(buffer)? {
            let malformed = || anyhow!("Malformed Arrow IPC file block at byte {offset}");
            let body_start = offset.checked_add(metadata_length).ok_or_else(malformed)?;
            let body_end = body_start.checked_add(body_length).ok_or_else(malformed)?;
            let metadata = buffer.get(offset..body_start).ok_or_else(malformed)?;
            let metadata = if metadata.starts_with(&CONTINUATION_MARKER) {
                metadata.get(8..)
            } else {
                metadata.get(4..)
            }
            .ok_or_else(malformed)?;
            let message = root_as_message(metadata).map_err(|_| malformed())?;
            count(
                message,
                buffer.get(body_start..body_end).ok_or_else(malformed)?,
            )?;
        }
    } else {
        // A stream: one message after the other, as `StreamReader` reads them
        let mut position = 0;
        while let Some((message, body, next)) = stream_message_at(buffer, position)? {
            count(message, body)?;
            position = next;
        }
    }
    Ok(())
}

const ARROW_FILE_MAGIC: &[u8] = b"ARROW1";
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

fn read_i32(buffer: &[u8], position: usize) -> Option<i32> {
    let bytes = buffer.get(position..position.checked_add(4)?)?;
    Some(i32::from_le_bytes(bytes.try_into().ok()?))
}

/// The message of a stream at `position`, its body, and where the next message starts. `None`
/// at the end of the stream.
fn stream_message_at(
    buffer: &[u8],
    position: usize,
) -> Result<Option<(Message<'_>, &[u8], usize)>> {
    let malformed = || anyhow!("Malformed Arrow IPC message at byte {position}");
    // A stream may end without its end marker
    let Some(mut length) = read_i32(buffer, position) else {
        return Ok(None);
    };
    let mut start = position + 4;
    if length == -1 {
        length = read_i32(buffer, start).ok_or_else(malformed)?;
        start += 4;
    }
    if length == 0 {
        return Ok(None);
    }
    let length = usize::try_from(length).map_err(|_| malformed())?;
    let body_start = start.checked_add(length).ok_or_else(malformed)?;
    let message = root_as_message(buffer.get(start..body_start).ok_or_else(malformed)?)
        .map_err(|_| malformed())?;
    let body_length = usize::try_from(message.bodyLength()).map_err(|_| malformed())?;
    let body_end = body_start.checked_add(body_length).ok_or_else(malformed)?;
    let body = buffer.get(body_start..body_end).ok_or_else(malformed)?;
    Ok(Some((message, body, body_end)))
}

/// The offset, metadata length and body length of the dictionaries and batches of a file.
fn file_blocks(buffer: &[u8]) -> Result<Vec<(usize, usize, usize)>> {
    let malformed = || anyhow!("Malformed Arrow IPC file footer");
    let end = buffer.len().checked_sub(10).ok_or_else(malformed)?;
    let length = read_i32(buffer, end).ok_or_else(malformed)?;
    let start = end
        .checked_sub(usize::try_from(length).map_err(|_| malformed())?)
        .ok_or_else(malformed)?;
    let footer = root_as_footer(&buffer[start..end]).map_err(|_| malformed())?;
    let size = |value: i64| usize::try_from(value).map_err(|_| malformed());
    footer
        .dictionaries()
        .into_iter()
        .flatten()
        .chain(footer.recordBatches().into_iter().flatten())
        .map(|block| {
            Ok((
                size(block.offset())?,
                size(block.metaDataLength().into())?,
                size(block.bodyLength())?,
            ))
        })
        .collect()
}

/// The bytes the compressed buffers of a message declare once decompressed.
fn declared_decompressed_size(message: Message<'_>, body: &[u8]) -> Result<u64> {
    let batch = message.header_as_record_batch().or_else(|| {
        message
            .header_as_dictionary_batch()
            .and_then(|dictionary| dictionary.data())
    });
    let Some(batch) = batch.filter(|batch| batch.compression().is_some()) else {
        return Ok(0);
    };
    let mut total = 0_u64;
    for buffer in batch.buffers().into_iter().flatten() {
        // An empty buffer is not decompressed
        if buffer.length() == 0 {
            continue;
        }
        let prefix = usize::try_from(buffer.offset())
            .ok()
            .and_then(|offset| body.get(offset..offset.checked_add(8)?))
            .ok_or_else(|| anyhow!("Malformed Arrow IPC buffer"))?;
        // -1 is a buffer left uncompressed, which takes no more than its bytes
        let declared = i64::from_le_bytes(prefix.try_into()?);
        total = total.saturating_add(u64::try_from(declared).unwrap_or(0));
    }
    Ok(total)
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

    // Validate timestamp column
    if !matches!(
        batch.column(timestamp_idx).data_type(),
        DataType::Timestamp(TimeUnit::Microsecond, _)
    ) {
        return Err(anyhow!(
            "Timestamp column must be Timestamp(Microsecond, _) type"
        ));
    }
    let null_timestamps = batch.column(timestamp_idx).null_count();
    if null_timestamps > 0 {
        return Err(anyhow!(
            "Arrow data has {null_timestamps} null timestamp(s): every sample needs a time"
        ));
    }

    // The sensor columns are read before the rows without a value are dropped, so that they are
    // checked on every row
    let sensor_id = extract_sensor_id(batch, sensor_id_idx)?;
    let sensor_name = extract_sensor_name(batch, sensor_name_idx)?;

    // A null value is no sample
    let batch = &filter_record_batch(batch, &present_values(batch.column(value_idx))?)?;
    let timestamp_data = batch
        .column(timestamp_idx)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| anyhow!("Failed to downcast timestamp array"))?;
    let value_array = batch.column(value_idx);

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
    let (column_type, typed_samples) =
        convert_arrow_array_to_typed_samples(value_array, &timestamps)?;

    // The type of the series is the type of its value column. The metadata may only say the same,
    // or say JSON for a text column (JSON series are exported as text): otherwise the samples would
    // be stored in the table of their column while the series is registered with another type.
    let (sensor_type, typed_samples) = match extract_sensor_type(batch) {
        None => (column_type, typed_samples),
        Some(declared) if declared == column_type => (declared, typed_samples),
        Some(SensorType::Json) if column_type == SensorType::String => {
            (SensorType::Json, parse_json_samples(typed_samples)?)
        }
        Some(declared) => {
            return Err(anyhow!(
                "The series is declared as {declared} (sensapp.sensor.type) but its value \
                 column holds {column_type} values"
            ));
        }
    };

    // Extract sensor metadata
    let unit = extract_sensor_unit(batch);
    let labels = extract_sensor_labels(batch)?;

    // An explicit UUID is the series identity. Without one, the name is required and the UUID is
    // derived from it, so publishing the same sensor again appends to the same series.
    let sensor = match (sensor_id, sensor_name) {
        (Some(uuid), name) => {
            let name = name.unwrap_or_else(|| uuid.to_string());
            Sensor::new(uuid, name, sensor_type, unit, Some(labels))
        }
        (None, Some(name)) => Sensor::new_without_uuid(name, sensor_type, unit, Some(labels))?,
        (None, None) => {
            return Err(anyhow!(
                "Arrow data must identify its sensor: provide a sensor UUID (sensapp.sensor.uuid \
                 metadata or sensor_id column) or a sensor name (sensapp.sensor.name metadata or \
                 sensor_name column)"
            ));
        }
    };
    let sensor_name = sensor.name.clone();
    let sensor = Arc::new(sensor);

    let mut result = HashMap::new();
    // Only null values: nothing to store
    if let Some(&first) = timestamps.first() {
        result.insert(sensor_name, (sensor, vec![(first, typed_samples)]));
    }
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
                    Ok(Sample {
                        datetime: timestamps[i],
                        value: arrow_decimal(decimal_array.value(i), *scale)?,
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
/// An Arrow decimal (`raw` × 10^-`scale`) as a `Decimal`. Arrow allows a scale up to 38 and a
/// negative scale; `Decimal` holds 96 bits and a scale up to 28, so the rest is an error.
fn arrow_decimal(raw: i128, scale: i8) -> Result<Decimal> {
    let out_of_range = || anyhow!("The decimal {raw}e{} is outside the Numeric range", -scale);
    if scale >= 0 {
        return Decimal::try_from_i128_with_scale(raw, scale as u32).map_err(|_| out_of_range());
    }
    let mut value = Decimal::try_from_i128_with_scale(raw, 0).map_err(|_| out_of_range())?;
    for _ in 0..scale.unsigned_abs() {
        value = value.checked_mul(Decimal::TEN).ok_or_else(out_of_range)?;
    }
    Ok(value)
}

fn find_column_index(schema: &arrow::datatypes::Schema, column_name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == column_name)
}

fn extract_sensor_id(batch: &RecordBatch, sensor_id_idx: Option<usize>) -> Result<Option<Uuid>> {
    let column = sensor_id_idx
        .map(|idx| one_sensor(batch, idx, "sensor_id"))
        .transpose()?;
    let Some(uuid_str) = batch
        .schema()
        .metadata()
        .get("sensapp.sensor.uuid")
        .cloned()
        .or(column)
    else {
        return Ok(None);
    };
    Uuid::parse_str(&uuid_str)
        .map(Some)
        .map_err(|e| anyhow!("Invalid sensor UUID {uuid_str:?}: {e}"))
}

fn extract_sensor_name(
    batch: &RecordBatch,
    sensor_name_idx: Option<usize>,
) -> Result<Option<String>> {
    let column = sensor_name_idx
        .map(|idx| one_sensor(batch, idx, "sensor_name"))
        .transpose()?;
    Ok(batch
        .schema()
        .metadata()
        .get("sensapp.sensor.name")
        .cloned()
        .or(column))
}

/// The value of a sensor column, which must be the same on every row: a batch holds the samples
/// of one sensor. The column is text, or a dictionary of text as the multi-series export writes.
fn one_sensor(batch: &RecordBatch, idx: usize, name: &str) -> Result<String> {
    fn is_text(data_type: &DataType) -> bool {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
            DataType::Dictionary(_, values) => is_text(values),
            _ => false,
        }
    }
    let column = batch.column(idx);
    if !is_text(column.data_type()) {
        return Err(anyhow!("The {name} column must be text"));
    }
    let column = cast(column, &DataType::Utf8)?;
    let column = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("Failed to downcast the {name} column"))?;
    let first = column.iter().next().flatten();
    match first {
        Some(first) if column.iter().all(|value| value == Some(first)) => Ok(first.to_string()),
        _ => Err(anyhow!(
            "Every row of the {name} column must name the same sensor: an Arrow batch holds \
             the samples of one sensor"
        )),
    }
}

/// The rows that hold a value. A null value, or a location with a null coordinate, is no sample.
fn present_values(array: &ArrayRef) -> Result<BooleanArray> {
    let mut present = is_not_null(array)?;
    if let Some(location) = array.as_any().downcast_ref::<StructArray>() {
        for coordinate in location.columns() {
            present = and(&present, &is_not_null(coordinate)?)?;
        }
    }
    Ok(present)
}

/// The text samples of a series declared as JSON, parsed.
fn parse_json_samples(samples: TypedSamples) -> Result<TypedSamples> {
    let TypedSamples::String(samples) = samples else {
        return Err(anyhow!("JSON values must be in a text column"));
    };
    let samples = samples
        .into_iter()
        .map(|sample| {
            let value = serde_json::from_str(&sample.value)
                .map_err(|error| anyhow!("Invalid JSON value in a JSON series: {error}"))?;
            Ok(Sample {
                datetime: sample.datetime,
                value,
            })
        })
        .collect::<Result<_>>()?;
    Ok(TypedSamples::Json(samples))
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
    Some(SensAppDateTime::from_unix_microseconds_i64(micros))
}

#[cfg(test)]
pub mod test_utils {
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
        let batches = parse_arrow_file(&arrow_data, 1024 * 1024).unwrap();

        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn test_convert_record_batch_to_sensors() {
        let arrow_data = create_test_arrow_stream_data();
        let batches = parse_arrow_file(&arrow_data, 1024 * 1024).unwrap();
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

    /// A one-sample float batch carrying the given `sensapp.sensor.*` metadata and columns.
    fn batch_with_identity(
        metadata: &[(&str, &str)],
        sensor_id: Option<&str>,
        sensor_name: Option<&str>,
    ) -> RecordBatch {
        use arrow::array::TimestampMicrosecondArray;
        use arrow::datatypes::{Field, Schema};

        let mut fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(vec![1_640_995_200_000_000])),
            Arc::new(Float64Array::from(vec![1.5])),
        ];
        if let Some(id) = sensor_id {
            fields.push(Field::new("sensor_id", DataType::Utf8, false));
            columns.push(Arc::new(StringArray::from(vec![id])));
        }
        if let Some(name) = sensor_name {
            fields.push(Field::new("sensor_name", DataType::Utf8, false));
            columns.push(Arc::new(StringArray::from(vec![name])));
        }
        let metadata: HashMap<String, String> = metadata
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(fields, metadata)),
            columns,
        )
        .unwrap()
    }

    fn sensor_of(batch: &RecordBatch) -> Result<Arc<Sensor>> {
        _ = crate::config::load_configuration();
        let map = convert_record_batch_to_sensors(batch)?;
        Ok(map.into_values().next().unwrap().0)
    }

    #[test]
    fn test_named_sensor_without_uuid_has_a_stable_uuid() {
        let first = sensor_of(&batch_with_identity(&[], None, Some("temp"))).unwrap();
        let second = sensor_of(&batch_with_identity(
            &[("sensapp.sensor.name", "temp")],
            None,
            None,
        ))
        .unwrap();
        let other = sensor_of(&batch_with_identity(&[], None, Some("humidity"))).unwrap();

        assert_eq!(first.uuid, second.uuid);
        assert_ne!(first.uuid, other.uuid);
        assert_eq!(first.name, "temp");
    }

    #[test]
    fn test_unit_and_labels_are_part_of_the_derived_identity() {
        let plain = sensor_of(&batch_with_identity(&[], None, Some("temp"))).unwrap();
        let with_unit = sensor_of(&batch_with_identity(
            &[("sensapp.sensor.unit", "Cel")],
            None,
            Some("temp"),
        ))
        .unwrap();
        let with_labels = sensor_of(&batch_with_identity(
            &[("sensapp.sensor.labels", r#"{"room":"lab"}"#)],
            None,
            Some("temp"),
        ))
        .unwrap();

        assert_ne!(plain.uuid, with_unit.uuid);
        assert_ne!(plain.uuid, with_labels.uuid);
    }

    #[test]
    fn test_explicit_uuid_wins_over_the_name() {
        let uuid = Uuid::new_v4();
        let from_metadata = sensor_of(&batch_with_identity(
            &[("sensapp.sensor.uuid", &uuid.to_string())],
            None,
            Some("temp"),
        ))
        .unwrap();
        let from_column = sensor_of(&batch_with_identity(
            &[],
            Some(&uuid.to_string()),
            Some("temp"),
        ))
        .unwrap();
        let without_name = sensor_of(&batch_with_identity(
            &[("sensapp.sensor.uuid", &uuid.to_string())],
            None,
            None,
        ))
        .unwrap();

        assert_eq!(from_metadata.uuid, uuid);
        assert_eq!(from_metadata.name, "temp");
        assert_eq!(from_column.uuid, uuid);
        assert_eq!(without_name.uuid, uuid);
        assert_eq!(without_name.name, uuid.to_string());
    }

    /// A type in the metadata that is not the type of the value column is refused: the samples
    /// would go to the table of their column while the series is registered with another type.
    #[test]
    fn test_metadata_type_must_match_the_value_column() {
        let error = sensor_of(&batch_with_identity(
            &[("sensapp.sensor.type", "integer")],
            None,
            Some("temp"),
        ))
        .unwrap_err();
        assert!(error.to_string().contains("declared as Integer"), "{error}");

        let same = sensor_of(&batch_with_identity(
            &[("sensapp.sensor.type", "float")],
            None,
            Some("temp"),
        ))
        .unwrap();
        assert_eq!(same.sensor_type, SensorType::Float);
    }

    /// Every type exported as one series comes back with its type and its samples.
    #[test]
    fn test_single_series_export_imports_back_with_its_type() {
        use crate::datamodel::{Sample, SensorData};
        use crate::exporters::arrow::ArrowConverter;
        use geo::Point;
        use rust_decimal::Decimal;

        let datetime = microseconds_to_sensapp_datetime(1_640_995_200_000_000).unwrap();
        /// A type, and samples of that type at the given time
        type Case = (SensorType, fn(SensAppDateTime) -> TypedSamples);
        let all: [Case; 8] = [
            (SensorType::Integer, |datetime| {
                TypedSamples::Integer(smallvec::smallvec![Sample {
                    datetime,
                    value: 42
                }])
            }),
            (SensorType::Numeric, |datetime| {
                TypedSamples::Numeric(smallvec::smallvec![Sample {
                    datetime,
                    value: Decimal::new(1234, 2)
                }])
            }),
            (SensorType::Float, |datetime| {
                TypedSamples::Float(smallvec::smallvec![Sample {
                    datetime,
                    value: 1.5
                }])
            }),
            (SensorType::String, |datetime| {
                TypedSamples::String(smallvec::smallvec![Sample {
                    datetime,
                    value: "on".to_string()
                }])
            }),
            (SensorType::Boolean, |datetime| {
                TypedSamples::Boolean(smallvec::smallvec![Sample {
                    datetime,
                    value: true
                }])
            }),
            (SensorType::Location, |datetime| {
                TypedSamples::Location(smallvec::smallvec![Sample {
                    datetime,
                    value: Point::new(10.4, 63.4)
                }])
            }),
            (SensorType::Json, |datetime| {
                TypedSamples::Json(smallvec::smallvec![Sample {
                    datetime,
                    value: serde_json::json!({"state": "on"})
                }])
            }),
            (SensorType::Blob, |datetime| {
                TypedSamples::Blob(smallvec::smallvec![Sample {
                    datetime,
                    value: vec![1, 2, 3]
                }])
            }),
        ];
        for (sensor_type, samples) in all {
            let sensor = Sensor::new(
                Uuid::new_v4(),
                "series".to_string(),
                sensor_type,
                None,
                None,
            );
            let exported = SensorData::new(sensor, samples(datetime));
            let batch = ArrowConverter::to_record_batch(&exported).unwrap();

            let map = convert_record_batch_to_sensors(&batch)
                .unwrap_or_else(|error| panic!("{sensor_type:?}: {error}"));
            let (imported, entries) = map.into_values().next().unwrap();
            assert_eq!(imported.sensor_type, sensor_type);
            assert_eq!(entries[0].1, samples(datetime), "{sensor_type:?}");
        }
    }

    /// Arrow decimals that `Decimal` cannot hold are refused, and a negative scale is understood.
    #[test]
    fn test_arrow_decimals_outside_the_numeric_range_are_refused() {
        use rust_decimal::Decimal;

        let datetime = microseconds_to_sensapp_datetime(1_640_995_200_000_000).unwrap();
        let convert = |raw: i128, precision: u8, scale: i8| {
            let array: ArrayRef = Arc::new(
                Decimal128Array::from(vec![raw])
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            );
            convert_arrow_array_to_typed_samples(&array, &[datetime]).map(|(_, samples)| {
                match samples {
                    TypedSamples::Numeric(samples) => samples[0].value,
                    other => panic!("expected numeric samples, got {other:?}"),
                }
            })
        };

        assert_eq!(convert(1234, 10, 2).unwrap(), Decimal::new(1234, 2));
        assert_eq!(convert(12, 10, -2).unwrap(), Decimal::new(1200, 0));
        // 1.5 with 30 digits after the point: more than the 28 `Decimal` keeps
        assert!(convert(15 * 10_i128.pow(29), 38, 30).is_err());
        // More than 96 bits
        assert!(convert(1 << 100, 38, 2).is_err());
        // 1 × 10^38: more than 96 bits once multiplied
        assert!(convert(1, 10, -38).is_err());
    }

    #[test]
    fn test_sensor_without_uuid_and_name_is_rejected() {
        let error = sensor_of(&batch_with_identity(&[], None, None)).unwrap_err();
        assert!(error.to_string().contains("must identify its sensor"));
    }

    #[test]
    fn test_datetime_conversion() {
        let now_micros = 1640995200000000i64; // 2022-01-01T00:00:00Z in microseconds
        let datetime = microseconds_to_sensapp_datetime(now_micros);
        assert!(datetime.is_some());
    }

    /// The import went through `f64` seconds and put about 3 % of today's timestamps one
    /// microsecond early: an export imported again was not the same series of instants.
    #[test]
    fn test_imported_microseconds_are_exact() {
        let mut rng = fastrand::Rng::with_seed(7);
        let wrong = (0..2000)
            .map(|_| rng.i64(1_600_000_000_000_000..1_900_000_000_000_000))
            .filter(|&micros| {
                microseconds_to_sensapp_datetime(micros)
                    .unwrap()
                    .to_unix_microseconds_i64()
                    != micros
            })
            .count();
        assert_eq!(wrong, 0);
    }

    const T0: i64 = 1_640_995_200_000_000;

    /// A batch of `timestamp`, `value` and a `sensor_name` column.
    fn batch_of(timestamps: Vec<Option<i64>>, values: ArrayRef, names: ArrayRef) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "timestamp",
                Arc::new(TimestampMicrosecondArray::from(timestamps)) as ArrayRef,
            ),
            ("value", values),
            ("sensor_name", names),
        ])
        .unwrap()
    }

    fn names(names: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(names))
    }

    /// The samples of the only series of a batch.
    fn samples_of(batch: &RecordBatch) -> Result<Vec<TypedSamples>> {
        _ = crate::config::load_configuration();
        let map = convert_record_batch_to_sensors(batch)?;
        Ok(map
            .into_values()
            .flat_map(|(_, entries)| entries.into_iter().map(|(_, samples)| samples))
            .collect())
    }

    /// A null value is no sample: it was stored as 0 (or "", false), at its timestamp.
    #[test]
    fn test_null_values_are_skipped() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None, Some(3.5)]));
        let batch = batch_of(
            vec![Some(T0), Some(T0 + 1), Some(T0 + 2)],
            values,
            names(vec![Some("temp"); 3]),
        );
        let samples = samples_of(&batch).unwrap();
        let datetime = |micros| microseconds_to_sensapp_datetime(micros).unwrap();
        assert_eq!(
            samples,
            vec![TypedSamples::Float(smallvec::smallvec![
                Sample {
                    datetime: datetime(T0),
                    value: 1.5
                },
                Sample {
                    datetime: datetime(T0 + 2),
                    value: 3.5
                },
            ])]
        );

        // A location with a null latitude is no sample either
        let latitudes = Arc::new(Float64Array::from(vec![Some(63.4), None]));
        let longitudes = Arc::new(Float64Array::from(vec![Some(10.4), Some(10.5)]));
        let locations: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(arrow::datatypes::Field::new(
                    "latitude",
                    DataType::Float64,
                    true,
                )),
                latitudes as ArrayRef,
            ),
            (
                Arc::new(arrow::datatypes::Field::new(
                    "longitude",
                    DataType::Float64,
                    true,
                )),
                longitudes as ArrayRef,
            ),
        ]));
        let batch = batch_of(
            vec![Some(T0), Some(T0 + 1)],
            locations,
            names(vec![Some("position"); 2]),
        );
        let samples = samples_of(&batch).unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].len(), 1);

        // Only null values: nothing to store
        let values: ArrayRef = Arc::new(Int64Array::from(vec![None, None]));
        let batch = batch_of(
            vec![Some(T0), Some(T0 + 1)],
            values,
            names(vec![Some("count"); 2]),
        );
        assert!(samples_of(&batch).unwrap().is_empty());
    }

    /// A null timestamp became 1970-01-01.
    #[test]
    fn test_a_null_timestamp_is_refused() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5]));
        let batch = batch_of(vec![Some(T0), None], values, names(vec![Some("temp"); 2]));
        let error = samples_of(&batch).unwrap_err();
        assert!(error.to_string().contains("null timestamp"), "{error}");
    }

    /// Row 0 named the series of every row: `a, b` was stored as two samples of `a`.
    #[test]
    fn test_a_sensor_column_naming_two_sensors_is_refused() {
        let values = || Arc::new(Float64Array::from(vec![1.5, 2.5])) as ArrayRef;
        let timestamps = || vec![Some(T0), Some(T0 + 1)];

        let error = samples_of(&batch_of(
            timestamps(),
            values(),
            names(vec![Some("a"), Some("b")]),
        ))
        .unwrap_err();
        assert!(error.to_string().contains("one sensor"), "{error}");

        let error = samples_of(&batch_of(
            timestamps(),
            values(),
            names(vec![Some("a"), None]),
        ))
        .unwrap_err();
        assert!(error.to_string().contains("one sensor"), "{error}");

        let batch = RecordBatch::try_from_iter(vec![
            (
                "timestamp",
                Arc::new(TimestampMicrosecondArray::from(timestamps())) as ArrayRef,
            ),
            ("value", values()),
            (
                "sensor_id",
                Arc::new(StringArray::from(vec![
                    Uuid::new_v4().to_string(),
                    Uuid::new_v4().to_string(),
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        let error = samples_of(&batch).unwrap_err();
        assert!(error.to_string().contains("one sensor"), "{error}");
    }

    /// The multi-series export writes the sensor columns as dictionaries.
    #[test]
    fn test_a_dictionary_sensor_column_names_the_sensor() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let dictionary: DictionaryArray<Int32Type> = vec!["temp", "temp"].into_iter().collect();
        let batch = batch_of(
            vec![Some(T0), Some(T0 + 1)],
            Arc::new(Float64Array::from(vec![1.5, 2.5])),
            Arc::new(dictionary),
        );
        assert_eq!(sensor_of(&batch).unwrap().name, "temp");
    }

    /// An LZ4 Arrow stream or file of `rows` float zeros, which compress very well.
    fn compressed_zeros(rows: usize, file: bool) -> Vec<u8> {
        use arrow_ipc::CompressionType;
        use arrow_ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter};

        let batch = batch_of(
            vec![Some(T0); rows],
            Arc::new(Float64Array::from(vec![0.0; rows])),
            names(vec![Some("zeros"); rows]),
        );
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .unwrap();
        let mut buffer = Vec::new();
        if file {
            let mut writer =
                FileWriter::try_new_with_options(&mut buffer, &batch.schema(), options).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        } else {
            let mut writer =
                StreamWriter::try_new_with_options(&mut buffer, &batch.schema(), options).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        buffer
    }

    /// Compressed buffers were decompressed whatever their size: a body under the request limit
    /// could expand without bound, and a forged length aborted on allocation.
    #[test]
    fn test_compressed_bodies_are_limited_by_their_declared_size() {
        for file in [false, true] {
            // 4,000 rows: 32,000 bytes of timestamps, 32,000 of values, and the names, in a body
            // of about 17 KB
            let body = compressed_zeros(4_000, file);
            assert!(body.len() < 32 * 1024, "{} bytes", body.len());

            assert!(parse_arrow_sensors(&body, 1024 * 1024).is_ok());
            let error = parse_arrow_sensors(&body, 64 * 1024).unwrap_err();
            assert!(error.to_string().contains("decompressed"), "{error}");
        }

        // The declared lengths forged to 2^62 bytes
        let declared = (4_000 * 8_i64).to_le_bytes();
        let mut body = compressed_zeros(4_000, false);
        let mut forged = 0;
        for start in 0..body.len() - 8 {
            if body[start..start + 8] == declared {
                body[start..start + 8].copy_from_slice(&(1_i64 << 62).to_le_bytes());
                forged += 1;
            }
        }
        assert!(forged > 0);
        let error = parse_arrow_sensors(&body, 64 * 1024 * 1024).unwrap_err();
        assert!(error.to_string().contains("decompressed"), "{error}");
    }
}
