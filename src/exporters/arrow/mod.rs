use crate::datamodel::{SensAppDateTime, SensorData, TypedSamples};
use anyhow::Result;
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Decimal128Builder, Float64Builder, Int64Builder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_ipc::writer::FileWriter;
use std::sync::Arc;

/// Converter for SensorData to Apache Arrow format
pub struct ArrowConverter;

impl ArrowConverter {
    /// Convert SensorData to Arrow RecordBatch
    pub fn to_record_batch(sensor_data: &SensorData) -> Result<RecordBatch> {
        let (schema, columns) = Self::convert_sensor_data_to_arrow(sensor_data)?;

        RecordBatch::try_new(schema, columns)
            .map_err(|e| anyhow::anyhow!("Failed to create Arrow RecordBatch: {}", e))
    }

    /// Convert SensorData to Arrow IPC file format (bytes)
    pub fn to_arrow_file(sensor_data: &SensorData) -> Result<Vec<u8>> {
        let batch = Self::to_record_batch(sensor_data)?;
        Self::record_batch_to_arrow_file(&batch)
    }

    /// Internal method to convert SensorData to Arrow schema and columns
    fn convert_sensor_data_to_arrow(
        sensor_data: &SensorData,
    ) -> Result<(Arc<Schema>, Vec<ArrayRef>)> {
        let sample_count = sensor_data.samples.len();

        // Create timestamp column (microsecond precision to match SensApp)
        let timestamp_field = Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        );

        let mut timestamp_builder = TimestampMicrosecondBuilder::new();
        let value_column: ArrayRef;
        let value_field: Field;

        match &sensor_data.samples {
            TypedSamples::Integer(samples) => {
                value_field = Field::new("value", DataType::Int64, false);
                let mut builder = Int64Builder::new();

                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    builder.append_value(sample.value);
                }

                value_column = Arc::new(builder.finish());
            }

            TypedSamples::Numeric(samples) => {
                // Use Decimal128 for high precision numeric data
                value_field = Field::new("value", DataType::Decimal128(38, 18), false);
                let mut builder = Decimal128Builder::new().with_precision_and_scale(38, 18)?;

                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    // Convert Decimal to i128 representation for Arrow
                    let decimal_i128 = sample.value.mantissa()
                        * 10_i128.pow(18_u32.saturating_sub(sample.value.scale()));
                    builder.append_value(decimal_i128);
                }

                value_column = Arc::new(builder.finish());
            }

            TypedSamples::Float(samples) => {
                value_field = Field::new("value", DataType::Float64, false);
                let mut builder = Float64Builder::new();

                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    builder.append_value(sample.value);
                }

                value_column = Arc::new(builder.finish());
            }

            TypedSamples::String(samples) => {
                value_field = Field::new("value", DataType::Utf8, false);
                let mut builder = StringBuilder::new();

                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    builder.append_value(&sample.value);
                }

                value_column = Arc::new(builder.finish());
            }

            TypedSamples::Boolean(samples) => {
                value_field = Field::new("value", DataType::Boolean, false);
                let mut builder = BooleanBuilder::new();

                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    builder.append_value(sample.value);
                }

                value_column = Arc::new(builder.finish());
            }

            TypedSamples::Location(samples) => {
                // Represent location as a struct with lat/lon fields
                let lat_field = Arc::new(Field::new("latitude", DataType::Float64, false));
                let lon_field = Arc::new(Field::new("longitude", DataType::Float64, false));
                let fields = vec![lat_field, lon_field];

                value_field = Field::new("value", DataType::Struct(fields.clone().into()), false);

                let mut lat_builder = Float64Builder::new();
                let mut lon_builder = Float64Builder::new();

                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    lat_builder.append_value(sample.value.y());
                    lon_builder.append_value(sample.value.x());
                }

                let lat_array = Arc::new(lat_builder.finish()) as ArrayRef;
                let lon_array = Arc::new(lon_builder.finish()) as ArrayRef;
                let field_arrays = vec![lat_array, lon_array];

                value_column = Arc::new(
                    arrow::array::StructArray::try_new(fields.into(), field_arrays, None).map_err(
                        |e| anyhow::anyhow!("Failed to create struct array for location: {}", e),
                    )?,
                );
            }

            TypedSamples::Blob(samples) => {
                value_field = Field::new("value", DataType::Binary, false);
                let mut builder = BinaryBuilder::new();

                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    builder.append_value(&sample.value);
                }

                value_column = Arc::new(builder.finish());
            }

            TypedSamples::Json(samples) => {
                // Store JSON as string (serialized JSON)
                value_field = Field::new("value", DataType::Utf8, false);
                let mut builder = StringBuilder::new();

                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    let json_str = serde_json::to_string(&sample.value)?;
                    builder.append_value(json_str);
                }

                value_column = Arc::new(builder.finish());
            }
        }

        let timestamp_column = Arc::new(timestamp_builder.finish()) as ArrayRef;

        // Add sensor metadata as fields
        let mut fields = vec![timestamp_field, value_field];
        let mut columns = vec![timestamp_column, value_column];

        // Add sensor UUID as metadata field
        let sensor_id_field = Field::new("sensor_id", DataType::Utf8, false);
        let mut sensor_id_builder = StringBuilder::new();
        for _ in 0..sample_count {
            sensor_id_builder.append_value(sensor_data.sensor.uuid.to_string());
        }
        fields.push(sensor_id_field);
        columns.push(Arc::new(sensor_id_builder.finish()) as ArrayRef);

        // Add sensor name
        let name = &sensor_data.sensor.name;
        let sensor_name_field = Field::new("sensor_name", DataType::Utf8, false);
        let mut sensor_name_builder = StringBuilder::new();
        for _ in 0..sample_count {
            sensor_name_builder.append_value(name);
        }
        fields.push(sensor_name_field);
        columns.push(Arc::new(sensor_name_builder.finish()) as ArrayRef);

        let schema = Arc::new(Schema::new(fields));
        Ok((schema, columns))
    }

    /// Convert a single RecordBatch to Arrow file format
    fn record_batch_to_arrow_file(batch: &RecordBatch) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut buffer, &batch.schema())?;
            writer.write(batch)?;
            writer.finish()?;
        }
        Ok(buffer)
    }

}

// Implement conversion trait for SensAppDateTime to microseconds
trait ToMicroseconds {
    fn to_microseconds_since_epoch(&self) -> i64;
}

impl ToMicroseconds for SensAppDateTime {
    fn to_microseconds_since_epoch(&self) -> i64 {
        // Convert to microseconds since Unix epoch
        // SensApp uses hifitime internally which has microsecond precision
        (self.to_duration_since_j1900().total_nanoseconds() / 1000 -
        // J1900 to Unix epoch offset in microseconds
        2_208_988_800_000_000) as i64
    }
}

#[cfg(test)]
pub mod test_data_helpers {
    use super::*;
    use crate::datamodel::*;
    use geo::Point;
    use smallvec::{SmallVec, smallvec};
    use uuid::Uuid;

    pub fn create_test_sensor_data_integer() -> SensorData {
        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_sensor".to_string(),
            sensor_type: SensorType::Integer,
            unit: None,
            labels: SmallVec::new(),
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

        SensorData::new(sensor, samples)
    }

    pub fn create_test_sensor_data_location() -> SensorData {
        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "gps_sensor".to_string(),
            sensor_type: SensorType::Location,
            unit: None,
            labels: SmallVec::new(),
        };

        let datetime1 = SensAppDateTime::now().unwrap();
        let location = Point::new(2.3522, 48.8566); // Paris coordinates

        let samples = TypedSamples::Location(smallvec![Sample {
            datetime: datetime1,
            value: location
        },]);

        SensorData::new(sensor, samples)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arrow_conversion_integer() {
        let sensor_data = test_data_helpers::create_test_sensor_data_integer();

        let batch = ArrowConverter::to_record_batch(&sensor_data).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4); // timestamp, value, sensor_id, sensor_name

        // Verify schema
        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "value");
        assert_eq!(schema.field(2).name(), "sensor_id");
        assert_eq!(schema.field(3).name(), "sensor_name");

        // Check data types
        assert!(matches!(
            schema.field(0).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ));
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_arrow_file_format() {
        let sensor_data = test_data_helpers::create_test_sensor_data_integer();

        let arrow_bytes = ArrowConverter::to_arrow_file(&sensor_data).unwrap();
        assert!(!arrow_bytes.is_empty());

        // Arrow file should start with the Arrow magic number
        assert_eq!(&arrow_bytes[0..6], b"ARROW1");
    }

    #[test]
    fn test_location_conversion() {
        let sensor_data = test_data_helpers::create_test_sensor_data_location();

        let batch = ArrowConverter::to_record_batch(&sensor_data).unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Check that location is converted to struct
        let schema = batch.schema();
        assert!(matches!(schema.field(1).data_type(), DataType::Struct(_)));
    }

}
