use crate::datamodel::{SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples};
use anyhow::Result;
use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, Decimal128Builder, Float64Builder,
    Int64Builder, StringBuilder, StructArray, TimestampMicrosecondBuilder,
    builder::StringDictionaryBuilder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_ipc::CompressionType;
use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use std::collections::HashMap;
use std::sync::Arc;

pub struct ArrowConverter;

impl ArrowConverter {
    pub fn to_record_batch(sensor_data: &SensorData) -> Result<RecordBatch> {
        let (schema, columns) = Self::convert_sensor_data_to_arrow(sensor_data)?;

        RecordBatch::try_new(schema, columns)
            .map_err(|e| anyhow::anyhow!("Failed to create Arrow RecordBatch: {}", e))
    }

    pub fn to_arrow_stream(sensor_data: &SensorData) -> Result<Vec<u8>> {
        let batch = Self::to_record_batch(sensor_data)?;
        Self::record_batch_to_arrow_stream(&batch)
    }

    pub fn to_arrow_stream_multi(sensor_data_list: &[SensorData]) -> Result<Vec<u8>> {
        let batch = Self::to_record_batch_multi(sensor_data_list)?;
        Self::record_batch_to_arrow_stream(&batch)
    }

    fn to_record_batch_multi(sensor_data_list: &[SensorData]) -> Result<RecordBatch> {
        let present_types = PresentTypes::from_sensor_data_list(sensor_data_list);
        let total_samples: usize = sensor_data_list.iter().map(|sd| sd.samples.len()).sum();

        let mut timestamp_builder = TimestampMicrosecondBuilder::with_capacity(total_samples);
        let mut sensor_id_builder = StringDictionaryBuilder::<Int32Type>::new();
        let mut sensor_name_builder = StringDictionaryBuilder::<Int32Type>::new();
        let mut sensor_type_builder = StringDictionaryBuilder::<Int32Type>::new();
        let mut unit_builder = StringDictionaryBuilder::<Int32Type>::new();
        let mut labels_builder = StringDictionaryBuilder::<Int32Type>::new();
        let mut value_builders = MultiValueBuilders::new(&present_types)?;

        for sensor_data in sensor_data_list {
            Self::append_sensor_data_to_multi_builders(
                sensor_data,
                &present_types,
                &mut timestamp_builder,
                &mut sensor_id_builder,
                &mut sensor_name_builder,
                &mut sensor_type_builder,
                &mut unit_builder,
                &mut labels_builder,
                &mut value_builders,
            )?;
        }

        let sensor_id_array = sensor_id_builder.finish();
        let sensor_name_array = sensor_name_builder.finish();
        let sensor_type_array = sensor_type_builder.finish();
        let unit_array = unit_builder.finish();
        let labels_array = labels_builder.finish();

        let mut fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("sensor_id", sensor_id_array.data_type().clone(), false),
            Field::new("sensor_name", sensor_name_array.data_type().clone(), false),
            Field::new("sensor_type", sensor_type_array.data_type().clone(), false),
            Field::new("unit", unit_array.data_type().clone(), true),
            Field::new("labels", labels_array.data_type().clone(), false),
        ];

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(timestamp_builder.finish()),
            Arc::new(sensor_id_array),
            Arc::new(sensor_name_array),
            Arc::new(sensor_type_array),
            Arc::new(unit_array),
            Arc::new(labels_array),
        ];

        value_builders.finish_into(&mut fields, &mut columns, &present_types);

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| anyhow::anyhow!("Failed to create Arrow RecordBatch: {}", e))
    }

    #[allow(clippy::too_many_arguments)]
    fn append_sensor_data_to_multi_builders(
        sensor_data: &SensorData,
        present_types: &PresentTypes,
        timestamp_builder: &mut TimestampMicrosecondBuilder,
        sensor_id_builder: &mut StringDictionaryBuilder<Int32Type>,
        sensor_name_builder: &mut StringDictionaryBuilder<Int32Type>,
        sensor_type_builder: &mut StringDictionaryBuilder<Int32Type>,
        unit_builder: &mut StringDictionaryBuilder<Int32Type>,
        labels_builder: &mut StringDictionaryBuilder<Int32Type>,
        value_builders: &mut MultiValueBuilders,
    ) -> Result<()> {
        let sensor_id = sensor_data.sensor.uuid.to_string();
        let sensor_name = sensor_data.sensor.name.as_str();
        let sensor_type_name = sensor_type_name(&sensor_data.sensor.sensor_type);
        let labels_json = labels_to_json_string(&sensor_data.sensor);

        match &sensor_data.samples {
            TypedSamples::Integer(samples) => {
                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    sensor_id_builder.append(&sensor_id)?;
                    sensor_name_builder.append(sensor_name)?;
                    sensor_type_builder.append(sensor_type_name)?;
                    append_unit(unit_builder, sensor_data.sensor.unit.as_ref())?;
                    labels_builder.append(&labels_json)?;
                    value_builders.append_integer(sample.value, present_types);
                }
            }
            TypedSamples::Numeric(samples) => {
                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    sensor_id_builder.append(&sensor_id)?;
                    sensor_name_builder.append(sensor_name)?;
                    sensor_type_builder.append(sensor_type_name)?;
                    append_unit(unit_builder, sensor_data.sensor.unit.as_ref())?;
                    labels_builder.append(&labels_json)?;
                    value_builders.append_numeric(
                        sample.value.mantissa(),
                        sample.value.scale(),
                        present_types,
                    );
                }
            }
            TypedSamples::Float(samples) => {
                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    sensor_id_builder.append(&sensor_id)?;
                    sensor_name_builder.append(sensor_name)?;
                    sensor_type_builder.append(sensor_type_name)?;
                    append_unit(unit_builder, sensor_data.sensor.unit.as_ref())?;
                    labels_builder.append(&labels_json)?;
                    value_builders.append_float(sample.value, present_types);
                }
            }
            TypedSamples::String(samples) => {
                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    sensor_id_builder.append(&sensor_id)?;
                    sensor_name_builder.append(sensor_name)?;
                    sensor_type_builder.append(sensor_type_name)?;
                    append_unit(unit_builder, sensor_data.sensor.unit.as_ref())?;
                    labels_builder.append(&labels_json)?;
                    value_builders.append_string(&sample.value, present_types);
                }
            }
            TypedSamples::Boolean(samples) => {
                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    sensor_id_builder.append(&sensor_id)?;
                    sensor_name_builder.append(sensor_name)?;
                    sensor_type_builder.append(sensor_type_name)?;
                    append_unit(unit_builder, sensor_data.sensor.unit.as_ref())?;
                    labels_builder.append(&labels_json)?;
                    value_builders.append_boolean(sample.value, present_types);
                }
            }
            TypedSamples::Location(samples) => {
                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    sensor_id_builder.append(&sensor_id)?;
                    sensor_name_builder.append(sensor_name)?;
                    sensor_type_builder.append(sensor_type_name)?;
                    append_unit(unit_builder, sensor_data.sensor.unit.as_ref())?;
                    labels_builder.append(&labels_json)?;
                    value_builders.append_location(
                        sample.value.y(),
                        sample.value.x(),
                        present_types,
                    );
                }
            }
            TypedSamples::Blob(samples) => {
                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    sensor_id_builder.append(&sensor_id)?;
                    sensor_name_builder.append(sensor_name)?;
                    sensor_type_builder.append(sensor_type_name)?;
                    append_unit(unit_builder, sensor_data.sensor.unit.as_ref())?;
                    labels_builder.append(&labels_json)?;
                    value_builders.append_blob(&sample.value, present_types);
                }
            }
            TypedSamples::Json(samples) => {
                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    sensor_id_builder.append(&sensor_id)?;
                    sensor_name_builder.append(sensor_name)?;
                    sensor_type_builder.append(sensor_type_name)?;
                    append_unit(unit_builder, sensor_data.sensor.unit.as_ref())?;
                    labels_builder.append(&labels_json)?;
                    value_builders.append_json(&sample.value, present_types)?;
                }
            }
        }

        Ok(())
    }

    fn convert_sensor_data_to_arrow(
        sensor_data: &SensorData,
    ) -> Result<(Arc<Schema>, Vec<ArrayRef>)> {
        let mut timestamp_builder = TimestampMicrosecondBuilder::new();
        let value_field: Field;
        let value_column: ArrayRef;

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
                value_field = Field::new("value", DataType::Decimal128(38, 18), false);
                let mut builder = Decimal128Builder::new().with_precision_and_scale(38, 18)?;
                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
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
                let latitude_field = Arc::new(Field::new("latitude", DataType::Float64, false));
                let longitude_field = Arc::new(Field::new("longitude", DataType::Float64, false));
                let struct_fields = vec![latitude_field, longitude_field];

                value_field = Field::new(
                    "value",
                    DataType::Struct(struct_fields.clone().into()),
                    false,
                );

                let mut latitude_builder = Float64Builder::new();
                let mut longitude_builder = Float64Builder::new();

                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    latitude_builder.append_value(sample.value.y());
                    longitude_builder.append_value(sample.value.x());
                }

                value_column = Arc::new(
                    StructArray::try_new(
                        struct_fields.into(),
                        vec![
                            Arc::new(latitude_builder.finish()) as ArrayRef,
                            Arc::new(longitude_builder.finish()) as ArrayRef,
                        ],
                        None,
                    )
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to create location struct array: {}", e)
                    })?,
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
                value_field = Field::new("value", DataType::Utf8, false);
                let mut builder = StringBuilder::new();
                for sample in samples.iter() {
                    timestamp_builder.append_value(sample.datetime.to_microseconds_since_epoch());
                    builder.append_value(serde_json::to_string(&sample.value)?);
                }
                value_column = Arc::new(builder.finish());
            }
        }

        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                value_field,
            ],
            single_series_metadata(&sensor_data.sensor),
        ));

        Ok((
            schema,
            vec![
                Arc::new(timestamp_builder.finish()) as ArrayRef,
                value_column,
            ],
        ))
    }

    fn record_batch_to_arrow_stream(batch: &RecordBatch) -> Result<Vec<u8>> {
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .map_err(|e| anyhow::anyhow!("Failed to configure Arrow IPC compression: {}", e))?;

        let mut buffer = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new_with_options(&mut buffer, &batch.schema(), options)
                    .map_err(|e| anyhow::anyhow!("Failed to create Arrow stream writer: {}", e))?;
            writer
                .write(batch)
                .map_err(|e| anyhow::anyhow!("Failed to write Arrow stream batch: {}", e))?;
            writer
                .finish()
                .map_err(|e| anyhow::anyhow!("Failed to finish Arrow stream writer: {}", e))?;
        }
        Ok(buffer)
    }
}

#[derive(Default)]
struct PresentTypes {
    integer: bool,
    numeric: bool,
    float: bool,
    string: bool,
    boolean: bool,
    location: bool,
    blob: bool,
    json: bool,
}

impl PresentTypes {
    fn from_sensor_data_list(sensor_data_list: &[SensorData]) -> Self {
        let mut present = Self::default();

        for sensor_data in sensor_data_list {
            match &sensor_data.samples {
                TypedSamples::Integer(_) => present.integer = true,
                TypedSamples::Numeric(_) => present.numeric = true,
                TypedSamples::Float(_) => present.float = true,
                TypedSamples::String(_) => present.string = true,
                TypedSamples::Boolean(_) => present.boolean = true,
                TypedSamples::Location(_) => present.location = true,
                TypedSamples::Blob(_) => present.blob = true,
                TypedSamples::Json(_) => present.json = true,
            }
        }

        present
    }
}

struct MultiValueBuilders {
    integer: Option<Int64Builder>,
    numeric: Option<Decimal128Builder>,
    float: Option<Float64Builder>,
    string: Option<StringBuilder>,
    boolean: Option<BooleanBuilder>,
    latitude: Option<Float64Builder>,
    longitude: Option<Float64Builder>,
    blob: Option<BinaryBuilder>,
    json: Option<StringBuilder>,
}

impl MultiValueBuilders {
    fn new(present: &PresentTypes) -> Result<Self> {
        Ok(Self {
            integer: present.integer.then(Int64Builder::new),
            numeric: if present.numeric {
                Some(Decimal128Builder::new().with_precision_and_scale(38, 18)?)
            } else {
                None
            },
            float: present.float.then(Float64Builder::new),
            string: present.string.then(StringBuilder::new),
            boolean: present.boolean.then(BooleanBuilder::new),
            latitude: present.location.then(Float64Builder::new),
            longitude: present.location.then(Float64Builder::new),
            blob: present.blob.then(BinaryBuilder::new),
            json: present.json.then(StringBuilder::new),
        })
    }

    fn append_integer(&mut self, value: i64, present: &PresentTypes) {
        append_int64(&mut self.integer, Some(value));
        self.append_other_nulls(present, SensorType::Integer);
    }

    fn append_numeric(&mut self, mantissa: i128, scale: u32, present: &PresentTypes) {
        let decimal_i128 = mantissa * 10_i128.pow(18_u32.saturating_sub(scale));
        append_decimal128(&mut self.numeric, Some(decimal_i128));
        self.append_other_nulls(present, SensorType::Numeric);
    }

    fn append_float(&mut self, value: f64, present: &PresentTypes) {
        append_float64(&mut self.float, Some(value));
        self.append_other_nulls(present, SensorType::Float);
    }

    fn append_string(&mut self, value: &str, present: &PresentTypes) {
        append_string(&mut self.string, Some(value));
        self.append_other_nulls(present, SensorType::String);
    }

    fn append_boolean(&mut self, value: bool, present: &PresentTypes) {
        append_boolean(&mut self.boolean, Some(value));
        self.append_other_nulls(present, SensorType::Boolean);
    }

    fn append_location(&mut self, latitude: f64, longitude: f64, present: &PresentTypes) {
        append_float64(&mut self.latitude, Some(latitude));
        append_float64(&mut self.longitude, Some(longitude));
        self.append_other_nulls(present, SensorType::Location);
    }

    fn append_blob(&mut self, value: &[u8], present: &PresentTypes) {
        append_binary(&mut self.blob, Some(value));
        self.append_other_nulls(present, SensorType::Blob);
    }

    fn append_json(&mut self, value: &serde_json::Value, present: &PresentTypes) -> Result<()> {
        append_string(&mut self.json, Some(&serde_json::to_string(value)?));
        self.append_other_nulls(present, SensorType::Json);
        Ok(())
    }

    fn append_other_nulls(&mut self, present: &PresentTypes, current_type: SensorType) {
        if present.integer && current_type != SensorType::Integer {
            append_int64(&mut self.integer, None);
        }
        if present.numeric && current_type != SensorType::Numeric {
            append_decimal128(&mut self.numeric, None);
        }
        if present.float && current_type != SensorType::Float {
            append_float64(&mut self.float, None);
        }
        if present.string && current_type != SensorType::String {
            append_string(&mut self.string, None);
        }
        if present.boolean && current_type != SensorType::Boolean {
            append_boolean(&mut self.boolean, None);
        }
        if present.location && current_type != SensorType::Location {
            append_float64(&mut self.latitude, None);
            append_float64(&mut self.longitude, None);
        }
        if present.blob && current_type != SensorType::Blob {
            append_binary(&mut self.blob, None);
        }
        if present.json && current_type != SensorType::Json {
            append_string(&mut self.json, None);
        }
    }

    fn finish_into(
        self,
        fields: &mut Vec<Field>,
        columns: &mut Vec<ArrayRef>,
        present: &PresentTypes,
    ) {
        if present.integer {
            fields.push(Field::new("integer_value", DataType::Int64, true));
            columns.push(Arc::new(
                self.integer.expect("integer builder missing").finish(),
            ));
        }
        if present.numeric {
            fields.push(Field::new(
                "numeric_value",
                DataType::Decimal128(38, 18),
                true,
            ));
            columns.push(Arc::new(
                self.numeric.expect("numeric builder missing").finish(),
            ));
        }
        if present.float {
            fields.push(Field::new("float_value", DataType::Float64, true));
            columns.push(Arc::new(
                self.float.expect("float builder missing").finish(),
            ));
        }
        if present.string {
            fields.push(Field::new("string_value", DataType::Utf8, true));
            columns.push(Arc::new(
                self.string.expect("string builder missing").finish(),
            ));
        }
        if present.boolean {
            fields.push(Field::new("boolean_value", DataType::Boolean, true));
            columns.push(Arc::new(
                self.boolean.expect("boolean builder missing").finish(),
            ));
        }
        if present.location {
            fields.push(Field::new("latitude", DataType::Float64, true));
            columns.push(Arc::new(
                self.latitude.expect("latitude builder missing").finish(),
            ));
            fields.push(Field::new("longitude", DataType::Float64, true));
            columns.push(Arc::new(
                self.longitude.expect("longitude builder missing").finish(),
            ));
        }
        if present.blob {
            fields.push(Field::new("blob_value", DataType::Binary, true));
            columns.push(Arc::new(self.blob.expect("blob builder missing").finish()));
        }
        if present.json {
            fields.push(Field::new("json_value", DataType::Utf8, true));
            columns.push(Arc::new(self.json.expect("json builder missing").finish()));
        }
    }
}

fn append_unit(
    builder: &mut StringDictionaryBuilder<Int32Type>,
    unit: Option<&crate::datamodel::unit::Unit>,
) -> Result<()> {
    if let Some(unit) = unit {
        builder.append(unit.name.as_str())?;
    } else {
        builder.append_null();
    }
    Ok(())
}

fn append_int64(builder: &mut Option<Int64Builder>, value: Option<i64>) {
    if let Some(builder) = builder.as_mut() {
        match value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
}

fn append_decimal128(builder: &mut Option<Decimal128Builder>, value: Option<i128>) {
    if let Some(builder) = builder.as_mut() {
        match value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
}

fn append_float64(builder: &mut Option<Float64Builder>, value: Option<f64>) {
    if let Some(builder) = builder.as_mut() {
        match value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
}

fn append_string(builder: &mut Option<StringBuilder>, value: Option<&str>) {
    if let Some(builder) = builder.as_mut() {
        match value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
}

fn append_boolean(builder: &mut Option<BooleanBuilder>, value: Option<bool>) {
    if let Some(builder) = builder.as_mut() {
        match value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
}

fn append_binary(builder: &mut Option<BinaryBuilder>, value: Option<&[u8]>) {
    if let Some(builder) = builder.as_mut() {
        match value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }
}

fn single_series_metadata(sensor: &Sensor) -> HashMap<String, String> {
    let mut metadata = HashMap::from([
        ("sensapp.sensor.uuid".to_string(), sensor.uuid.to_string()),
        ("sensapp.sensor.name".to_string(), sensor.name.clone()),
        (
            "sensapp.sensor.type".to_string(),
            sensor_type_name(&sensor.sensor_type).to_string(),
        ),
        (
            "sensapp.sensor.labels".to_string(),
            labels_to_json_string(sensor),
        ),
    ]);

    if let Some(unit) = &sensor.unit {
        metadata.insert("sensapp.sensor.unit".to_string(), unit.name.clone());
    }

    metadata
}

fn labels_to_json_string(sensor: &Sensor) -> String {
    use serde_json::{Map, Value};

    let mut map = Map::new();
    for (key, value) in sensor.labels.iter() {
        map.insert(key.clone(), Value::String(value.clone()));
    }
    Value::Object(map).to_string()
}

fn sensor_type_name(sensor_type: &SensorType) -> &'static str {
    match sensor_type {
        SensorType::Integer => "integer",
        SensorType::Numeric => "numeric",
        SensorType::Float => "float",
        SensorType::String => "string",
        SensorType::Boolean => "boolean",
        SensorType::Location => "location",
        SensorType::Json => "json",
        SensorType::Blob => "blob",
    }
}

trait ToMicroseconds {
    fn to_microseconds_since_epoch(&self) -> i64;
}

impl ToMicroseconds for SensAppDateTime {
    fn to_microseconds_since_epoch(&self) -> i64 {
        (self.to_duration_since_j1900().total_nanoseconds() / 1000 - 2_208_988_800_000_000) as i64
    }
}

#[cfg(test)]
pub mod test_data_helpers {
    use super::*;
    use crate::datamodel::unit::Unit;
    use crate::datamodel::*;
    use geo::Point;
    use smallvec::{SmallVec, smallvec};
    use uuid::Uuid;

    pub fn create_test_sensor_data_integer() -> SensorData {
        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_sensor".to_string(),
            sensor_type: SensorType::Integer,
            unit: Some(Unit::new("Celsius".to_string(), None)),
            labels: SmallVec::from_vec(vec![("room".to_string(), "lab".to_string())]),
        };

        let datetime1 = SensAppDateTime::now().unwrap();
        let datetime2 = datetime1 + hifitime::Duration::from_seconds(1.0);

        let samples = TypedSamples::Integer(smallvec![
            Sample {
                datetime: datetime1,
                value: 42,
            },
            Sample {
                datetime: datetime2,
                value: 84,
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
        let location = Point::new(2.3522, 48.8566);

        let samples = TypedSamples::Location(smallvec![Sample {
            datetime: datetime1,
            value: location,
        }]);

        SensorData::new(sensor, samples)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::{Sample, Sensor, SensorData, SensorType, TypedSamples};
    use arrow::array::{Array, BooleanArray, Float64Array};
    use arrow_ipc::reader::StreamReader;
    use geo::Point;
    use smallvec::{SmallVec, smallvec};
    use std::collections::BTreeSet;
    use std::io::Cursor;
    use uuid::Uuid;

    #[test]
    fn test_arrow_conversion_integer() {
        let sensor_data = test_data_helpers::create_test_sensor_data_integer();

        let batch = ArrowConverter::to_record_batch(&sensor_data).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "value");
        assert!(matches!(
            schema.field(0).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ));
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert_eq!(
            schema
                .metadata()
                .get("sensapp.sensor.name")
                .map(String::as_str),
            Some("test_sensor")
        );
        assert_eq!(
            schema
                .metadata()
                .get("sensapp.sensor.type")
                .map(String::as_str),
            Some("integer")
        );
        assert_eq!(
            schema
                .metadata()
                .get("sensapp.sensor.unit")
                .map(String::as_str),
            Some("Celsius")
        );
    }

    #[test]
    fn test_arrow_stream_format_roundtrip() {
        let sensor_data = test_data_helpers::create_test_sensor_data_integer();

        let arrow_bytes = ArrowConverter::to_arrow_stream(&sensor_data).unwrap();
        assert!(!arrow_bytes.is_empty());

        let reader = StreamReader::try_new(Cursor::new(arrow_bytes), None).unwrap();
        let batches: Vec<_> = reader.into_iter().map(|batch| batch.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_location_conversion() {
        let sensor_data = test_data_helpers::create_test_sensor_data_location();

        let batch = ArrowConverter::to_record_batch(&sensor_data).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(matches!(
            batch.schema().field(1).data_type(),
            DataType::Struct(_)
        ));
    }

    #[test]
    fn test_arrow_multi_uses_sparse_typed_columns() {
        let float_sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "temperature".to_string(),
            sensor_type: SensorType::Float,
            unit: Some(crate::datamodel::unit::Unit::new("C".to_string(), None)),
            labels: smallvec![("room".to_string(), "lab".to_string())],
        };
        let bool_sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "occupied".to_string(),
            sensor_type: SensorType::Boolean,
            unit: None,
            labels: SmallVec::new(),
        };

        let now = SensAppDateTime::now().unwrap();
        let results = vec![
            SensorData::new(
                float_sensor,
                TypedSamples::Float(smallvec![Sample {
                    datetime: now,
                    value: 21.5,
                }]),
            ),
            SensorData::new(
                bool_sensor,
                TypedSamples::Boolean(smallvec![Sample {
                    datetime: now,
                    value: true,
                }]),
            ),
        ];

        let bytes = ArrowConverter::to_arrow_stream_multi(&results).unwrap();
        let reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        let schema = batch.schema();
        let field_names: BTreeSet<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();

        assert!(field_names.contains("timestamp"));
        assert!(field_names.contains("sensor_id"));
        assert!(field_names.contains("sensor_name"));
        assert!(field_names.contains("sensor_type"));
        assert!(field_names.contains("unit"));
        assert!(field_names.contains("labels"));
        assert!(field_names.contains("float_value"));
        assert!(field_names.contains("boolean_value"));
        assert!(!field_names.contains("integer_value"));

        assert!(matches!(
            schema.field(1).data_type(),
            DataType::Dictionary(_, _)
        ));
        assert!(matches!(
            schema.field(2).data_type(),
            DataType::Dictionary(_, _)
        ));
        assert!(matches!(
            schema.field(3).data_type(),
            DataType::Dictionary(_, _)
        ));

        let float_idx = schema
            .fields()
            .iter()
            .position(|field| field.name() == "float_value")
            .unwrap();
        let boolean_idx = schema
            .fields()
            .iter()
            .position(|field| field.name() == "boolean_value")
            .unwrap();

        let float_col = batch
            .column(float_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let boolean_col = batch
            .column(boolean_idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert_eq!(float_col.value(0), 21.5);
        assert!(float_col.is_null(1));
        assert!(boolean_col.is_null(0));
        assert!(boolean_col.value(1));
    }

    #[test]
    fn test_arrow_multi_location_uses_latitude_and_longitude_columns() {
        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "gps".to_string(),
            sensor_type: SensorType::Location,
            unit: None,
            labels: SmallVec::new(),
        };

        let now = SensAppDateTime::now().unwrap();
        let results = vec![SensorData::new(
            sensor,
            TypedSamples::Location(smallvec![Sample {
                datetime: now,
                value: Point::new(10.3951, 63.4305),
            }]),
        )];

        let bytes = ArrowConverter::to_arrow_stream_multi(&results).unwrap();
        let reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        let schema = batch.schema();

        assert!(
            schema
                .fields()
                .iter()
                .any(|field| field.name() == "latitude")
        );
        assert!(
            schema
                .fields()
                .iter()
                .any(|field| field.name() == "longitude")
        );
        assert!(
            !schema
                .fields()
                .iter()
                .any(|field| field.name() == "float_value")
        );

        let lat_idx = schema
            .fields()
            .iter()
            .position(|field| field.name() == "latitude")
            .unwrap();
        let lon_idx = schema
            .fields()
            .iter()
            .position(|field| field.name() == "longitude")
            .unwrap();

        let lat = batch
            .column(lat_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let lon = batch
            .column(lon_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(lat.value(0), 63.4305);
        assert_eq!(lon.value(0), 10.3951);
    }
}
