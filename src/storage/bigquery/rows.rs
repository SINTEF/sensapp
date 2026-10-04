//! The rows sent to the Storage Write API and the descriptors of their tables.
//!
//! Types follow the table of supported protocol buffer types of the API: a TIMESTAMP is an `int64`
//! of microseconds since the epoch, a NUMERIC a decimal `string`, a JSON a `string`.
//! `migrations/init.sql` is the other side of this file; a test compares their columns.
//!
//! Floats are `double`, which the first version of the backend could not write: with
//! `gcp-bigquery-client` 0.22 a `ColumnType::Float64` descriptor was declared to the service as a protobuf
//! `float` (32 bits) while the row carried a `double`, and BigQuery stored NULL
//! (<https://github.com/lquerel/gcp-bigquery-client/issues/106>), hence the `f32` and its comment. Since
//! 0.26 the descriptor has `ColumnType::Double`, which is declared as `double`, and the integration test
//! `every_type_comes_back_as_written` checks that `0.1 + 0.2` comes back exactly.

use gcp_bigquery_client::storage::{ColumnMode, ColumnType, FieldDescriptor, TableDescriptor};
use prost::Message;
use std::sync::{Arc, LazyLock};

#[derive(Clone, PartialEq, Message)]
pub struct UnitRow {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
    #[prost(string, required, tag = "2")]
    pub name: String,
    #[prost(string, optional, tag = "3")]
    pub description: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SensorRow {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(string, required, tag = "2")]
    pub uuid: String,
    #[prost(string, required, tag = "3")]
    pub name: String,
    #[prost(string, required, tag = "4")]
    pub r#type: String,
    #[prost(int64, optional, tag = "5")]
    pub unit: Option<i64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct LabelRow {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(string, required, tag = "2")]
    pub name: String,
    #[prost(string, required, tag = "3")]
    pub description: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct IntegerValueRow {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(int64, required, tag = "2")]
    pub timestamp: i64,
    #[prost(int64, required, tag = "3")]
    pub value: i64,
}

#[derive(Clone, PartialEq, Message)]
pub struct NumericValueRow {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(int64, required, tag = "2")]
    pub timestamp: i64,
    #[prost(string, required, tag = "3")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct FloatValueRow {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(int64, required, tag = "2")]
    pub timestamp: i64,
    #[prost(double, required, tag = "3")]
    pub value: f64,
}

#[derive(Clone, PartialEq, Message)]
pub struct StringValueRow {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(int64, required, tag = "2")]
    pub timestamp: i64,
    #[prost(string, required, tag = "3")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct BooleanValueRow {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(int64, required, tag = "2")]
    pub timestamp: i64,
    #[prost(bool, required, tag = "3")]
    pub value: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct LocationValueRow {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(int64, required, tag = "2")]
    pub timestamp: i64,
    #[prost(double, required, tag = "3")]
    pub latitude: f64,
    #[prost(double, required, tag = "4")]
    pub longitude: f64,
}

#[derive(Clone, PartialEq, Message)]
pub struct JsonValueRow {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(int64, required, tag = "2")]
    pub timestamp: i64,
    #[prost(string, required, tag = "3")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct BlobValueRow {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(int64, required, tag = "2")]
    pub timestamp: i64,
    #[prost(bytes = "vec", required, tag = "3")]
    pub value: Vec<u8>,
}

/// Columns in the order of their protocol buffer tags, starting at 1.
fn descriptor(columns: &[(&str, ColumnType, ColumnMode)]) -> Arc<TableDescriptor> {
    Arc::new(TableDescriptor {
        field_descriptors: columns
            .iter()
            .zip(1..)
            .map(|((name, typ, mode), number)| FieldDescriptor {
                name: name.to_string(),
                number,
                typ: *typ,
                mode: *mode,
            })
            .collect(),
    })
}

fn sample_descriptor(value: &[(&str, ColumnType)]) -> Arc<TableDescriptor> {
    let mut columns = vec![
        ("sensor_id", ColumnType::Int64, ColumnMode::Required),
        ("timestamp", ColumnType::Int64, ColumnMode::Required),
    ];
    columns.extend(
        value
            .iter()
            .map(|(name, typ)| (*name, *typ, ColumnMode::Required)),
    );
    descriptor(&columns)
}

pub static UNITS: LazyLock<Arc<TableDescriptor>> = LazyLock::new(|| {
    descriptor(&[
        ("id", ColumnType::Int64, ColumnMode::Required),
        ("name", ColumnType::String, ColumnMode::Required),
        ("description", ColumnType::String, ColumnMode::Nullable),
    ])
});

pub static SENSORS: LazyLock<Arc<TableDescriptor>> = LazyLock::new(|| {
    descriptor(&[
        ("sensor_id", ColumnType::Int64, ColumnMode::Required),
        ("uuid", ColumnType::String, ColumnMode::Required),
        ("name", ColumnType::String, ColumnMode::Required),
        ("type", ColumnType::String, ColumnMode::Required),
        ("unit", ColumnType::Int64, ColumnMode::Nullable),
    ])
});

pub static LABELS: LazyLock<Arc<TableDescriptor>> = LazyLock::new(|| {
    descriptor(&[
        ("sensor_id", ColumnType::Int64, ColumnMode::Required),
        ("name", ColumnType::String, ColumnMode::Required),
        ("description", ColumnType::String, ColumnMode::Required),
    ])
});

pub static INTEGER_VALUES: LazyLock<Arc<TableDescriptor>> =
    LazyLock::new(|| sample_descriptor(&[("value", ColumnType::Int64)]));
pub static NUMERIC_VALUES: LazyLock<Arc<TableDescriptor>> =
    LazyLock::new(|| sample_descriptor(&[("value", ColumnType::String)]));
pub static FLOAT_VALUES: LazyLock<Arc<TableDescriptor>> =
    LazyLock::new(|| sample_descriptor(&[("value", ColumnType::Double)]));
pub static STRING_VALUES: LazyLock<Arc<TableDescriptor>> =
    LazyLock::new(|| sample_descriptor(&[("value", ColumnType::String)]));
pub static BOOLEAN_VALUES: LazyLock<Arc<TableDescriptor>> =
    LazyLock::new(|| sample_descriptor(&[("value", ColumnType::Bool)]));
pub static LOCATION_VALUES: LazyLock<Arc<TableDescriptor>> = LazyLock::new(|| {
    sample_descriptor(&[
        ("latitude", ColumnType::Double),
        ("longitude", ColumnType::Double),
    ])
});
pub static JSON_VALUES: LazyLock<Arc<TableDescriptor>> =
    LazyLock::new(|| sample_descriptor(&[("value", ColumnType::String)]));
pub static BLOB_VALUES: LazyLock<Arc<TableDescriptor>> =
    LazyLock::new(|| sample_descriptor(&[("value", ColumnType::Bytes)]));

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// `table -> [(column, SQL type)]`, read from the migration.
    fn schema() -> BTreeMap<String, Vec<(String, String)>> {
        let sql = include_str!("migrations/init.sql");
        let mut tables = BTreeMap::new();
        for statement in sql.split(';') {
            let Some(start) = statement.find("CREATE TABLE IF NOT EXISTS `{dataset}.") else {
                continue;
            };
            let after = &statement[start + "CREATE TABLE IF NOT EXISTS `{dataset}.".len()..];
            let (name, rest) = after.split_once('`').unwrap();
            let columns = &rest[rest.find('(').unwrap() + 1..rest.find("\n)").unwrap()];
            let columns = columns
                .lines()
                .map(|line| line.trim().trim_end_matches(','))
                .filter(|line| !line.is_empty())
                .map(|line| {
                    let mut words = line.split_whitespace();
                    (
                        words.next().unwrap().to_string(),
                        words.next().unwrap().to_string(),
                    )
                })
                .collect();
            tables.insert(name.to_string(), columns);
        }
        tables
    }

    fn proto_type(sql_type: &str) -> ColumnType {
        match sql_type {
            "INT64" | "TIMESTAMP" => ColumnType::Int64,
            "FLOAT64" => ColumnType::Double,
            "BOOL" => ColumnType::Bool,
            "BYTES" => ColumnType::Bytes,
            // STRING, JSON and NUMERIC (a decimal string)
            _ => ColumnType::String,
        }
    }

    #[test]
    fn descriptors_match_the_tables_of_the_migration() {
        let descriptors: [(&str, &Arc<TableDescriptor>); 11] = [
            ("units", &UNITS),
            ("sensors", &SENSORS),
            ("labels", &LABELS),
            ("integer_values", &INTEGER_VALUES),
            ("numeric_values", &NUMERIC_VALUES),
            ("float_values", &FLOAT_VALUES),
            ("string_values", &STRING_VALUES),
            ("boolean_values", &BOOLEAN_VALUES),
            ("location_values", &LOCATION_VALUES),
            ("json_values", &JSON_VALUES),
            ("blob_values", &BLOB_VALUES),
        ];
        let schema = schema();
        assert_eq!(schema.len(), descriptors.len(), "{:?}", schema.keys());
        for (table, descriptor) in descriptors {
            let columns = &schema[table];
            assert_eq!(
                columns.len(),
                descriptor.field_descriptors.len(),
                "columns of {table}"
            );
            for (field, (name, sql_type)) in descriptor.field_descriptors.iter().zip(columns) {
                assert_eq!(&field.name, name, "column of {table}");
                assert_eq!(
                    format!("{:?}", field.typ),
                    format!("{:?}", proto_type(sql_type)),
                    "type of {table}.{name}"
                );
            }
        }
    }

    #[test]
    fn the_tables_of_the_value_types_are_the_shared_list() {
        let schema = schema();
        for table in crate::storage::common::VALUE_TABLES {
            assert!(schema.contains_key(table), "{table}");
        }
    }

    #[test]
    fn tags_follow_the_column_order() {
        for descriptor in [&*UNITS, &*SENSORS, &*LOCATION_VALUES, &*BLOB_VALUES] {
            for (index, field) in descriptor.field_descriptors.iter().enumerate() {
                assert_eq!(field.number as usize, index + 1);
            }
        }
        // The rows encode the fields at those tags
        let row = LocationValueRow {
            sensor_id: 1,
            timestamp: 2,
            latitude: 3.0,
            longitude: 4.0,
        };
        let decoded = LocationValueRow::decode(row.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, row);
        assert_eq!(row.encode_to_vec()[0], 0x08, "field 1, varint");
    }
}
