use gcp_bigquery_client::storage::{ColumnMode, ColumnType, FieldDescriptor, TableDescriptor};
use once_cell::sync::Lazy;

fn field(name: &str, number: u32, typ: ColumnType) -> FieldDescriptor {
    FieldDescriptor {
        name: name.to_string(),
        number,
        typ,
        mode: ColumnMode::Nullable,
    }
}

pub static UNITS_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        field("id", 1, ColumnType::Int64),
        field("name", 2, ColumnType::String),
        field("description", 3, ColumnType::String),
    ],
});

pub static SENSORS_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        field("sensor_id", 1, ColumnType::Int64),
        field("uuid", 2, ColumnType::String),
        field("name", 3, ColumnType::String),
        field("type", 4, ColumnType::String),
        field("unit", 5, ColumnType::Int64),
    ],
});

pub static LABELS_NAME_DICTIONARY_DESCRIPTOR: Lazy<TableDescriptor> =
    Lazy::new(|| TableDescriptor {
        field_descriptors: vec![
            field("id", 1, ColumnType::Int64),
            field("name", 2, ColumnType::String),
        ],
    });

pub static LABELS_DESCRIPTION_DICTIONARY_DESCRIPTOR: Lazy<TableDescriptor> =
    Lazy::new(|| TableDescriptor {
        field_descriptors: vec![
            field("id", 1, ColumnType::Int64),
            field("description", 2, ColumnType::String),
        ],
    });

pub static LABELS_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        field("sensor_id", 1, ColumnType::Int64),
        field("name", 2, ColumnType::Int64),
        field("description", 3, ColumnType::Int64),
    ],
});

pub static STRINGS_VALUES_DICTIONARY_DESCRIPTOR: Lazy<TableDescriptor> =
    Lazy::new(|| TableDescriptor {
        field_descriptors: vec![
            field("id", 1, ColumnType::Int64),
            field("value", 2, ColumnType::String),
        ],
    });

pub static INTEGER_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        field("sensor_id", 1, ColumnType::Int64),
        field("timestamp", 2, ColumnType::String),
        field("value", 3, ColumnType::Int64),
    ],
});

pub static NUMERIC_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        field("sensor_id", 1, ColumnType::Int64),
        field("timestamp", 2, ColumnType::String),
        field("value", 3, ColumnType::Bytes),
    ],
});

pub static FLOAT_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        field("sensor_id", 1, ColumnType::Int64),
        field("timestamp", 2, ColumnType::String),
        field("value", 3, ColumnType::Double),
    ],
});

pub static STRING_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        field("sensor_id", 1, ColumnType::Int64),
        field("timestamp", 2, ColumnType::String),
        field("value", 3, ColumnType::Int64),
    ],
});

pub static BOOLEAN_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        field("sensor_id", 1, ColumnType::Int64),
        field("timestamp", 2, ColumnType::String),
        field("value", 3, ColumnType::Bool),
    ],
});

pub static LOCATION_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        field("sensor_id", 1, ColumnType::Int64),
        field("timestamp", 2, ColumnType::String),
        field("latitude", 3, ColumnType::Double),
        field("longitude", 4, ColumnType::Double),
    ],
});

pub static JSON_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        field("sensor_id", 1, ColumnType::Int64),
        field("timestamp", 2, ColumnType::String),
        field("value", 3, ColumnType::String),
    ],
});

pub static BLOB_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        field("sensor_id", 1, ColumnType::Int64),
        field("timestamp", 2, ColumnType::String),
        field("value", 3, ColumnType::Bytes),
    ],
});
