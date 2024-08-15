use gcp_bigquery_client::storage::{ColumnType, FieldDescriptor, TableDescriptor};
use once_cell::sync::Lazy;

pub static UNITS_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        FieldDescriptor {
            name: "id".to_string(),
            number: 1,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "name".to_string(),
            number: 2,
            typ: ColumnType::String,
        },
        FieldDescriptor {
            name: "description".to_string(),
            number: 3,
            typ: ColumnType::String,
        },
    ],
});

pub static SENSORS_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        FieldDescriptor {
            name: "sensor_id".to_string(),
            number: 1,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "uuid".to_string(),
            number: 2,
            typ: ColumnType::String,
        },
        FieldDescriptor {
            name: "name".to_string(),
            number: 3,
            typ: ColumnType::String,
        },
        FieldDescriptor {
            name: "created_at".to_string(),
            number: 4,
            typ: ColumnType::Timestamp,
        },
        FieldDescriptor {
            name: "type".to_string(),
            number: 5,
            typ: ColumnType::String,
        },
        FieldDescriptor {
            name: "unit".to_string(),
            number: 6,
            typ: ColumnType::Int64,
        },
    ],
});

pub static LABELS_NAME_DICTIONARY_DESCRIPTOR: Lazy<TableDescriptor> =
    Lazy::new(|| TableDescriptor {
        field_descriptors: vec![
            FieldDescriptor {
                name: "id".to_string(),
                number: 1,
                typ: ColumnType::Int64,
            },
            FieldDescriptor {
                name: "name".to_string(),
                number: 2,
                typ: ColumnType::String,
            },
        ],
    });

pub static LABELS_DESCRIPTION_DICTIONARY_DESCRIPTOR: Lazy<TableDescriptor> =
    Lazy::new(|| TableDescriptor {
        field_descriptors: vec![
            FieldDescriptor {
                name: "id".to_string(),
                number: 1,
                typ: ColumnType::Int64,
            },
            FieldDescriptor {
                name: "description".to_string(),
                number: 2,
                typ: ColumnType::String,
            },
        ],
    });

pub static LABELS_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        FieldDescriptor {
            name: "sensor_id".to_string(),
            number: 1,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "name".to_string(),
            number: 2,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "description".to_string(),
            number: 3,
            typ: ColumnType::Int64,
        },
    ],
});

pub static STRINGS_VALUES_DICTIONARY_DESCRIPTOR: Lazy<TableDescriptor> =
    Lazy::new(|| TableDescriptor {
        field_descriptors: vec![
            FieldDescriptor {
                name: "id".to_string(),
                number: 1,
                typ: ColumnType::Int64,
            },
            FieldDescriptor {
                name: "value".to_string(),
                number: 2,
                typ: ColumnType::String,
            },
        ],
    });

pub static INTEGER_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        FieldDescriptor {
            name: "sensor_id".to_string(),
            number: 1,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "timestamp".to_string(),
            number: 2,
            typ: ColumnType::Timestamp,
        },
        FieldDescriptor {
            name: "value".to_string(),
            number: 3,
            typ: ColumnType::Int64,
        },
    ],
});

pub static NUMERIC_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        FieldDescriptor {
            name: "sensor_id".to_string(),
            number: 1,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "timestamp".to_string(),
            number: 2,
            typ: ColumnType::Timestamp,
        },
        FieldDescriptor {
            name: "value".to_string(),
            number: 3,
            typ: ColumnType::Bytes,
        },
    ],
});

pub static FLOAT_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        FieldDescriptor {
            name: "sensor_id".to_string(),
            number: 1,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "timestamp".to_string(),
            number: 2,
            typ: ColumnType::Timestamp,
        },
        FieldDescriptor {
            name: "value".to_string(),
            number: 3,
            typ: ColumnType::Float64,
        },
    ],
});

pub static STRING_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        FieldDescriptor {
            name: "sensor_id".to_string(),
            number: 1,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "timestamp".to_string(),
            number: 2,
            typ: ColumnType::Timestamp,
        },
        FieldDescriptor {
            name: "value".to_string(),
            number: 3,
            typ: ColumnType::Int64,
        },
    ],
});

pub static BOOLEAN_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        FieldDescriptor {
            name: "sensor_id".to_string(),
            number: 1,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "timestamp".to_string(),
            number: 2,
            typ: ColumnType::Timestamp,
        },
        FieldDescriptor {
            name: "value".to_string(),
            number: 3,
            typ: ColumnType::Bool,
        },
    ],
});

pub static LOCATION_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        FieldDescriptor {
            name: "sensor_id".to_string(),
            number: 1,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "timestamp".to_string(),
            number: 2,
            typ: ColumnType::Timestamp,
        },
        FieldDescriptor {
            name: "latitude".to_string(),
            number: 3,
            typ: ColumnType::Float64,
        },
        FieldDescriptor {
            name: "longitude".to_string(),
            number: 4,
            typ: ColumnType::Float64,
        },
    ],
});

pub static JSON_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        FieldDescriptor {
            name: "sensor_id".to_string(),
            number: 1,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "timestamp".to_string(),
            number: 2,
            typ: ColumnType::Timestamp,
        },
        FieldDescriptor {
            name: "value".to_string(),
            number: 3,
            typ: ColumnType::Json,
        },
    ],
});

pub static BLOB_VALUES_DESCRIPTOR: Lazy<TableDescriptor> = Lazy::new(|| TableDescriptor {
    field_descriptors: vec![
        FieldDescriptor {
            name: "sensor_id".to_string(),
            number: 1,
            typ: ColumnType::Int64,
        },
        FieldDescriptor {
            name: "timestamp".to_string(),
            number: 2,
            typ: ColumnType::Timestamp,
        },
        FieldDescriptor {
            name: "value".to_string(),
            number: 3,
            typ: ColumnType::Bytes,
        },
    ],
});
