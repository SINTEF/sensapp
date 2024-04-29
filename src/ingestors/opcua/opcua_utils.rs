use anyhow::{bail, Result};
use opcua::client::prelude::MonitoredItem;
use opcua::types::{DataValue, DateTime, DateTimeUtc, Identifier, NodeId, Variant};
use uuid::Uuid;

use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::sensapp_vec::SensAppLabels;
use crate::datamodel::{Sample, SensAppDateTime, SensAppVec, SensorType};
use crate::datamodel::{Sensor, TypedSamples};

pub fn node_id_to_name(name_prefix: Option<String>, node: &NodeId) -> String {
    let mut name = String::new();

    if let Some(prefix) = name_prefix {
        name.push_str(&prefix);
    }

    match &node.identifier {
        Identifier::Numeric(id) => {
            name.push_str(&id.to_string());
        }
        Identifier::String(id) => {
            name.push_str(id.as_ref());
        }
        Identifier::Guid(id) => {
            name.push_str(&id.to_string());
        }
        Identifier::ByteString(id) => {
            name.push_str(&id.as_base64());
        }
    }

    name
}

pub fn node_id_to_labels(node: &NodeId) -> SensAppLabels {
    let mut labels = SensAppLabels::with_capacity(7);

    labels.push(("namespace".to_string(), node.namespace.to_string()));

    let identifier_type = match &node.identifier {
        Identifier::Numeric(_) => "numeric",
        Identifier::String(_) => "string",
        Identifier::Guid(_) => "guid",
        Identifier::ByteString(_) => "binary",
    };

    labels.push(("identifier_kind".to_string(), identifier_type.to_string()));

    labels
}

fn variant_to_sensor_type(variant: &Variant) -> Option<SensorType> {
    match variant {
        Variant::Empty => None,

        Variant::Boolean(_) => Some(SensorType::Boolean),

        // This may look bad, but SensApp doesn't support
        // as many types for now.
        Variant::SByte(_)
        | Variant::Byte(_)
        | Variant::Int16(_)
        | Variant::UInt16(_)
        | Variant::Int32(_)
        | Variant::UInt32(_)
        | Variant::Int64(_)
        | Variant::UInt64(_) => Some(SensorType::Integer),

        Variant::Float(_) | Variant::Double(_) => Some(SensorType::Float),

        Variant::String(_) => Some(SensorType::String),

        // Let's store the datetypes as integers for now
        Variant::DateTime(_) => Some(SensorType::Integer),

        // A uuid will be binary
        Variant::Guid(_) => Some(SensorType::Blob),

        // Status codes are actually u32
        Variant::StatusCode(_) => Some(SensorType::Integer),

        // ByteString is binary
        Variant::ByteString(_) => Some(SensorType::Blob),

        // Please don't use SensApp for XML
        Variant::XmlElement(_) => Some(SensorType::String),

        // A bunch of types that we fallback to JSON, but
        // we are not expected to really use them.
        Variant::QualifiedName(_)
        | Variant::LocalizedText(_)
        | Variant::NodeId(_)
        | Variant::ExpandedNodeId(_)
        | Variant::ExtensionObject(_)
        | Variant::Variant(_)
        | Variant::DataValue(_)
        | Variant::DiagnosticInfo(_) => Some(SensorType::Json),

        // Array is a complex type, and for now we will use JSON too.
        // JSON is often the solution to problems.
        Variant::Array(_) => Some(SensorType::Json),
    }
}

pub fn data_value_to_sensor_type(data_value: &DataValue) -> Option<SensorType> {
    match &data_value.value {
        None => None,
        Some(variant) => variant_to_sensor_type(variant),
    }
}

pub fn monitored_item_to_sensor(monitored_item: &MonitoredItem) -> Result<Sensor> {
    let node_id = &monitored_item.item_to_monitor().node_id;
    let name = node_id_to_name(None, node_id);
    let labels = node_id_to_labels(node_id);

    let uuid: Option<Uuid> = match &node_id.identifier {
        // The opcua crate uses the uuid internally, but we need to
        // copy it through its bytes as it's a private field.
        Identifier::Guid(guid) => Some(Uuid::from_bytes(*guid.as_bytes())),
        _ => None,
    };

    //let sensor_type = data_value_to_sensor_type(monitored_item.last_value())
    //    .ok_or_else(|| anyhow::anyhow!("Cannot determine the sensor type"))?;

    // Iterate through the values and find the first non None one, or None if all are None
    let sensor_type = monitored_item
        .values()
        .iter()
        .find_map(data_value_to_sensor_type)
        .ok_or_else(|| anyhow::anyhow!("Cannot determine the sensor type"))?;

    match uuid {
        Some(uuid) => Ok(Sensor::new(uuid, name, sensor_type, None, Some(labels))),
        None => Sensor::new_without_uuid(name, sensor_type, None, Some(labels)),
    }
}

fn timestamp_to_datetime(timestamp: Option<DateTime>) -> Option<SensAppDateTime> {
    match timestamp {
        None => None,
        Some(timestamp) => {
            let chrono = timestamp.as_chrono();
            let timestamp_nano = chrono.timestamp_nanos_opt();
            timestamp_nano.map(SensAppDateTime::from_unix_nanoseconds_i64)
        }
    }
}

fn get_data_value_datetime_or_bail(data_value: &DataValue) -> Result<SensAppDateTime> {
    let datetime = timestamp_to_datetime(data_value.source_timestamp);
    if datetime.is_none() {
        let datetime = timestamp_to_datetime(data_value.server_timestamp);
        if datetime.is_none() {
            bail!("No timestamp found");
        }
    }
    Ok(datetime.unwrap())
}

fn data_values_to_boolean_samples(data_values: &[DataValue]) -> Result<TypedSamples> {
    let samples = data_values
        .iter()
        .map(|data_value| {
            let datetime = get_data_value_datetime_or_bail(data_value)?;
            let value = match data_value.value {
                None | Some(Variant::Empty) => None,
                Some(Variant::Boolean(value)) => Some(value),
                _ => bail!("Unexpected non boolean variant type"),
            };

            Ok(value.map(|value| Sample { datetime, value }))
        })
        .filter_map(|data_value| match data_value {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        })
        .collect::<Result<SensAppVec<Sample<bool>>>>()?;

    Ok(TypedSamples::Boolean(samples))
}

fn data_values_to_integer_samples(data_values: &[DataValue]) -> Result<TypedSamples> {
    let samples = data_values
        .iter()
        .map(|data_value| {
            let datetime = get_data_value_datetime_or_bail(data_value)?;
            let value = match data_value.value {
                None | Some(Variant::Empty) => None,
                Some(Variant::SByte(value)) => Some(value as i64),
                Some(Variant::Byte(value)) => Some(value as i64),
                Some(Variant::Int16(value)) => Some(value as i64),
                Some(Variant::UInt16(value)) => Some(value as i64),
                Some(Variant::Int32(value)) => Some(value as i64),
                Some(Variant::UInt32(value)) => Some(value as i64),
                Some(Variant::Int64(value)) => Some(value),
                Some(Variant::UInt64(value)) => Some(value as i64),
                // StatusCode are actually  a bitflags u32
                Some(Variant::StatusCode(value)) => Some(value.bits() as i64),
                // Otherwise we don't support it.
                _ => match &data_value.value {
                    // TODO: datetimes should perhaps not be stored
                    // as integers but as they own type.
                    // This may change if we need to support this.
                    Some(Variant::DateTime(value)) => value.as_chrono().timestamp_nanos_opt(),
                    _ => bail!("Unexpected non integer variant type"),
                },
            };

            Ok(value.map(|value| Sample { datetime, value }))
        })
        .filter_map(|data_value| match data_value {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        })
        .collect::<Result<SensAppVec<Sample<i64>>>>()?;

    Ok(TypedSamples::Integer(samples))
}

fn data_values_to_float_samples(data_values: &[DataValue]) -> Result<TypedSamples> {
    let samples = data_values
        .iter()
        .map(|data_value| {
            let datetime = get_data_value_datetime_or_bail(data_value)?;
            let value = match data_value.value {
                None | Some(Variant::Empty) => None,
                Some(Variant::Float(value)) => Some(value as f64),
                Some(Variant::Double(value)) => Some(value),
                _ => bail!("Unexpected non float variant type"),
            };

            Ok(value.map(|value| Sample { datetime, value }))
        })
        .filter_map(|data_value| match data_value {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        })
        .collect::<Result<SensAppVec<Sample<f64>>>>()?;

    Ok(TypedSamples::Float(samples))
}

fn data_values_to_string_samples(data_values: &[DataValue]) -> Result<TypedSamples> {
    let samples = data_values
        .iter()
        .map(|data_value| {
            let datetime = get_data_value_datetime_or_bail(data_value)?;
            let value = match &data_value.value {
                None | Some(Variant::Empty) => None,
                Some(Variant::String(value)) => Some(value.to_string()),
                Some(Variant::XmlElement(value)) => Some(value.to_string()),
                _ => bail!("Unexpected non string variant type"),
            };

            Ok(value.map(|value| Sample { datetime, value }))
        })
        .filter_map(|data_value| match data_value {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        })
        .collect::<Result<SensAppVec<Sample<String>>>>()?;

    Ok(TypedSamples::String(samples))
}

fn data_values_to_blob_samples(data_values: &[DataValue]) -> Result<TypedSamples> {
    let samples = data_values
        .iter()
        .map(|data_value| {
            let datetime = get_data_value_datetime_or_bail(data_value)?;
            let value = match &data_value.value {
                None | Some(Variant::Empty) => None,
                Some(Variant::Guid(value)) => Some(value.as_bytes().to_vec()),
                Some(Variant::ByteString(value)) => value.value.clone(),
                _ => bail!("Unexpected non blob variant type"),
            };

            Ok(value.map(|value| Sample { datetime, value }))
        })
        .filter_map(|data_value| match data_value {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        })
        .collect::<Result<SensAppVec<Sample<Vec<u8>>>>>()?;

    Ok(TypedSamples::Blob(samples))
}

fn data_values_to_json_samples(data_values: &[DataValue]) -> Result<TypedSamples> {
    let samples = data_values
        .iter()
        .map(|data_value| {
            let datetime = get_data_value_datetime_or_bail(data_value)?;
            let value = match &data_value.value {
                None | Some(Variant::Empty) => None,
                Some(Variant::QualifiedName(value)) => Some(serde_json::to_value(value)?),
                Some(Variant::LocalizedText(value)) => Some(serde_json::to_value(value)?),
                Some(Variant::NodeId(value)) => Some(serde_json::to_value(value)?),
                Some(Variant::ExpandedNodeId(value)) => Some(serde_json::to_value(value)?),
                Some(Variant::ExtensionObject(value)) => Some(serde_json::to_value(value)?),
                Some(Variant::Variant(value)) => Some(serde_json::to_value(value)?),
                Some(Variant::DataValue(value)) => Some(serde_json::to_value(value)?),
                Some(Variant::DiagnosticInfo(value)) => Some(serde_json::to_value(value)?),
                Some(Variant::Array(value)) => Some(serde_json::to_value(value)?),
                _ => bail!("Unexpected non json variant type"),
            };

            Ok(value.map(|value| Sample { datetime, value }))
        })
        .filter_map(|data_value| match data_value {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        })
        .collect::<Result<SensAppVec<Sample<serde_json::Value>>>>()?;

    Ok(TypedSamples::Json(samples))
}

pub fn data_values_to_typed_samples(
    sensor_type: SensorType,
    data_values: &[DataValue],
) -> Result<TypedSamples> {
    match sensor_type {
        SensorType::Boolean => data_values_to_boolean_samples(data_values),
        SensorType::Integer => data_values_to_integer_samples(data_values),
        SensorType::Float => data_values_to_float_samples(data_values),
        SensorType::String => data_values_to_string_samples(data_values),
        SensorType::Blob => data_values_to_blob_samples(data_values),
        SensorType::Json => data_values_to_json_samples(data_values),
        _ => bail!("Unsupported sensor type"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opcua::types::{
        Array, ByteString, ExpandedNodeId, ExtensionObject, Guid, LocalizedText, QualifiedName,
        StatusCode, UAString, VariantTypeId, XmlElement,
    };

    #[test]
    fn test_node_to_name() {
        let node_numeric = NodeId::new(1, 123);
        assert_eq!(node_id_to_name(None, &node_numeric), "123");
        assert_eq!(
            node_id_to_name(Some("prefix_".to_string()), &node_numeric),
            "prefix_123"
        );

        let node_string = NodeId::new(1, "abc");
        assert_eq!(node_id_to_name(None, &node_string), "abc");
        assert_eq!(
            node_id_to_name(Some("prefix_".to_string()), &node_string),
            "prefix_abc"
        );

        let node_guid = NodeId::new(1, Guid::null());
        assert_eq!(
            node_id_to_name(None, &node_guid),
            "00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(
            node_id_to_name(Some("prefix_".to_string()), &node_guid),
            "prefix_00000000-0000-0000-0000-000000000000"
        );

        let node_bytestring = NodeId::new(1, ByteString::from(vec![1, 2, 3]));
        assert_eq!(node_id_to_name(None, &node_bytestring), "AQID");
        assert_eq!(
            node_id_to_name(Some("prefix_".to_string()), &node_bytestring),
            "prefix_AQID"
        );
    }

    #[test]
    fn test_node_to_labels() {
        let node_numeric = NodeId::new(1, 123);
        let labels_numeric = node_id_to_labels(&node_numeric);
        assert_eq!(labels_numeric.len(), 2);
        assert_eq!(
            labels_numeric[0],
            ("namespace".to_string(), "1".to_string())
        );
        assert_eq!(
            labels_numeric[1],
            ("identifier_kind".to_string(), "numeric".to_string())
        );

        let node_string = NodeId::new(2, "abc");
        let labels_string = node_id_to_labels(&node_string);
        assert_eq!(labels_string.len(), 2);
        assert_eq!(labels_string[0], ("namespace".to_string(), "2".to_string()));
        assert_eq!(
            labels_string[1],
            ("identifier_kind".to_string(), "string".to_string())
        );

        let node_guid = NodeId::new(3, Guid::null());
        let labels_guid = node_id_to_labels(&node_guid);
        assert_eq!(labels_guid.len(), 2);
        assert_eq!(labels_guid[0], ("namespace".to_string(), "3".to_string()));
        assert_eq!(
            labels_guid[1],
            ("identifier_kind".to_string(), "guid".to_string())
        );

        let node_bytestring = NodeId::new(4, ByteString::from(vec![1, 2, 3]));
        let labels_bytestring = node_id_to_labels(&node_bytestring);
        assert_eq!(labels_bytestring.len(), 2);
        assert_eq!(
            labels_bytestring[0],
            ("namespace".to_string(), "4".to_string())
        );
        assert_eq!(
            labels_bytestring[1],
            ("identifier_kind".to_string(), "binary".to_string())
        );
    }

    #[test]
    fn test_variant_to_sensor_type() {
        // Test Empty variant
        assert_eq!(variant_to_sensor_type(&Variant::Empty), None);

        // Test Boolean variant
        assert_eq!(
            variant_to_sensor_type(&Variant::Boolean(true)),
            Some(SensorType::Boolean)
        );

        // Test integer variants
        assert_eq!(
            variant_to_sensor_type(&Variant::SByte(42)),
            Some(SensorType::Integer)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::Byte(42)),
            Some(SensorType::Integer)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::Int16(42)),
            Some(SensorType::Integer)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::UInt16(42)),
            Some(SensorType::Integer)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::Int32(42)),
            Some(SensorType::Integer)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::UInt32(42)),
            Some(SensorType::Integer)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::Int64(42)),
            Some(SensorType::Integer)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::UInt64(42)),
            Some(SensorType::Integer)
        );

        // Test float variants
        assert_eq!(
            variant_to_sensor_type(&Variant::Float(42.0)),
            Some(SensorType::Float)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::Double(42.0)),
            Some(SensorType::Float)
        );

        // Test string variant
        assert_eq!(
            variant_to_sensor_type(&Variant::String(UAString::from("value"))),
            Some(SensorType::String)
        );

        // Test DateTime variant
        assert_eq!(
            variant_to_sensor_type(&Variant::DateTime(Box::new(DateTime::default()))),
            Some(SensorType::Integer)
        );

        // Test Guid variant
        assert_eq!(
            variant_to_sensor_type(&Variant::Guid(Box::new(Guid::null()))),
            Some(SensorType::Blob)
        );

        // Test StatusCode variant
        assert_eq!(
            variant_to_sensor_type(&Variant::StatusCode(StatusCode::Good)),
            Some(SensorType::Integer)
        );

        // Test ByteString variant
        assert_eq!(
            variant_to_sensor_type(&Variant::ByteString(ByteString::default())),
            Some(SensorType::Blob)
        );

        // Test XmlElement variant
        assert_eq!(
            variant_to_sensor_type(&Variant::XmlElement(XmlElement::default())),
            Some(SensorType::String)
        );

        // Test fallback variants
        assert_eq!(
            variant_to_sensor_type(&Variant::QualifiedName(Box::new(QualifiedName::null()))),
            Some(SensorType::Json)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::LocalizedText(Box::new(LocalizedText::null()))),
            Some(SensorType::Json)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::NodeId(Box::new(NodeId::null()))),
            Some(SensorType::Json)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::ExpandedNodeId(Box::new(ExpandedNodeId::null()))),
            Some(SensorType::Json)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::ExtensionObject(Box::new(ExtensionObject::null()))),
            Some(SensorType::Json)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::Variant(Box::new(Variant::Empty))),
            Some(SensorType::Json)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::DataValue(Default::default())),
            Some(SensorType::Json)
        );
        assert_eq!(
            variant_to_sensor_type(&Variant::DiagnosticInfo(Default::default())),
            Some(SensorType::Json)
        );

        // Test Array variant
        assert_eq!(
            variant_to_sensor_type(&Variant::Array(Box::new(
                Array::new(VariantTypeId::Boolean, vec![Variant::Boolean(true)],).unwrap()
            ))),
            Some(SensorType::Json)
        );
    }


}
