use crate::name_to_uuid::uuid_v8_blake3;

use super::{unit::Unit, SensAppLabels, SensorType};
use anyhow::{anyhow, Error};
use smallvec::SmallVec;
use std::fmt;
use uuid::Uuid;

#[derive(Debug)]
pub struct Sensor {
    pub uuid: Uuid,
    pub name: String,
    pub sensor_type: SensorType,
    pub unit: Option<Unit>,
    pub labels: SensAppLabels,
}

impl fmt::Display for Sensor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Sensor {{ uuid: {}, name: {}, sensor_type: {:?}",
            self.uuid, self.name, self.sensor_type
        )?;

        if let Some(unit) = &self.unit {
            write!(f, ", unit: {}", unit)?;
        }

        if !self.labels.is_empty() {
            write!(f, ", labels: {:?}", self.labels)?;
        }

        write!(f, " }}")
    }
}

/// Sorts the labels in the given vector by key.
/// This is done in-place.
fn sort_labels(labels: &mut SensAppLabels) {
    labels.sort_by(|(a_key, a_value), (b_key, b_value)| {
        if a_key == b_key {
            a_value.cmp(b_value)
        } else {
            a_key.cmp(b_key)
        }
    });
}

/// Checks if the given string contains any of the special ASCII characters.
///
/// The special characters checked are:
/// - Vertical Tab (VT) - ASCII 11 (0x0B)
/// - File Separator (FS) - ASCII 28 (0x1C)
/// - Group Separator (GS) - ASCII 29 (0x1D)
/// - Record Separator (RS) - ASCII 30 (0x1E)
/// - Unit Separator (US) - ASCII 31 (0x1F)
///
/// Arguments:
/// * `s`: The string slice to check for special characters.
///
/// Returns:
/// * `bool`: `true` if any of the special characters are found, `false` otherwise.
fn contains_special_chars(s: &str) -> bool {
    s.bytes().any(|b| matches!(b, 11 | 28 | 29 | 30 | 31))
}

fn compute_uuid_buffer(
    name: &str,
    sensor_type: &SensorType,
    unit: &Option<Unit>,
    sorted_labels: &Option<SensAppLabels>,
) -> Result<Vec<u8>, Error> {
    if contains_special_chars(name) {
        return Err(anyhow!(
            "The name '{}' contains special characters. Please remove them.",
            name
        ));
    }
    let mut buffer_size = name.len()
        + 1 // Record Separator (RS) - ASCII 30 (0x1E)
        + 1 // Sensor type
        + 1 // Record Separator (RS) - ASCII 30 (0x1E)
        + match unit {
            None => 0,
            Some(unit) => unit.name.len() + 1,
        }
        + 1; // Record Separator (RS) - ASCII 30 (0x1E)

    if let Some(sorted_labels) = sorted_labels {
        for (key, value) in sorted_labels.iter() {
            if contains_special_chars(key) {
                return Err(anyhow!(
                    "The tag key '{}' contains special characters. Please remove them.",
                    key
                ));
            }
            if contains_special_chars(value) {
                return Err(anyhow!(
                    "The tag value '{}' contains special characters. Please remove them.",
                    value
                ));
            }
            buffer_size += key.len()
            + 1 // Unit Separator (US) - ASCII 31 (0x1F)
             + value.len()
             + 1 // Record Separator (RS) - ASCII 30 (0x1E)
        }
    }

    let mut buffer = Vec::with_capacity(buffer_size);
    buffer.extend_from_slice(name.as_bytes());
    buffer.push(30u8); // Record Separator (RS) - ASCII 30 (0x1E)
    sensor_type.write_to(&mut buffer)?;
    buffer.push(30u8); // Record Separator (RS) - ASCII 30 (0x1E)
    if let Some(unit) = unit {
        buffer.extend_from_slice(unit.name.as_bytes());
    }
    buffer.push(30u8); // Record Separator (RS) - ASCII 30 (0x1E)
    if let Some(sorted_labels) = sorted_labels {
        for (key, value) in sorted_labels.iter() {
            buffer.extend_from_slice(key.as_bytes());
            buffer.push(31u8); // Unit Separator (US) - ASCII 31 (0x1F)
            buffer.extend_from_slice(value.as_bytes());
            buffer.push(30u8); // Record Separator (RS) - ASCII 30 (0x1E)
        }
    }

    Ok(buffer)
}

impl Sensor {
    pub fn new(
        uuid: Uuid,
        name: String,
        sensor_type: SensorType,
        unit: Option<Unit>,
        labels: Option<SensAppLabels>,
    ) -> Self {
        Self {
            uuid,
            name,
            sensor_type,
            unit,
            labels: {
                match labels {
                    None => SmallVec::new(),
                    Some(mut labels) => {
                        sort_labels(&mut labels);
                        labels
                    }
                }
            },
        }
    }

    pub fn new_without_uuid(
        name: String,
        sensor_type: SensorType,
        unit: Option<Unit>,
        labels: Option<SensAppLabels>,
    ) -> Result<Self, Error> {
        let sorted_labels = match labels {
            None => None,
            Some(mut labels) => {
                sort_labels(&mut labels);
                Some(labels)
            }
        };
        let uuid_buffer = compute_uuid_buffer(&name, &sensor_type, &unit, &sorted_labels)?;
        let uuid = uuid_v8_blake3(&name, uuid_buffer)?;
        Ok(Self {
            uuid,
            name,
            sensor_type,
            unit,
            labels: sorted_labels.unwrap_or_else(SmallVec::new),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::config::load_configuration;

    use super::*;

    #[test]
    fn test_sort_labels() {
        // Classic
        let mut labels: SensAppLabels = SmallVec::new();
        labels.push(("b".to_string(), "2".to_string()));
        labels.push(("a".to_string(), "1".to_string()));
        sort_labels(&mut labels);
        assert_eq!(labels[0].0, "a");
        assert_eq!(labels[1].0, "b");

        // Same key
        labels.push(("b".to_string(), "1".to_string()));
        sort_labels(&mut labels);
        assert_eq!(labels[0].1, "1");
        assert_eq!(labels[1].0, "b");
        assert_eq!(labels[1].1, "1");
        assert_eq!(labels[2].1, "2");

        // Empty
        let mut empty_labels: SensAppLabels = SmallVec::new();
        sort_labels(&mut empty_labels);
        assert!(empty_labels.is_empty());
    }

    #[test]
    fn test_contains_special_chars() {
        assert!(contains_special_chars("\x0Btest"));
        assert!(contains_special_chars("test\x1C"));
        assert!(!contains_special_chars("normal_string"));
    }

    #[test]
    fn test_compute_uuid_buffer() {
        let name = "TestSensor";
        let sensor_type = SensorType::Numeric;
        let unit = Some(Unit::new("Celsius".to_string(), None));
        let mut labels: SensAppLabels = SmallVec::new();
        labels.push(("location".to_string(), "office".to_string()));
        let labels = Some(labels);
        let result = compute_uuid_buffer(name, &sensor_type, &unit, &labels);
        assert!(result.is_ok());

        // Change the unit
        let unit = None;
        let result_2 = compute_uuid_buffer(name, &sensor_type, &unit, &labels);
        assert!(result_2.is_ok());
        // Compare taht result and result2 are different
        assert_ne!(result.unwrap(), result_2.unwrap());

        // Test with special characters in name
        let name_with_special_char = "Test\x0BSensor";
        let result = compute_uuid_buffer(name_with_special_char, &sensor_type, &unit, &labels);
        assert!(result.is_err());

        // Test with special characters in tag key
        let mut labels: SensAppLabels = SmallVec::new();
        labels.push(("location\x0B".to_string(), "office".to_string()));
        let labels = Some(labels);
        let result = compute_uuid_buffer(name, &sensor_type, &unit, &labels);
        assert!(result.is_err());

        // Test with special characters in tag value
        let mut labels: SensAppLabels = SmallVec::new();
        labels.push(("location".to_string(), "office\x0B".to_string()));
        let labels = Some(labels);
        let result = compute_uuid_buffer(name, &sensor_type, &unit, &labels);
        assert!(result.is_err());
    }

    #[test]
    fn test_sensor_new() {
        let uuid = Uuid::new_v4();
        let sensor = Sensor::new(
            uuid,
            "TestSensor".to_string(),
            SensorType::Integer,
            Some(Unit::new("Celsius".to_string(), None)),
            None,
        );
        assert_eq!(sensor.uuid, uuid);
        assert_eq!(sensor.name, "TestSensor");

        let mut labels: SensAppLabels = SmallVec::new();
        labels.push(("location".to_string(), "office".to_string()));
        let sensor = Sensor::new(
            uuid,
            "TestSensor".to_string(),
            SensorType::Integer,
            Some(Unit::new("Celsius".to_string(), None)),
            Some(labels),
        );
        assert_eq!(sensor.labels.len(), 1);
    }

    #[test]
    fn test_sensor_new_without_uuid() {
        _ = load_configuration();
        let sensor = Sensor::new_without_uuid(
            "TestSensor".to_string(),
            SensorType::Location,
            Some(Unit::new("WGS84".to_string(), None)),
            None,
        )
        .unwrap();
        assert_eq!(sensor.name, "TestSensor");
        let uuid = sensor.uuid;
        assert_eq!(
            uuid,
            Uuid::from_str("20115fa5-aecd-8271-835d-07bfee981d6a").unwrap()
        );

        // Let's add a tag
        let mut labels: SensAppLabels = SmallVec::new();
        labels.push(("location".to_string(), "office".to_string()));
        let sensor = Sensor::new_without_uuid(
            "TestSensor".to_string(),
            SensorType::Location,
            Some(Unit::new("WGS84".to_string(), None)),
            Some(labels),
        )
        .unwrap();
        assert_eq!(
            sensor.uuid,
            // Note that the beginning of the UUID is the same as the previous one
            Uuid::from_str("20115fa5-33a2-8870-876d-ff32d73b2419").unwrap()
        );
    }

    #[test]
    fn test_sensor_to_string() {
        let mut labels: SensAppLabels = SmallVec::new();
        labels.push(("location".to_string(), "office".to_string()));
        let sensor = Sensor::new(
            Uuid::new_v4(),
            "TestSensor".to_string(),
            SensorType::Integer,
            Some(Unit::new("Celsius".to_string(), None)),
            Some(labels),
        );
        let s = sensor.to_string();
        assert!(s.contains("Sensor"));
        assert!(s.contains("TestSensor"));
        assert!(s.contains("Integer"));
        assert!(s.contains("Celsius"));
        assert!(s.contains("location"));
        assert!(s.contains("office"));
    }
}
