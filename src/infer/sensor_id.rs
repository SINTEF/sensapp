use super::uuid::attempt_uuid_parsing;

/// Represents different types of sensor identifiers
#[derive(Debug, Clone, PartialEq)]
pub enum SensorId {
    /// UUID-based sensor identifier
    Uuid(uuid::Uuid),
    /// Name-based sensor identifier
    Name(String),
}

impl From<uuid::Uuid> for SensorId {
    fn from(uuid: uuid::Uuid) -> Self {
        SensorId::Uuid(uuid)
    }
}

impl From<String> for SensorId {
    fn from(name: String) -> Self {
        SensorId::Name(name)
    }
}

impl From<&str> for SensorId {
    fn from(name: &str) -> Self {
        SensorId::Name(name.to_string())
    }
}

/// Detect the type of sensor identifier from a string
/// Uses the fast nom-based UUID parser for optimal performance
pub fn detect_sensor_id(input: &str) -> SensorId {
    // Try to parse as UUID using the fast nom-based parser
    if let Some(uuid) = attempt_uuid_parsing(input) {
        SensorId::Uuid(uuid)
    } else {
        // If not a UUID, treat as a name
        SensorId::Name(input.to_string())
    }
}

/// Check if a string is a valid UUID using the nom-based parser
pub fn is_valid_uuid(input: &str) -> bool {
    attempt_uuid_parsing(input).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_sensor_id_with_uuid() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let sensor_id = detect_sensor_id(uuid_str);

        // Test that it detected a UUID (this is the core functionality we actually use)
        match sensor_id {
            SensorId::Uuid(_) => (), // Good, it's a UUID
            SensorId::Name(_) => panic!("Expected UUID but got Name"),
        }
    }

    #[test]
    fn test_detect_sensor_id_with_name() {
        let name = "temperature_sensor_01";
        let sensor_id = detect_sensor_id(name);

        // Test that it detected a name (this is the core functionality we actually use)
        match sensor_id {
            SensorId::Name(detected_name) => assert_eq!(detected_name, name),
            SensorId::Uuid(_) => panic!("Expected Name but got UUID"),
        }
    }

    #[test]
    fn test_is_valid_uuid() {
        // This function IS used in csv.rs
        assert!(is_valid_uuid("550e8400-e29b-41d4-a716-446655440000"));
        assert!(!is_valid_uuid("not-a-uuid"));
        assert!(!is_valid_uuid(""));
        assert!(!is_valid_uuid("550e8400-e29b-41d4-a716")); // Incomplete UUID
    }
}
