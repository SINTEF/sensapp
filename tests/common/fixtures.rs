use hifitime::Epoch;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
/// Test data fixtures for consistent testing
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples, unit::Unit};
use std::sync::Arc;
use uuid::Uuid;

/// Sample CSV data for temperature sensor
pub fn temperature_sensor_csv() -> &'static str {
    r#"datetime,sensor_name,value,unit
2024-01-01T00:00:00Z,temperature,20.5,°C
2024-01-01T00:01:00Z,temperature,21.0,°C
2024-01-01T00:02:00Z,temperature,21.5,°C
2024-01-01T00:03:00Z,temperature,22.0,°C
2024-01-01T00:04:00Z,temperature,20.8,°C"#
}

/// Sample CSV data for multiple sensors
pub fn multi_sensor_csv() -> &'static str {
    r#"datetime,sensor_name,value,unit
2024-01-01T00:00:00Z,temperature,20.5,°C
2024-01-01T00:00:00Z,humidity,65.0,%
2024-01-01T00:01:00Z,temperature,21.0,°C
2024-01-01T00:01:00Z,humidity,64.5,%
2024-01-01T00:02:00Z,temperature,21.5,°C
2024-01-01T00:02:00Z,humidity,64.0,%"#
}

/// Sample JSON data for testing
pub fn temperature_sensor_json() -> &'static str {
    r#"[
  {"datetime": "2024-01-01T00:00:00Z", "sensor_name": "temperature", "value": 20.5, "unit": "°C"},
  {"datetime": "2024-01-01T00:01:00Z", "sensor_name": "temperature", "value": 21.0, "unit": "°C"},
  {"datetime": "2024-01-01T00:02:00Z", "sensor_name": "temperature", "value": 21.5, "unit": "°C"}
]"#
}

/// Sample SenML data
pub fn temperature_sensor_senml() -> &'static str {
    r#"[
  {"bn": "temperature", "bt": 1704067200, "v": 20.5, "u": "Cel"},
  {"t": 60, "v": 21.0},
  {"t": 120, "v": 21.5}
]"#
}

/// Sample InfluxDB line protocol data
pub fn temperature_sensor_influxdb() -> &'static str {
    r#"temperature,location=room1 value=20.5 1704067200000000000
temperature,location=room1 value=21.0 1704067260000000000
temperature,location=room1 value=21.5 1704067320000000000"#
}

/// Create a test sensor with specified parameters
pub fn create_test_sensor(name: &str, sensor_type: SensorType) -> Arc<Sensor> {
    Arc::new(Sensor {
        uuid: Uuid::new_v4(),
        name: name.to_string(),
        unit: match name {
            "temperature" => Some(Unit::new("°C".to_string(), None)),
            "humidity" => Some(Unit::new("%".to_string(), None)),
            _ => None,
        },
        sensor_type,
        labels: SensAppLabels::new(),
    })
}

/// Create test samples for a sensor
pub fn create_test_float_samples(count: usize, start_value: f64) -> TypedSamples {
    let samples = (0..count)
        .map(|i| Sample {
            datetime: Epoch::from_unix_seconds((1704067200 + i * 60) as f64), // Start from 2024-01-01
            value: start_value + (i as f64 * 0.5),
        })
        .collect();
    TypedSamples::Float(samples)
}

/// Create test integer samples
pub fn create_test_integer_samples(count: usize, start_value: i64) -> TypedSamples {
    let samples = (0..count)
        .map(|i| Sample {
            datetime: Epoch::from_unix_seconds((1704067200 + i * 60) as f64),
            value: start_value + i as i64,
        })
        .collect();
    TypedSamples::Integer(samples)
}

/// Create test boolean samples
pub fn create_test_boolean_samples(count: usize) -> TypedSamples {
    let samples = (0..count)
        .map(|i| Sample {
            datetime: Epoch::from_unix_seconds((1704067200 + i * 60) as f64),
            value: i % 2 == 0,
        })
        .collect();
    TypedSamples::Boolean(samples)
}

/// Create test string samples
pub fn create_test_string_samples(count: usize) -> TypedSamples {
    let samples = (0..count)
        .map(|i| Sample {
            datetime: Epoch::from_unix_seconds((1704067200 + i * 60) as f64),
            value: format!("sample_{}", i),
        })
        .collect();
    TypedSamples::String(samples)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixtures_are_valid() {
        // Ensure our fixtures are well-formed
        assert!(temperature_sensor_csv().contains("temperature"));
        assert!(temperature_sensor_json().starts_with('['));
        assert!(temperature_sensor_senml().contains("bn"));
        assert!(temperature_sensor_influxdb().contains("temperature"));
    }

    #[test]
    fn test_sample_creation() {
        let samples = create_test_float_samples(5, 20.0);
        if let TypedSamples::Float(float_samples) = samples {
            assert_eq!(float_samples.len(), 5);
            assert_eq!(float_samples[0].value, 20.0);
            assert_eq!(float_samples[4].value, 22.0);
        } else {
            panic!("Expected float samples");
        }
    }
}
