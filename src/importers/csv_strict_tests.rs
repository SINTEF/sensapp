// Unit tests for strict mode parsing logic

use super::*;
use crate::datamodel::SensorType;

#[test]
fn test_parse_strict_formats() {
    // Test long format with names
    let data_grid = StringDataGrid::new(
        vec!["datetime".to_string(), "sensor_name".to_string(), "value".to_string()],
        vec![
            vec!["2024-01-01T00:00:00Z".to_string(), "temp1".to_string(), "22.5".to_string()],
            vec!["2024-01-01T01:00:00Z".to_string(), "temp2".to_string(), "23.1".to_string()],
        ],
    ).unwrap();

    let result = parse_csv_strict(data_grid);
    assert!(result.is_ok());
    let sensors = result.unwrap();
    assert_eq!(sensors.len(), 2);
    assert!(sensors.contains_key("temp1"));
    assert!(sensors.contains_key("temp2"));
}

#[test]
fn test_parse_strict_long_format_with_uuids() {
    let uuid1 = uuid::Uuid::new_v4();
    let uuid2 = uuid::Uuid::new_v4();
    let data_grid = StringDataGrid::new(
        vec!["datetime".to_string(), "sensor_uuid".to_string(), "value".to_string()],
        vec![
            vec!["2024-01-01T00:00:00Z".to_string(), uuid1.to_string(), "22.5".to_string()],
            vec!["2024-01-01T01:00:00Z".to_string(), uuid2.to_string(), "23.1".to_string()],
        ],
    ).unwrap();

    let result = parse_csv_strict(data_grid);
    assert!(result.is_ok());
    let sensors = result.unwrap();
    assert_eq!(sensors.len(), 2);
}

#[test]
fn test_parse_strict_wide_format_error() {
    let data_grid = StringDataGrid::new(
        vec!["datetime".to_string(), "temperature".to_string(), "humidity".to_string()],
        vec![
            vec!["2024-01-01T00:00:00Z".to_string(), "22.5".to_string(), "65.2".to_string()],
            vec!["2024-01-01T01:00:00Z".to_string(), "23.1".to_string(), "63.8".to_string()],
        ],
    ).unwrap();

    let result = parse_csv_strict(data_grid);
    assert!(result.is_err());
    let error_msg = result.err().unwrap().to_string();
    assert!(error_msg.contains("Strict mode"));
    assert!(error_msg.contains("Expected formats"));
    assert!(error_msg.contains("mode=infer"));
}

#[test]
fn test_parse_strict_invalid_uuid_error() {
    let data_grid = StringDataGrid::new(
        vec!["datetime".to_string(), "sensor_uuid".to_string(), "value".to_string()],
        vec![
            vec!["2024-01-01T00:00:00Z".to_string(), "not-a-uuid".to_string(), "22.5".to_string()],
        ],
    ).unwrap();

    let result = parse_csv_strict(data_grid);
    assert!(result.is_err());
    let error_msg = result.err().unwrap().to_string();
    assert!(error_msg.contains("Invalid UUID format"));
}

#[test]
fn test_is_valid_uuid() {
    let valid_uuid = uuid::Uuid::new_v4().to_string();
    assert!(is_valid_uuid(&valid_uuid));
    assert!(!is_valid_uuid("not-a-uuid"));
    assert!(!is_valid_uuid(""));
}

#[test]
fn test_parse_mode_display() {
    assert_eq!(format!("{:?}", ParseMode::Strict), "Strict");
    assert_eq!(format!("{:?}", ParseMode::Infer), "Infer");
}