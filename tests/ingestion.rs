use anyhow::Result;
use axum::http::StatusCode;
use sensapp::test_utils::db;
use sensapp::test_utils::http::TestApp;
use sensapp::test_utils::{TestDb, fixtures};
use sensapp::test_utils::load_configuration_for_tests;
use serial_test::serial;

// Ensure configuration is loaded once for all tests in this module
static INIT: std::sync::Once = std::sync::Once::new();
fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// Test CSV data ingestion end-to-end
#[tokio::test]
#[serial]
async fn test_csv_ingestion_temperature_sensor() -> Result<()> {
    ensure_config();
    // Given: A test database and HTTP server
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // When: We POST CSV data for a temperature sensor
    let csv_data = fixtures::temperature_sensor_csv();
    let response = app.post_csv("/sensors/publish", &csv_data).await?;

    // Then: Response should be successful
    response.assert_status(StatusCode::OK);

    // And: Data should be stored in the database (check our specific sensor exists)

    // Get the actual sensor name from CSV data (it has unique test ID)
    let csv_lines: Vec<&str> = csv_data.lines().collect();
    let sensor_name = if csv_lines.len() > 1 {
        let parts: Vec<&str> = csv_lines[1].split(',').collect();
        if parts.len() > 1 {
            parts[1].to_string()
        } else {
            return Err(anyhow::anyhow!("Could not parse sensor name from CSV"));
        }
    } else {
        return Err(anyhow::anyhow!("CSV has no data rows"));
    };

    let sensor = db::get_sensor_by_name(&storage, &sensor_name)
        .await?
        .expect("Temperature sensor should exist");

    assert!(sensor.name.starts_with("temperature_"));
    assert_eq!(
        sensor.unit.as_ref().map(|u| &u.name),
        Some(&"°C".to_string())
    );

    // And: All samples should be stored
    let sensor_data = db::verify_sensor_data(&storage, &sensor_name, 5).await?;

    // Verify the first sample
    if let sensapp::datamodel::TypedSamples::Float(samples) = &sensor_data.samples {
        assert_eq!(samples[0].value, 20.5);
        assert_eq!(samples[1].value, 21.0);
        assert_eq!(samples[4].value, 20.8);
    } else {
        panic!("Expected float samples for temperature sensor");
    }

    Ok(())
}

/// Test CSV ingestion with multiple sensors
#[tokio::test]
#[serial]
async fn test_csv_ingestion_multiple_sensors() -> Result<()> {
    ensure_config();
    // Given: A test database and HTTP server
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // When: We POST CSV data with multiple sensors
    let csv_data = fixtures::multi_sensor_csv();
    let response = app.post_csv("/sensors/publish", &csv_data).await?;

    // Then: Response should be successful
    response.assert_status(StatusCode::OK);

    // Extract sensor names from CSV data to check specific sensors exist
    let csv_lines: Vec<&str> = csv_data.lines().collect();
    let mut expected_sensors = std::collections::HashSet::new();
    for line in csv_lines.iter().skip(1) {
        // Skip header
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() > 1 {
            expected_sensors.insert(parts[1].to_string());
        }
    }

    // Verify both expected sensors exist
    for sensor_name in &expected_sensors {
        let sensor = db::get_sensor_by_name(&storage, sensor_name)
            .await?
            .unwrap_or_else(|| panic!("Sensor {} should exist", sensor_name));

        if sensor_name.starts_with("temperature_") {
            assert_eq!(
                sensor.unit.as_ref().map(|u| &u.name),
                Some(&"°C".to_string())
            );
            db::verify_sensor_data(&storage, sensor_name, 3).await?;
        } else if sensor_name.starts_with("humidity_") {
            assert_eq!(
                sensor.unit.as_ref().map(|u| &u.name),
                Some(&"%".to_string())
            );
            db::verify_sensor_data(&storage, sensor_name, 3).await?;
        }
    }

    assert_eq!(
        expected_sensors.len(),
        2,
        "Should have exactly 2 unique sensors"
    );

    Ok(())
}

/// Test JSON data ingestion
#[tokio::test]
#[serial]
async fn test_json_ingestion() -> Result<()> {
    ensure_config();
    // Given: A test database and HTTP server
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // When: We POST JSON data
    let json_data = fixtures::temperature_sensor_json();
    let response = app.post_json("/sensors/publish", &json_data).await?;

    // Then: Response should be successful
    response.assert_status(StatusCode::OK);

    // Extract sensor name from SenML JSON data (bn field) and verify it exists
    let json_value: serde_json::Value = serde_json::from_str(&json_data)?;
    let sensor_name = json_value[0]["bn"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Could not parse sensor name from SenML JSON"))?;

    let sensor = db::get_sensor_by_name(&storage, sensor_name)
        .await?
        .expect("Temperature sensor should exist");

    assert!(sensor.name.starts_with("temperature_"));
    db::verify_sensor_data(&storage, sensor_name, 3).await?;

    Ok(())
}

/// Test sensor data querying after ingestion
#[tokio::test]
#[serial]
async fn test_sensor_query_after_ingestion() -> Result<()> {
    ensure_config();
    // Given: A database with sensor data
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // Ingest some test data first
    let csv_data = fixtures::temperature_sensor_csv();
    app.post_csv("/sensors/publish", &csv_data).await?;

    // When: We query the sensors list
    let response = app.get("/series").await?;

    // Then: Response should be successful and contain our sensor
    response
        .assert_status(StatusCode::OK)
        .assert_body_contains("temperature");

    Ok(())
}

/// Test error handling for malformed CSV
#[tokio::test]
#[serial]
async fn test_malformed_csv_handling() -> Result<()> {
    ensure_config();
    // Given: A test database and HTTP server
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // When: We POST malformed CSV data
    let malformed_csv = "this,is,not,proper\ncsv,data,missing,headers";
    let response = app.post_csv("/sensors/publish", malformed_csv).await?;

    // Then: Response should indicate an error
    assert!(
        !response.is_success(),
        "Expected error response for malformed CSV"
    );

    // And: No new sensors should be created from malformed data
    // (We can't check total count due to shared database, but the test should fail before creating anything)

    Ok(())
}

/// Test large CSV ingestion performance
#[tokio::test]
#[serial]
async fn test_large_csv_ingestion() -> Result<()> {
    ensure_config();
    // Given: A test database and HTTP server
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // When: We POST a large CSV file (simulate 1000 samples)
    let mut large_csv = String::from("datetime,sensor_name,value,unit\n");
    for i in 0..1000 {
        large_csv.push_str(&format!(
            "2024-01-01T{:02}:{:02}:{:02}Z,temperature_bulk,{:.1},°C\n",
            (i / 3600) % 24,
            (i / 60) % 60,
            i % 60,
            20.0 + (i as f64 * 0.01)
        ));
    }

    let response = app.post_csv("/sensors/publish", &large_csv).await?;

    // Then: Response should be successful
    response.assert_status(StatusCode::OK);

    // And: All samples should be stored for our bulk sensor
    let sensor = db::get_sensor_by_name(&storage, "temperature_bulk")
        .await?
        .expect("Bulk temperature sensor should exist");

    assert_eq!(sensor.name, "temperature_bulk");
    assert_eq!(
        sensor.unit.as_ref().map(|u| &u.name),
        Some(&"°C".to_string())
    );
    db::verify_sensor_data(&storage, "temperature_bulk", 1000).await?;

    Ok(())
}
