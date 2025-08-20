mod common;

use anyhow::Result;
use axum::http::StatusCode;
use common::db::DbHelpers;
use common::http::TestApp;
use common::{TestDb, TestHelpers, fixtures};

/// Test CSV data ingestion end-to-end
#[tokio::test]
async fn test_csv_ingestion_temperature_sensor() -> Result<()> {
    // Given: A test database and HTTP server
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // When: We POST CSV data for a temperature sensor
    let csv_data = fixtures::temperature_sensor_csv();
    let response = app.post_csv("/sensors/publish", csv_data).await?;

    // Then: Response should be successful
    response.assert_status(StatusCode::OK);

    // And: Data should be stored in the database
    storage.expect_sensor_count(1).await?;

    let sensor = DbHelpers::get_sensor_by_name(&storage, "temperature")
        .await?
        .expect("Temperature sensor should exist");

    assert_eq!(sensor.name, "temperature");
    assert_eq!(
        sensor.unit.as_ref().map(|u| &u.name),
        Some(&"°C".to_string())
    );

    // And: All samples should be stored
    let sensor_data = DbHelpers::verify_sensor_data(&storage, "temperature", 5).await?;

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
async fn test_csv_ingestion_multiple_sensors() -> Result<()> {
    // Given: A test database and HTTP server
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // When: We POST CSV data with multiple sensors
    let csv_data = fixtures::multi_sensor_csv();
    let response = app.post_csv("/sensors/publish", csv_data).await?;

    // Then: Response should be successful
    response.assert_status(StatusCode::OK);

    // And: Both sensors should be stored
    storage.expect_sensor_count(2).await?;

    let sensor_names = DbHelpers::get_sensor_names(&storage).await?;
    assert!(sensor_names.contains(&"temperature".to_string()));
    assert!(sensor_names.contains(&"humidity".to_string()));

    // And: Each sensor should have the correct number of samples
    DbHelpers::verify_sensor_data(&storage, "temperature", 3).await?;
    DbHelpers::verify_sensor_data(&storage, "humidity", 3).await?;

    // And: Total sample count should be correct
    let total_samples = DbHelpers::count_total_samples(&storage).await?;
    assert_eq!(total_samples, 6);

    Ok(())
}

/// Test JSON data ingestion
#[tokio::test]
async fn test_json_ingestion() -> Result<()> {
    // Given: A test database and HTTP server
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // When: We POST JSON data
    let json_data = fixtures::temperature_sensor_json();
    let response = app.post_json("/sensors/publish", json_data).await?;

    // Then: Response should be successful
    response.assert_status(StatusCode::OK);

    // And: Sensor should be stored with correct samples
    storage.expect_sensor_count(1).await?;
    DbHelpers::verify_sensor_data(&storage, "temperature", 3).await?;

    Ok(())
}

/// Test sensor data querying after ingestion
#[tokio::test]
async fn test_sensor_query_after_ingestion() -> Result<()> {
    // Given: A database with sensor data
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // Ingest some test data first
    let csv_data = fixtures::temperature_sensor_csv();
    app.post_csv("/sensors/publish", csv_data).await?;

    // When: We query the sensors list
    let response = app.get("/sensors").await?;

    // Then: Response should be successful and contain our sensor
    response
        .assert_status(StatusCode::OK)
        .assert_body_contains("temperature");

    Ok(())
}

/// Test error handling for malformed CSV
#[tokio::test]
async fn test_malformed_csv_handling() -> Result<()> {
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

    // And: No sensors should be created
    storage.expect_sensor_count(0).await?;

    Ok(())
}

/// Test large CSV ingestion performance
#[tokio::test]
async fn test_large_csv_ingestion() -> Result<()> {
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

    // And: All samples should be stored
    storage.expect_sensor_count(1).await?;
    DbHelpers::verify_sensor_data(&storage, "temperature_bulk", 1000).await?;

    Ok(())
}
