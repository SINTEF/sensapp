mod common;

use anyhow::Result;
use axum::http::StatusCode;
use common::TestDb;
use common::db::DbHelpers;
use common::http::TestApp;
use sensapp::config::load_configuration_for_tests;
use serial_test::serial;

static INIT: std::sync::Once = std::sync::Once::new();
fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// Test basic InfluxDB line protocol ingestion
#[tokio::test]
#[serial]
async fn test_influxdb_basic_write() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    let influx_data = "cpu,host=serverA,region=us-west usage_idle=99.1 1704067200000000000\n\
                       cpu,host=serverA,region=us-west usage_idle=98.5 1704067260000000000\n\
                       cpu,host=serverA,region=us-west usage_idle=97.8 1704067320000000000";

    let response = app
        .post_influxdb("/api/v2/write?bucket=test&org=sensapp", influx_data)
        .await?;

    response.assert_status(StatusCode::NO_CONTENT);

    // Verify data was stored — InfluxDB creates sensor names as "measurement field_key"
    let sensor_name = "cpu usage_idle";
    let sensor = DbHelpers::get_sensor_by_name(&storage, sensor_name).await?;
    assert!(sensor.is_some(), "Sensor '{}' should exist", sensor_name);

    let sensor_data = DbHelpers::verify_sensor_data(&storage, sensor_name, 3).await?;

    // InfluxDB float values should be stored as Float type
    if let sensapp::datamodel::TypedSamples::Float(samples) = &sensor_data.samples {
        assert_eq!(samples[0].value, 99.1);
        assert_eq!(samples[1].value, 98.5);
        assert_eq!(samples[2].value, 97.8);
    } else {
        panic!("Expected float samples for cpu sensor");
    }

    Ok(())
}

/// Test InfluxDB write with multiple measurements
#[tokio::test]
#[serial]
async fn test_influxdb_multiple_measurements() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    let influx_data = "temperature,room=kitchen value=22.5 1704067200000000000\n\
                       humidity,room=kitchen value=65.0 1704067200000000000\n\
                       temperature,room=kitchen value=23.0 1704067260000000000\n\
                       humidity,room=kitchen value=64.0 1704067260000000000";

    let response = app
        .post_influxdb("/api/v2/write?bucket=test&org=sensapp", influx_data)
        .await?;

    response.assert_status(StatusCode::NO_CONTENT);

    // Both sensors should exist
    let temp_sensor = DbHelpers::get_sensor_by_name(&storage, "temperature value").await?;
    assert!(temp_sensor.is_some(), "Temperature sensor should exist");

    let humidity_sensor = DbHelpers::get_sensor_by_name(&storage, "humidity value").await?;
    assert!(humidity_sensor.is_some(), "Humidity sensor should exist");

    // Verify sample counts
    DbHelpers::verify_sensor_data(&storage, "temperature value", 2).await?;
    DbHelpers::verify_sensor_data(&storage, "humidity value", 2).await?;

    Ok(())
}

/// Test InfluxDB write with multiple fields per measurement
#[tokio::test]
#[serial]
async fn test_influxdb_multiple_fields() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // InfluxDB supports multiple fields per line — each becomes a separate sensor
    let influx_data =
        "weather,city=oslo temperature=5.0,humidity=80.0,wind_speed=12.3 1704067200000000000";

    let response = app
        .post_influxdb("/api/v2/write?bucket=test&org=sensapp", influx_data)
        .await?;

    response.assert_status(StatusCode::NO_CONTENT);

    // Each field creates a separate sensor named "measurement field_key"
    DbHelpers::verify_sensor_data(&storage, "weather temperature", 1).await?;
    DbHelpers::verify_sensor_data(&storage, "weather humidity", 1).await?;
    DbHelpers::verify_sensor_data(&storage, "weather wind_speed", 1).await?;

    Ok(())
}

/// Test InfluxDB write with integer fields
#[tokio::test]
#[serial]
async fn test_influxdb_integer_fields() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // InfluxDB integer values end with 'i'
    let influx_data = "counter,host=web1 requests=42i 1704067200000000000\n\
                       counter,host=web1 requests=100i 1704067260000000000";

    let response = app
        .post_influxdb("/api/v2/write?bucket=test&org=sensapp", influx_data)
        .await?;

    response.assert_status(StatusCode::NO_CONTENT);

    let sensor_data = DbHelpers::verify_sensor_data(&storage, "counter requests", 2).await?;

    if let sensapp::datamodel::TypedSamples::Integer(samples) = &sensor_data.samples {
        assert_eq!(samples[0].value, 42);
        assert_eq!(samples[1].value, 100);
    } else {
        panic!("Expected integer samples for counter sensor");
    }

    Ok(())
}

/// Test InfluxDB write with boolean fields
#[tokio::test]
#[serial]
async fn test_influxdb_boolean_fields() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    let influx_data = "system,host=web1 online=true 1704067200000000000\n\
                       system,host=web1 online=false 1704067260000000000";

    let response = app
        .post_influxdb("/api/v2/write?bucket=test&org=sensapp", influx_data)
        .await?;

    response.assert_status(StatusCode::NO_CONTENT);

    let sensor_data = DbHelpers::verify_sensor_data(&storage, "system online", 2).await?;

    if let sensapp::datamodel::TypedSamples::Boolean(samples) = &sensor_data.samples {
        assert!(samples[0].value);
        assert!(!samples[1].value);
    } else {
        panic!("Expected boolean samples for system online sensor");
    }

    Ok(())
}

/// Test InfluxDB write requires org or org_id
#[tokio::test]
#[serial]
async fn test_influxdb_requires_org() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    let influx_data = "cpu,host=serverA usage_idle=99.1 1704067200000000000";

    // Missing org and org_id should return 400
    let response = app
        .post_influxdb("/api/v2/write?bucket=test", influx_data)
        .await?;

    response.assert_status(StatusCode::BAD_REQUEST);

    Ok(())
}

/// Test InfluxDB write with different precision values
#[tokio::test]
#[serial]
async fn test_influxdb_precision_seconds() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // Timestamp in seconds precision
    let influx_data = "temp,room=lab value=25.0 1704067200";

    let response = app
        .post_influxdb(
            "/api/v2/write?bucket=test&org=sensapp&precision=s",
            influx_data,
        )
        .await?;

    response.assert_status(StatusCode::NO_CONTENT);

    let sensor_data = DbHelpers::verify_sensor_data(&storage, "temp value", 1).await?;

    if let sensapp::datamodel::TypedSamples::Float(samples) = &sensor_data.samples {
        assert_eq!(samples[0].value, 25.0);
    } else {
        panic!("Expected float samples for temp sensor");
    }

    Ok(())
}

/// Test InfluxDB write with tags stored as labels
#[tokio::test]
#[serial]
async fn test_influxdb_tags_become_labels() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    let influx_data = "cpu,host=serverA,region=eu-west usage_idle=95.0 1704067200000000000";

    let response = app
        .post_influxdb("/api/v2/write?bucket=mybucket&org=myorg", influx_data)
        .await?;

    response.assert_status(StatusCode::NO_CONTENT);

    let sensor = DbHelpers::get_sensor_by_name(&storage, "cpu usage_idle")
        .await?
        .expect("Sensor should exist");

    // Tags should be stored as labels
    let label_names: Vec<&str> = sensor.labels.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        label_names.contains(&"host"),
        "Should have 'host' label, got: {:?}",
        label_names
    );
    assert!(
        label_names.contains(&"region"),
        "Should have 'region' label, got: {:?}",
        label_names
    );
    // InfluxDB adapter also adds bucket and org as labels
    assert!(
        label_names.contains(&"influxdb_bucket"),
        "Should have 'influxdb_bucket' label, got: {:?}",
        label_names
    );
    assert!(
        label_names.contains(&"influxdb_org"),
        "Should have 'influxdb_org' label, got: {:?}",
        label_names
    );

    Ok(())
}

/// Test InfluxDB write with empty body
#[tokio::test]
#[serial]
async fn test_influxdb_empty_body() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    let response = app
        .post_influxdb("/api/v2/write?bucket=test&org=sensapp", "")
        .await?;

    // Empty body should succeed (no data to ingest)
    response.assert_status(StatusCode::NO_CONTENT);

    Ok(())
}

/// Test InfluxDB write with gzip compression
#[tokio::test]
#[serial]
async fn test_influxdb_gzip_compression() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    let influx_data = "pressure,sensor=bme280 value=1013.25 1704067200000000000";

    // Compress the data with gzip
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(influx_data.as_bytes())?;
    let compressed = encoder.finish()?;

    // Send with content-encoding: gzip
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/api/v2/write?bucket=test&org=sensapp")
        .header("content-type", "text/plain")
        .header("content-encoding", "gzip")
        .body(axum::body::Body::from(compressed))?;

    let response = app.raw_request(request).await?;

    response.assert_status(StatusCode::NO_CONTENT);

    DbHelpers::verify_sensor_data(&storage, "pressure value", 1).await?;

    Ok(())
}
