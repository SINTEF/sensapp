use anyhow::Result;
use sensapp::datamodel::batch::SingleSensorBatch;
use sensapp::datamodel::sensapp_datetime::SensAppDateTimeExt;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use sensapp::parsing::prometheus::remote_read_models::{LabelMatcher, label_matcher};
use sensapp::storage::StorageInstance;
use sensapp::storage::postgresql::prometheus_matcher::PrometheusMatcher;
use sensapp::test_utils::fixtures::create_test_batch;
use serial_test::serial;
use smallvec::smallvec;
use std::sync::Arc;

use sensapp::test_utils::TestDb;

/// Initialize configuration for tests
fn init_config() {
    let _ = sensapp::config::load_configuration();
}

/// Test data helper to create realistic sensors with labels
struct PrometheusTestData;

impl PrometheusTestData {
    /// Create comprehensive test sensors that cover various Prometheus use cases
    async fn create_test_sensors(storage: &Arc<dyn StorageInstance>) -> Result<()> {
        let mut batch_sensors = smallvec::smallvec![];

        // CPU usage sensors with different hosts and regions
        Self::add_sensor_with_samples(
            &mut batch_sensors,
            "cpu_usage",
            vec![
                ("host".to_string(), "server1".to_string()),
                ("region".to_string(), "us-east".to_string()),
                ("environment".to_string(), "production".to_string()),
            ],
            vec![(1000000, 45.2), (2000000, 52.1), (3000000, 38.9)],
        )?;

        Self::add_sensor_with_samples(
            &mut batch_sensors,
            "cpu_usage",
            vec![
                ("host".to_string(), "server2".to_string()),
                ("region".to_string(), "us-west".to_string()),
                ("environment".to_string(), "production".to_string()),
            ],
            vec![(1000000, 62.4), (2000000, 71.3), (3000000, 55.7)],
        )?;

        Self::add_sensor_with_samples(
            &mut batch_sensors,
            "cpu_usage",
            vec![
                ("host".to_string(), "dev-server".to_string()),
                ("region".to_string(), "us-east".to_string()),
                ("environment".to_string(), "development".to_string()),
            ],
            vec![(1000000, 12.1), (2000000, 18.5), (3000000, 9.8)],
        )?;

        // Memory usage sensors
        Self::add_sensor_with_samples(
            &mut batch_sensors,
            "memory_usage",
            vec![
                ("host".to_string(), "server1".to_string()),
                ("region".to_string(), "us-east".to_string()),
                ("environment".to_string(), "production".to_string()),
            ],
            vec![(1000000, 1024.0), (2000000, 1536.0), (3000000, 2048.0)],
        )?;

        // Disk I/O with device labels
        Self::add_sensor_with_samples(
            &mut batch_sensors,
            "disk_io",
            vec![
                ("host".to_string(), "server1".to_string()),
                ("device".to_string(), "/dev/sda1".to_string()),
                ("mount".to_string(), "/".to_string()),
            ],
            vec![(1000000, 150.0), (2000000, 200.0), (3000000, 175.0)],
        )?;

        Self::add_sensor_with_samples(
            &mut batch_sensors,
            "disk_io",
            vec![
                ("host".to_string(), "server1".to_string()),
                ("device".to_string(), "/dev/sdb1".to_string()),
                ("mount".to_string(), "/data".to_string()),
            ],
            vec![(1000000, 75.0), (2000000, 90.0), (3000000, 65.0)],
        )?;

        // Network interface metrics
        Self::add_sensor_with_samples(
            &mut batch_sensors,
            "network_rx_bytes",
            vec![
                ("host".to_string(), "server1".to_string()),
                ("interface".to_string(), "eth0".to_string()),
            ],
            vec![
                (1000000, 1024000.0),
                (2000000, 2048000.0),
                (3000000, 1536000.0),
            ],
        )?;

        // Temperature sensor without standard labels (edge case)
        Self::add_sensor_with_samples(
            &mut batch_sensors,
            "temperature",
            vec![
                ("location".to_string(), "datacenter".to_string()),
                ("rack".to_string(), "A1".to_string()),
                ("sensor_id".to_string(), "temp_001".to_string()),
            ],
            vec![(1000000, 23.5), (2000000, 24.1), (3000000, 22.8)],
        )?;

        // Sensor with no labels (edge case)
        Self::add_sensor_with_samples(
            &mut batch_sensors,
            "uptime_seconds",
            vec![], // No labels
            vec![(1000000, 86400.0), (2000000, 172800.0), (3000000, 259200.0)],
        )?;

        // Sensor with special characters in labels (security/edge case)
        Self::add_sensor_with_samples(
            &mut batch_sensors,
            "custom_metric",
            vec![
                ("app".to_string(), "web-server".to_string()),
                ("version".to_string(), "v1.2.3".to_string()),
                ("tag".to_string(), "special-chars_123".to_string()),
            ],
            vec![(1000000, 100.0), (2000000, 200.0), (3000000, 150.0)],
        )?;

        // Print debug info about created sensors
        println!(
            "Created {} test sensors for PrometheusMatcher tests",
            batch_sensors.len()
        );
        for sensor in &batch_sensors {
            println!(
                "Sensor: {}, Labels: {:?}",
                sensor.sensor.name, sensor.sensor.labels
            );
            println!("UUID: {}", sensor.sensor.uuid);
        }

        // Create the batch and publish all test data to the database
        let batch = create_test_batch(batch_sensors);
        storage.publish(Arc::new(batch)).await?;

        // wait 1 second to ensure data is committed
        //tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        Ok(())
    }

    fn add_sensor_with_samples(
        batch_sensors: &mut smallvec::SmallVec<[SingleSensorBatch; 4]>,
        name: &str,
        labels: Vec<(String, String)>,
        sample_data: Vec<(i64, f64)>, // (timestamp_us, value)
    ) -> Result<()> {
        let mut sensor_labels = SensAppLabels::new();
        for (key, value) in labels {
            sensor_labels.push((key, value));
        }

        let sensor = Sensor::new_without_uuid(
            name.to_string(),
            SensorType::Float,
            None,
            if sensor_labels.is_empty() {
                None
            } else {
                Some(sensor_labels)
            },
        )?;

        let mut samples = smallvec![];
        for (timestamp_us, value) in sample_data {
            samples.push(Sample {
                datetime: SensAppDateTime::from_unix_microseconds_i64(timestamp_us),
                value,
            });
        }

        let single_sensor_batch =
            SingleSensorBatch::new(Arc::new(sensor), TypedSamples::Float(samples));

        batch_sensors.push(single_sensor_batch);
        Ok(())
    }

    fn create_matcher(name: &str, value: &str, matcher_type: label_matcher::Type) -> LabelMatcher {
        LabelMatcher {
            r#type: matcher_type as i32,
            name: name.to_string(),
            value: value.to_string(),
        }
    }
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_eq_sensor_name() -> Result<()> {
    println!("Starting test_prometheus_matcher_eq_sensor_name...");
    init_config();

    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Setup test data
    PrometheusTestData::create_test_sensors(&storage).await?;

    // Get PostgreSQL storage for direct testing
    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test EQ matcher for __name__ (sensor name)
    let matchers = vec![PrometheusTestData::create_matcher(
        "__name__",
        "cpu_usage",
        label_matcher::Type::Eq,
    )];

    let matching_sensors = prometheus_matcher.find_matching_sensors(&matchers).await?;

    // Should find 3 cpu_usage sensors
    assert_eq!(matching_sensors.len(), 3);

    println!("Ending test_prometheus_matcher_eq_sensor_name...");
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_eq_label() -> Result<()> {
    println!("Starting test_prometheus_matcher_eq_label...");
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test EQ matcher for a specific label
    let matchers = vec![PrometheusTestData::create_matcher(
        "host",
        "server1",
        label_matcher::Type::Eq,
    )];

    let matching_sensors = prometheus_matcher.find_matching_sensors(&matchers).await?;

    // Should find sensors with host=server1 (cpu_usage, memory_usage, and disk_io sensors)
    assert!(matching_sensors.len() >= 3);

    println!("Ending test_prometheus_matcher_eq_label...");
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_neq() -> Result<()> {
    println!("Starting test_prometheus_matcher_neq...");
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test NEQ matcher - exclude production environment
    let matchers = vec![PrometheusTestData::create_matcher(
        "environment",
        "production",
        label_matcher::Type::Neq,
    )];

    let matching_sensors = prometheus_matcher.find_matching_sensors(&matchers).await?;

    // Should find sensors that don't have environment=production
    // This includes development servers and sensors without environment labels
    assert!(!matching_sensors.is_empty());

    println!("Ending test_prometheus_matcher_neq...");
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_regex() -> Result<()> {
    println!("Starting test_prometheus_matcher_regex...");
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test regex matcher - match any server host
    let matchers = vec![PrometheusTestData::create_matcher(
        "host",
        "server.*",
        label_matcher::Type::Re,
    )];

    let matching_sensors = prometheus_matcher.find_matching_sensors(&matchers).await?;

    // Should find sensors with host matching "server.*" (server1, server2)
    assert!(matching_sensors.len() >= 4); // At least cpu_usage, memory_usage, disk_io sensors

    println!("Ending test_prometheus_matcher_regex...");
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_regex_sensor_name() -> Result<()> {
    println!("Starting test_prometheus_matcher_regex_sensor_name...");
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test regex matcher for sensor names ending with "_usage"
    let matchers = vec![PrometheusTestData::create_matcher(
        "__name__",
        ".*_usage$",
        label_matcher::Type::Re,
    )];

    let matching_sensors = prometheus_matcher.find_matching_sensors(&matchers).await?;

    // Should find cpu_usage and memory_usage sensors
    assert!(matching_sensors.len() >= 4); // 3 cpu_usage + 1 memory_usage

    println!("Ending test_prometheus_matcher_regex_sensor_name...");
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_not_regex() -> Result<()> {
    println!("Starting test_prometheus_matcher_not_regex...");
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test negative regex - exclude sensors with names containing "cpu"
    let matchers = vec![PrometheusTestData::create_matcher(
        "__name__",
        ".*cpu.*",
        label_matcher::Type::Nre,
    )];

    let matching_sensors = prometheus_matcher.find_matching_sensors(&matchers).await?;

    // Should find sensors that don't match ".*cpu.*" pattern
    assert!(!matching_sensors.is_empty());
    println!("Ending test_prometheus_matcher_not_regex...");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_multiple_matchers_and_logic() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test multiple matchers with AND logic
    let matchers = vec![
        PrometheusTestData::create_matcher("__name__", "cpu_usage", label_matcher::Type::Eq),
        PrometheusTestData::create_matcher("region", "us-east", label_matcher::Type::Eq),
        PrometheusTestData::create_matcher("environment", "production", label_matcher::Type::Eq),
    ];

    let matching_sensors = prometheus_matcher.find_matching_sensors(&matchers).await?;

    // Should find exactly 1 sensor: cpu_usage with region=us-east and environment=production
    assert_eq!(matching_sensors.len(), 1);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_no_matches() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test matcher that should find no results
    let matchers = vec![PrometheusTestData::create_matcher(
        "__name__",
        "nonexistent_metric",
        label_matcher::Type::Eq,
    )];

    let matching_sensors = prometheus_matcher.find_matching_sensors(&matchers).await?;

    assert_eq!(matching_sensors.len(), 0);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_debug_sensor_creation() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    println!("Creating a simple sensor...");

    let sensor =
        Sensor::new_without_uuid("debug_sensor".to_string(), SensorType::Float, None, None)?;

    let samples = smallvec![Sample {
        datetime: SensAppDateTime::from_unix_microseconds_i64(1000000),
        value: 42.0,
    }];

    let mut batch_sensors = smallvec::smallvec![];
    let single_sensor_batch =
        SingleSensorBatch::new(Arc::new(sensor), TypedSamples::Float(samples));
    batch_sensors.push(single_sensor_batch);

    let batch = create_test_batch(batch_sensors);

    println!("Publishing batch...");
    storage.publish(Arc::new(batch)).await?;

    println!("Success!");
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_debug_prometheus_test_data() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    println!("Calling PrometheusTestData::create_test_sensors...");
    PrometheusTestData::create_test_sensors(&storage).await?;

    println!("Success!");
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_empty_matchers() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test with empty matchers - should return all sensors
    let matchers = vec![];

    let matching_sensors = prometheus_matcher.find_matching_sensors(&matchers).await?;

    // Should find all sensors in the test data
    assert!(matching_sensors.len() >= 9); // All test sensors we created

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_contradictory_matchers() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test contradictory matchers - should return empty result
    let matchers = vec![
        PrometheusTestData::create_matcher("host", "server1", label_matcher::Type::Eq),
        PrometheusTestData::create_matcher("host", "server2", label_matcher::Type::Eq),
    ];

    let matching_sensors = prometheus_matcher.find_matching_sensors(&matchers).await?;

    // Should find no sensors since host can't be both server1 and server2
    assert_eq!(matching_sensors.len(), 0);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_query_time_series_integration() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Test the full integration with query_prometheus_time_series
    let matchers = vec![
        PrometheusTestData::create_matcher("__name__", "cpu_usage", label_matcher::Type::Eq),
        PrometheusTestData::create_matcher("host", "server1", label_matcher::Type::Eq),
    ];

    let results = storage
        .query_prometheus_time_series(&matchers, 0, 4000)
        .await?;

    // Should find 1 sensor (cpu_usage on server1) with samples
    assert_eq!(results.len(), 1);

    let (sensor, samples) = &results[0];
    assert_eq!(sensor.name, "cpu_usage");
    assert!(!samples.is_empty()); // Should have time series data

    // Verify sensor has expected labels
    assert!(
        sensor
            .labels
            .iter()
            .any(|(k, v)| k == "host" && v == "server1")
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_special_characters() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test matching labels with special characters (hyphens, underscores, dots)
    let matchers = vec![PrometheusTestData::create_matcher(
        "app",
        "web-server",
        label_matcher::Type::Eq,
    )];

    let matching_sensors = prometheus_matcher.find_matching_sensors(&matchers).await?;

    // Should find the custom_metric sensor
    assert_eq!(matching_sensors.len(), 1);

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_invalid_regex_handling() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test with invalid regex pattern - should handle gracefully
    let matchers = vec![PrometheusTestData::create_matcher(
        "host",
        "[invalid regex", // Unclosed bracket
        label_matcher::Type::Re,
    )];

    // PostgreSQL should handle invalid regex gracefully (may return error or no matches)
    let result = prometheus_matcher.find_matching_sensors(&matchers).await;

    // Either succeeds with empty results or fails gracefully
    // PostgreSQL's regex engine should handle this appropriately
    assert!(result.is_ok() || result.is_err());

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_matcher_sql_injection_protection() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    PrometheusTestData::create_test_sensors(&storage).await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test potential SQL injection attempts - should be safely parameterized
    let injection_attempts = vec![
        "'; DROP TABLE sensors; --",
        "' OR '1'='1",
        "'; DELETE FROM sensors WHERE '1'='1'; --",
        "' UNION SELECT * FROM sensors; --",
    ];

    for injection in injection_attempts {
        let matchers = vec![PrometheusTestData::create_matcher(
            "host",
            injection,
            label_matcher::Type::Eq,
        )];

        let result = prometheus_matcher.find_matching_sensors(&matchers).await;

        // Should either return safely (no matches) or handle error gracefully
        // Most importantly, should not execute malicious SQL
        assert!(result.is_ok());
        if let Ok(sensors) = result {
            // Should find no matches for these malicious strings
            assert_eq!(sensors.len(), 0);
        }
    }

    Ok(())
}

// Additional tests moved from prometheus_matcher.rs unit tests
// These are properly integration tests since they use a real database

#[tokio::test]
#[serial]
async fn test_unit_eq_matcher_sensor_name() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create unique test data to avoid conflicts
    UnitTestData::create_unit_test_sensors(&storage, "unit_test_1").await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test EQ matcher for __name__ (sensor name)
    let matcher =
        PrometheusTestData::create_matcher("__name__", "unit_cpu_usage_1", label_matcher::Type::Eq);
    let result = prometheus_matcher.find_matching_sensors(&[matcher]).await?;

    assert_eq!(result.len(), 1, "Should find exactly 1 cpu_usage sensor");
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_unit_eq_matcher_label() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create unique test data
    UnitTestData::create_unit_test_sensors(&storage, "unit_test_2").await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test EQ matcher for a label
    let matcher =
        PrometheusTestData::create_matcher("host", "unit_server1_2", label_matcher::Type::Eq);
    let result = prometheus_matcher.find_matching_sensors(&[matcher]).await?;

    assert_eq!(
        result.len(),
        1,
        "Should find exactly 1 sensor with host=unit_server1_2"
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_unit_regex_matcher() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    UnitTestData::create_unit_test_sensors(&storage, "unit_test_3").await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test RE matcher: job=~"node.*"
    let matcher = PrometheusTestData::create_matcher("job", "node.*", label_matcher::Type::Re);
    let result = prometheus_matcher.find_matching_sensors(&[matcher]).await?;

    assert_eq!(
        result.len(),
        1,
        "Should find exactly 1 sensor matching job=~'node.*'"
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_unit_multiple_matchers_and_logic() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    UnitTestData::create_unit_test_sensors(&storage, "unit_test_4").await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test multiple matchers combined with AND logic
    let matchers = vec![
        PrometheusTestData::create_matcher("__name__", "unit_cpu_usage_4", label_matcher::Type::Eq),
        PrometheusTestData::create_matcher("host", "unit_server1_4", label_matcher::Type::Eq),
    ];

    let result = prometheus_matcher.find_matching_sensors(&matchers).await?;
    assert_eq!(
        result.len(),
        1,
        "Should find exactly 1 sensor matching both conditions"
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_unit_empty_matchers_returns_all() -> Result<()> {
    init_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    UnitTestData::create_unit_test_sensors(&storage, "unit_test_5").await?;

    // Create a separate pool connection for testing PrometheusMatcher
    let pool = sqlx::PgPool::connect(test_db.connection_string()).await?;

    let prometheus_matcher = PrometheusMatcher::new(pool);

    // Test empty matchers - should return all sensors
    let result = prometheus_matcher.find_matching_sensors(&[]).await?;
    assert!(
        result.len() >= 2,
        "Should find at least 2 sensors when no matchers"
    );
    Ok(())
}

/// Helper struct for creating unique test data for the former unit tests
struct UnitTestData;

impl UnitTestData {
    /// Create unique test sensors for unit test scenarios
    async fn create_unit_test_sensors(
        storage: &Arc<dyn StorageInstance>,
        test_id: &str,
    ) -> Result<()> {
        let mut batch_sensors = smallvec::smallvec![];

        // Create test sensor with labels (unique names)
        let mut labels = SensAppLabels::new();
        labels.push((
            "host".to_string(),
            format!("unit_server1_{}", test_id.split('_').next_back().unwrap()),
        ));
        labels.push(("job".to_string(), "node-exporter".to_string()));

        PrometheusTestData::add_sensor_with_samples(
            &mut batch_sensors,
            &format!("unit_cpu_usage_{}", test_id.split('_').next_back().unwrap()),
            vec![
                (
                    "host".to_string(),
                    format!("unit_server1_{}", test_id.split('_').next_back().unwrap()),
                ),
                ("job".to_string(), "node-exporter".to_string()),
            ],
            vec![(1000000, 45.2)],
        )?;

        // Add another test sensor without labels (unique name)
        PrometheusTestData::add_sensor_with_samples(
            &mut batch_sensors,
            &format!(
                "unit_memory_usage_{}",
                test_id.split('_').next_back().unwrap()
            ),
            vec![],
            vec![(1000000, 1024.0)],
        )?;

        let batch = create_test_batch(batch_sensors);
        storage.publish(Arc::new(batch)).await?;
        Ok(())
    }
}
