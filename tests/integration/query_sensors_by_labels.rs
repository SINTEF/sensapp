//! Integration tests for query_sensors_by_labels functionality.
//!
//! These tests verify that the PostgreSQL implementation of `query_sensors_by_labels`
//! correctly handles various label matching scenarios including:
//! - Exact match (`=`)
//! - Not equal (`!=`)
//! - Regex match (`=~`)
//! - Regex not match (`!~`)
//! - Multiple matchers combined with AND logic
//! - The special `__name__` label for sensor name matching

use crate::common;

use anyhow::Result;
use common::TestDb;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use sensapp::storage::query::LabelMatcher;
use serial_test::serial;
use std::sync::Arc;
use uuid::Uuid;

// Ensure configuration is loaded once for all tests in this module
static INIT: std::sync::Once = std::sync::Once::new();
fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// Helper to create a test sensor with labels
fn create_sensor_with_labels(
    name: &str,
    sensor_type: SensorType,
    labels: Vec<(String, String)>,
) -> Sensor {
    let labels: SensAppLabels = labels.into_iter().collect();
    Sensor::new(
        Uuid::new_v4(),
        name.to_string(),
        sensor_type,
        None,
        Some(labels),
    )
}

/// Helper to create float samples
fn create_float_samples(count: usize) -> TypedSamples {
    let samples: smallvec::SmallVec<[Sample<f64>; 4]> = (0..count)
        .map(|i| Sample {
            datetime: hifitime::Epoch::from_unix_seconds((1704067200 + i * 60) as f64),
            value: 20.0 + i as f64,
        })
        .collect();
    TypedSamples::Float(samples)
}

/// Helper to publish sensors with samples to the storage
async fn publish_test_sensors(
    storage: &Arc<dyn sensapp::storage::StorageInstance>,
    sensors_with_samples: Vec<(Sensor, TypedSamples)>,
) -> Result<()> {
    let mut batch_builder = BatchBuilder::new()?;

    for (sensor, samples) in sensors_with_samples {
        let sensor_arc = Arc::new(sensor);
        batch_builder.add(sensor_arc.clone(), samples).await?;
    }

    batch_builder.send_what_is_left(storage.clone()).await?;

    Ok(())
}

// ============================================================================
// Basic Name Matcher Tests (__name__)
// ============================================================================

/// Test querying sensors by exact name match using __name__
#[tokio::test]
#[serial]
async fn test_query_by_exact_name() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors
    let sensor1 = create_sensor_with_labels("cpu_usage", SensorType::Float, vec![]);
    let sensor2 = create_sensor_with_labels("memory_usage", SensorType::Float, vec![]);

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
        ],
    )
    .await?;

    // Query by exact name
    let matchers = vec![LabelMatcher::eq("__name__", "cpu_usage")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 1, "Should find exactly one sensor");
    assert_eq!(results[0].sensor.name, "cpu_usage");

    Ok(())
}

/// Matcher text must stay in bound values even when it contains SQL syntax.
#[tokio::test]
#[serial]
async fn test_query_matcher_sql_text_is_literal() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    let quoted = create_sensor_with_labels(
        "quoted_sensor",
        SensorType::Float,
        vec![("site".into(), "O'Reilly".into())],
    );
    let other = create_sensor_with_labels(
        "other_sensor",
        SensorType::Float,
        vec![("site".into(), "other".into())],
    );
    publish_test_sensors(
        &storage,
        vec![
            (quoted, create_float_samples(1)),
            (other, create_float_samples(1)),
        ],
    )
    .await?;

    for matcher in [
        LabelMatcher::eq("__name__", "' OR 1=1 --"),
        LabelMatcher::eq("site", "' OR 1=1 --"),
        LabelMatcher::eq("site' OR 1=1 --", "O'Reilly"),
    ] {
        let results = storage
            .query_sensors_by_labels(&[matcher], None, None, None, false)
            .await?;
        assert!(
            results.is_empty(),
            "SQL-looking matcher text must not widen results"
        );
    }

    let results = storage
        .query_sensors_by_labels(
            &[LabelMatcher::eq("site", "O'Reilly")],
            None,
            None,
            None,
            false,
        )
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].sensor.name, "quoted_sensor");

    Ok(())
}

/// Test querying sensors by name with not-equal matcher
#[tokio::test]
#[serial]
async fn test_query_by_name_not_equal() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors
    let sensor1 = create_sensor_with_labels("cpu_usage", SensorType::Float, vec![]);
    let sensor2 = create_sensor_with_labels("memory_usage", SensorType::Float, vec![]);
    let sensor3 = create_sensor_with_labels("disk_usage", SensorType::Float, vec![]);

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
            (sensor3, create_float_samples(3)),
        ],
    )
    .await?;

    // Query by name != cpu_usage
    let matchers = vec![LabelMatcher::neq("__name__", "cpu_usage")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 2, "Should find two sensors");
    let names: Vec<&str> = results.iter().map(|r| r.sensor.name.as_str()).collect();
    assert!(names.contains(&"memory_usage"));
    assert!(names.contains(&"disk_usage"));
    assert!(!names.contains(&"cpu_usage"));

    Ok(())
}

/// Test querying sensors by name with regex match
#[tokio::test]
#[serial]
async fn test_query_by_name_regex() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors
    let sensor1 = create_sensor_with_labels("cpu_usage", SensorType::Float, vec![]);
    let sensor2 = create_sensor_with_labels("cpu_temperature", SensorType::Float, vec![]);
    let sensor3 = create_sensor_with_labels("memory_usage", SensorType::Float, vec![]);

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
            (sensor3, create_float_samples(3)),
        ],
    )
    .await?;

    // Query by name matching regex "cpu.*"
    let matchers = vec![LabelMatcher::regex("__name__", "cpu.*")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 2, "Should find two cpu sensors");
    let names: Vec<&str> = results.iter().map(|r| r.sensor.name.as_str()).collect();
    assert!(names.contains(&"cpu_usage"));
    assert!(names.contains(&"cpu_temperature"));

    Ok(())
}

/// Test querying sensors by name with regex not match
#[tokio::test]
#[serial]
async fn test_query_by_name_regex_not_match() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors
    let sensor1 = create_sensor_with_labels("cpu_usage", SensorType::Float, vec![]);
    let sensor2 = create_sensor_with_labels("cpu_temperature", SensorType::Float, vec![]);
    let sensor3 = create_sensor_with_labels("memory_usage", SensorType::Float, vec![]);

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
            (sensor3, create_float_samples(3)),
        ],
    )
    .await?;

    // Query by name NOT matching regex "cpu.*"
    let matchers = vec![LabelMatcher::not_regex("__name__", "cpu.*")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 1, "Should find one non-cpu sensor");
    assert_eq!(results[0].sensor.name, "memory_usage");

    Ok(())
}

// ============================================================================
// Label Matcher Tests
// ============================================================================

/// Test querying sensors by exact label match
#[tokio::test]
#[serial]
async fn test_query_by_label_exact() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors with labels
    let sensor1 = create_sensor_with_labels(
        "cpu_usage",
        SensorType::Float,
        vec![("environment".to_string(), "production".to_string())],
    );
    let sensor2 = create_sensor_with_labels(
        "cpu_usage_staging",
        SensorType::Float,
        vec![("environment".to_string(), "staging".to_string())],
    );
    let sensor3 = create_sensor_with_labels(
        "memory_usage",
        SensorType::Float,
        vec![("environment".to_string(), "production".to_string())],
    );

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
            (sensor3, create_float_samples(3)),
        ],
    )
    .await?;

    // Query by label environment=production
    let matchers = vec![LabelMatcher::eq("environment", "production")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 2, "Should find two production sensors");
    for result in &results {
        let labels: Vec<_> = result.sensor.labels.iter().collect();
        let env_label = labels.iter().find(|(k, _)| k == "environment");
        assert!(env_label.is_some());
        assert_eq!(env_label.unwrap().1, "production");
    }

    Ok(())
}

/// Test querying sensors by label not-equal
#[tokio::test]
#[serial]
async fn test_query_by_label_not_equal() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors with labels
    let sensor1 = create_sensor_with_labels(
        "cpu_prod",
        SensorType::Float,
        vec![("environment".to_string(), "production".to_string())],
    );
    let sensor2 = create_sensor_with_labels(
        "cpu_staging",
        SensorType::Float,
        vec![("environment".to_string(), "staging".to_string())],
    );
    let sensor3 = create_sensor_with_labels(
        "memory_test",
        SensorType::Float,
        vec![("environment".to_string(), "test".to_string())],
    );

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
            (sensor3, create_float_samples(3)),
        ],
    )
    .await?;

    // Query by label environment != production
    let matchers = vec![LabelMatcher::neq("environment", "production")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 2, "Should find two non-production sensors");
    let names: Vec<&str> = results.iter().map(|r| r.sensor.name.as_str()).collect();
    assert!(names.contains(&"cpu_staging"));
    assert!(names.contains(&"memory_test"));

    Ok(())
}

/// Test querying sensors by label regex match
#[tokio::test]
#[serial]
async fn test_query_by_label_regex() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors with labels
    let sensor1 = create_sensor_with_labels(
        "server_eu1",
        SensorType::Float,
        vec![("region".to_string(), "eu-west-1".to_string())],
    );
    let sensor2 = create_sensor_with_labels(
        "server_eu2",
        SensorType::Float,
        vec![("region".to_string(), "eu-central-1".to_string())],
    );
    let sensor3 = create_sensor_with_labels(
        "server_us",
        SensorType::Float,
        vec![("region".to_string(), "us-east-1".to_string())],
    );

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
            (sensor3, create_float_samples(3)),
        ],
    )
    .await?;

    // Query by label region matching regex "eu-.*"
    let matchers = vec![LabelMatcher::regex("region", "eu-.*")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 2, "Should find two EU servers");
    let names: Vec<&str> = results.iter().map(|r| r.sensor.name.as_str()).collect();
    assert!(names.contains(&"server_eu1"));
    assert!(names.contains(&"server_eu2"));

    Ok(())
}

/// Test querying sensors by label regex not match
#[tokio::test]
#[serial]
async fn test_query_by_label_regex_not_match() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors with labels
    let sensor1 = create_sensor_with_labels(
        "server_eu",
        SensorType::Float,
        vec![("region".to_string(), "eu-west-1".to_string())],
    );
    let sensor2 = create_sensor_with_labels(
        "server_us",
        SensorType::Float,
        vec![("region".to_string(), "us-east-1".to_string())],
    );

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
        ],
    )
    .await?;

    // Query by label region NOT matching regex "eu-.*"
    let matchers = vec![LabelMatcher::not_regex("region", "eu-.*")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 1, "Should find one non-EU server");
    assert_eq!(results[0].sensor.name, "server_us");

    Ok(())
}

// ============================================================================
// Combined Matcher Tests (Name + Labels)
// ============================================================================

/// Test querying with both name and label matchers
#[tokio::test]
#[serial]
async fn test_query_combined_name_and_label() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors
    let sensor1 = create_sensor_with_labels(
        "cpu_usage",
        SensorType::Float,
        vec![("environment".to_string(), "production".to_string())],
    );
    let sensor2 = create_sensor_with_labels(
        "cpu_usage",
        SensorType::Float,
        vec![("environment".to_string(), "staging".to_string())],
    );
    let sensor3 = create_sensor_with_labels(
        "memory_usage",
        SensorType::Float,
        vec![("environment".to_string(), "production".to_string())],
    );

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
            (sensor3, create_float_samples(3)),
        ],
    )
    .await?;

    // Query by name=cpu_usage AND environment=production
    let matchers = vec![
        LabelMatcher::eq("__name__", "cpu_usage"),
        LabelMatcher::eq("environment", "production"),
    ];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 1, "Should find exactly one sensor");
    assert_eq!(results[0].sensor.name, "cpu_usage");

    Ok(())
}

/// Test querying with multiple label matchers
#[tokio::test]
#[serial]
async fn test_query_multiple_labels() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors with multiple labels
    let sensor1 = create_sensor_with_labels(
        "app_metrics",
        SensorType::Float,
        vec![
            ("environment".to_string(), "production".to_string()),
            ("service".to_string(), "api".to_string()),
        ],
    );
    let sensor2 = create_sensor_with_labels(
        "app_metrics_web",
        SensorType::Float,
        vec![
            ("environment".to_string(), "production".to_string()),
            ("service".to_string(), "web".to_string()),
        ],
    );
    let sensor3 = create_sensor_with_labels(
        "app_metrics_staging",
        SensorType::Float,
        vec![
            ("environment".to_string(), "staging".to_string()),
            ("service".to_string(), "api".to_string()),
        ],
    );

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
            (sensor3, create_float_samples(3)),
        ],
    )
    .await?;

    // Query by environment=production AND service=api
    let matchers = vec![
        LabelMatcher::eq("environment", "production"),
        LabelMatcher::eq("service", "api"),
    ];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 1, "Should find exactly one sensor");
    assert_eq!(results[0].sensor.name, "app_metrics");

    Ok(())
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Test empty matchers returns empty result
#[tokio::test]
#[serial]
async fn test_query_empty_matchers() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create a test sensor
    let sensor = create_sensor_with_labels("test_sensor", SensorType::Float, vec![]);
    publish_test_sensors(&storage, vec![(sensor, create_float_samples(3))]).await?;

    // Query with empty matchers
    let matchers: Vec<LabelMatcher> = vec![];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert!(
        results.is_empty(),
        "Empty matchers should return empty result"
    );

    Ok(())
}

/// Test no matching sensors returns empty result
#[tokio::test]
#[serial]
async fn test_query_no_matches() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create a test sensor
    let sensor = create_sensor_with_labels(
        "existing_sensor",
        SensorType::Float,
        vec![("environment".to_string(), "production".to_string())],
    );
    publish_test_sensors(&storage, vec![(sensor, create_float_samples(3))]).await?;

    // Query for non-existent sensor name
    let matchers = vec![LabelMatcher::eq("__name__", "nonexistent_sensor")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert!(
        results.is_empty(),
        "Should return empty for non-matching query"
    );

    Ok(())
}

/// Test querying sensors without labels using label matcher
#[tokio::test]
#[serial]
async fn test_query_sensor_without_labels() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create a sensor without labels
    let sensor = create_sensor_with_labels("no_labels_sensor", SensorType::Float, vec![]);
    publish_test_sensors(&storage, vec![(sensor, create_float_samples(3))]).await?;

    // Query by label - should not match sensor without that label
    let matchers = vec![LabelMatcher::eq("environment", "production")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert!(
        results.is_empty(),
        "Sensor without labels should not match label query"
    );

    Ok(())
}

/// Test time range filtering
#[tokio::test]
#[serial]
async fn test_query_with_time_range() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create a sensor with many samples
    let sensor = create_sensor_with_labels("time_test_sensor", SensorType::Float, vec![]);
    let samples = create_float_samples(10); // 10 samples, 1 minute apart
    publish_test_sensors(&storage, vec![(sensor, samples)]).await?;

    // Define time range (samples 3-7, which is 5 samples)
    let start_time = hifitime::Epoch::from_unix_seconds((1704067200 + 2 * 60) as f64); // 2 minutes in
    let end_time = hifitime::Epoch::from_unix_seconds((1704067200 + 6 * 60) as f64); // 6 minutes in

    let matchers = vec![LabelMatcher::eq("__name__", "time_test_sensor")];
    let results = storage
        .query_sensors_by_labels(&matchers, Some(start_time), Some(end_time), None, false)
        .await?;

    assert_eq!(results.len(), 1, "Should find one sensor");

    // Check sample count is within time range
    if let TypedSamples::Float(samples) = &results[0].samples {
        assert_eq!(samples.len(), 5, "Should have 5 samples within time range");
    } else {
        panic!("Expected float samples");
    }

    Ok(())
}

/// Test limit parameter
#[tokio::test]
#[serial]
async fn test_query_with_limit() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create a sensor with many samples
    let sensor = create_sensor_with_labels("limit_test_sensor", SensorType::Float, vec![]);
    let samples = create_float_samples(100);
    publish_test_sensors(&storage, vec![(sensor, samples)]).await?;

    let matchers = vec![LabelMatcher::eq("__name__", "limit_test_sensor")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, Some(10), false)
        .await?;

    assert_eq!(results.len(), 1, "Should find one sensor");

    if let TypedSamples::Float(samples) = &results[0].samples {
        assert_eq!(samples.len(), 10, "Should have limited to 10 samples");
    } else {
        panic!("Expected float samples");
    }

    Ok(())
}

/// The limit is a number of samples per sensor, the oldest first, not a total for the whole
/// query. The selector reads have a shared budget of their own (`query_selector`); this one is
/// the per-sensor limit of the trait, and the same on every backend.
#[tokio::test]
#[serial]
async fn test_query_limit_is_per_sensor() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    let group = Uuid::new_v4().to_string();
    let sensors = (0..3)
        .map(|index| {
            let sensor = create_sensor_with_labels(
                &format!("limit_per_sensor_{index}"),
                SensorType::Float,
                vec![("group".to_string(), group.clone())],
            );
            (sensor, create_float_samples(5))
        })
        .collect();
    publish_test_sensors(&storage, sensors).await?;

    let matchers = vec![LabelMatcher::eq("group", group)];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, Some(2), false)
        .await?;

    assert_eq!(results.len(), 3, "Should find the three sensors");
    for result in &results {
        let TypedSamples::Float(samples) = &result.samples else {
            panic!("Expected float samples");
        };
        let values: Vec<f64> = samples.iter().map(|sample| sample.value).collect();
        assert_eq!(
            values,
            vec![20.0, 21.0],
            "{}: the two oldest samples",
            result.sensor.name
        );
    }
    Ok(())
}

/// The same limit for every type of sample: the oldest `limit` samples of each sensor, and only
/// those (SQLite applies it in the query, with a window function, one query per type).
#[tokio::test]
#[serial]
async fn test_query_limit_applies_to_every_sample_type() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    fn samples_of<T>(
        count: usize,
        value: impl Fn(usize) -> T,
    ) -> smallvec::SmallVec<[Sample<T>; 4]> {
        (0..count)
            .map(|i| Sample {
                datetime: hifitime::Epoch::from_unix_seconds((1704067200 + i * 60) as f64),
                value: value(i),
            })
            .collect()
    }
    let all_types = |count: usize| -> Vec<(SensorType, TypedSamples)> {
        vec![
            (
                SensorType::Integer,
                TypedSamples::Integer(samples_of(count, |i| i as i64)),
            ),
            (
                SensorType::Numeric,
                TypedSamples::Numeric(samples_of(count, |i| {
                    rust_decimal::Decimal::new(i as i64, 1)
                })),
            ),
            (
                SensorType::Float,
                TypedSamples::Float(samples_of(count, |i| i as f64)),
            ),
            (
                SensorType::String,
                TypedSamples::String(samples_of(count, |i| format!("s{i}"))),
            ),
            (
                SensorType::Boolean,
                TypedSamples::Boolean(samples_of(count, |i| i % 2 == 0)),
            ),
            (
                SensorType::Location,
                TypedSamples::Location(samples_of(count, |i| geo::Point::new(i as f64, 60.0))),
            ),
            (
                SensorType::Json,
                TypedSamples::Json(samples_of(count, |i| serde_json::json!({ "i": i }))),
            ),
            (
                SensorType::Blob,
                TypedSamples::Blob(samples_of(count, |i| vec![i as u8])),
            ),
        ]
    };

    let group = Uuid::new_v4().to_string();
    let mut sensors = Vec::new();
    for copy in 0..2 {
        for (index, (sensor_type, samples)) in all_types(5).into_iter().enumerate() {
            let sensor = create_sensor_with_labels(
                &format!("limit_every_type_{index}_{copy}"),
                sensor_type,
                vec![("group".to_string(), group.clone())],
            );
            sensors.push((sensor, samples));
        }
    }
    publish_test_sensors(&storage, sensors).await?;

    let matchers = vec![LabelMatcher::eq("group", group)];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, Some(2), false)
        .await?;
    assert_eq!(results.len(), 16, "two sensors of each of the eight types");

    let seconds_of = |samples: &TypedSamples| -> Vec<i64> {
        fn seconds<T>(samples: &[Sample<T>]) -> Vec<i64> {
            samples
                .iter()
                .map(|sample| sample.datetime.to_unix_seconds() as i64 - 1704067200)
                .collect()
        }
        match samples {
            TypedSamples::Integer(s) => seconds(s),
            TypedSamples::Numeric(s) => seconds(s),
            TypedSamples::Float(s) => seconds(s),
            TypedSamples::String(s) => seconds(s),
            TypedSamples::Boolean(s) => seconds(s),
            TypedSamples::Location(s) => seconds(s),
            TypedSamples::Json(s) => seconds(s),
            TypedSamples::Blob(s) => seconds(s),
        }
    };
    for result in &results {
        assert_eq!(
            seconds_of(&result.samples),
            vec![0, 60],
            "{} ({}): the two oldest samples",
            result.sensor.name,
            result.sensor.sensor_type
        );
    }
    Ok(())
}

// ============================================================================
// Different Sensor Types
// ============================================================================

/// Test querying integer sensors
#[tokio::test]
#[serial]
async fn test_query_integer_sensor() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create integer sensor
    let sensor = create_sensor_with_labels(
        "request_count",
        SensorType::Integer,
        vec![("endpoint".to_string(), "/api/users".to_string())],
    );

    let samples: smallvec::SmallVec<[Sample<i64>; 4]> = (0..5)
        .map(|i| Sample {
            datetime: hifitime::Epoch::from_unix_seconds((1704067200 + i * 60) as f64),
            value: 100 + i as i64,
        })
        .collect();

    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(Arc::new(sensor), TypedSamples::Integer(samples))
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    let matchers = vec![LabelMatcher::eq("endpoint", "/api/users")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].sensor.name, "request_count");
    assert!(matches!(results[0].samples, TypedSamples::Integer(_)));

    Ok(())
}

/// Test querying string sensors
#[tokio::test]
#[serial]
async fn test_query_string_sensor() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create string sensor
    let sensor = create_sensor_with_labels(
        "status_log",
        SensorType::String,
        vec![("severity".to_string(), "info".to_string())],
    );

    let samples: smallvec::SmallVec<[Sample<String>; 4]> = (0..3)
        .map(|i| Sample {
            datetime: hifitime::Epoch::from_unix_seconds((1704067200 + i * 60) as f64),
            value: format!("Log message {}", i),
        })
        .collect();

    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(Arc::new(sensor), TypedSamples::String(samples))
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    let matchers = vec![LabelMatcher::eq("severity", "info")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].sensor.name, "status_log");
    assert!(matches!(results[0].samples, TypedSamples::String(_)));

    Ok(())
}

/// Test querying boolean sensors
#[tokio::test]
#[serial]
async fn test_query_boolean_sensor() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create boolean sensor
    let sensor = create_sensor_with_labels(
        "service_healthy",
        SensorType::Boolean,
        vec![("service".to_string(), "database".to_string())],
    );

    let samples: smallvec::SmallVec<[Sample<bool>; 4]> = (0..3)
        .map(|i| Sample {
            datetime: hifitime::Epoch::from_unix_seconds((1704067200 + i * 60) as f64),
            value: i % 2 == 0,
        })
        .collect();

    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(Arc::new(sensor), TypedSamples::Boolean(samples))
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    let matchers = vec![LabelMatcher::eq("service", "database")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].sensor.name, "service_healthy");
    assert!(matches!(results[0].samples, TypedSamples::Boolean(_)));

    Ok(())
}

// ============================================================================
// Regex Edge Cases
// ============================================================================

/// Test regex with special characters
#[tokio::test]
#[serial]
async fn test_query_regex_special_chars() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create sensors with various naming patterns
    let sensor1 = create_sensor_with_labels("http_requests_total", SensorType::Float, vec![]);
    let sensor2 = create_sensor_with_labels("http_request_duration", SensorType::Float, vec![]);
    let sensor3 = create_sensor_with_labels("grpc_requests", SensorType::Float, vec![]);

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
            (sensor3, create_float_samples(3)),
        ],
    )
    .await?;

    // Query using regex with word boundary-like pattern
    let matchers = vec![LabelMatcher::regex("__name__", "http_request.*")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(
        results.len(),
        2,
        "Should match http_requests_total and http_request_duration"
    );

    Ok(())
}

/// Test case-sensitive regex matching
#[tokio::test]
#[serial]
async fn test_query_regex_case_sensitive() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    let sensor1 = create_sensor_with_labels("CPU_Usage", SensorType::Float, vec![]);
    let sensor2 = create_sensor_with_labels("cpu_usage", SensorType::Float, vec![]);

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
        ],
    )
    .await?;

    // PostgreSQL ~ is case-sensitive by default
    let matchers = vec![LabelMatcher::regex("__name__", "cpu.*")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;

    assert_eq!(results.len(), 1, "Should only match lowercase cpu_usage");
    assert_eq!(results[0].sensor.name, "cpu_usage");

    Ok(())
}

// ============================================================================
// Performance Test
// ============================================================================

/// Test querying with many sensors (performance check)
#[tokio::test]
#[serial]
async fn test_query_performance_many_sensors() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create many sensors
    let mut sensors_with_samples = Vec::new();
    for i in 0..50 {
        let sensor = create_sensor_with_labels(
            &format!("perf_sensor_{}", i),
            SensorType::Float,
            vec![
                (
                    "environment".to_string(),
                    if i % 2 == 0 { "production" } else { "staging" }.to_string(),
                ),
                ("index".to_string(), i.to_string()),
            ],
        );
        sensors_with_samples.push((sensor, create_float_samples(10)));
    }

    publish_test_sensors(&storage, sensors_with_samples).await?;

    // Time the query
    let start = std::time::Instant::now();
    let matchers = vec![LabelMatcher::eq("environment", "production")];
    let results = storage
        .query_sensors_by_labels(&matchers, None, None, None, false)
        .await?;
    let duration = start.elapsed();

    assert_eq!(results.len(), 25, "Should find 25 production sensors");
    assert!(
        duration.as_secs() < 5,
        "Query should complete in under 5 seconds"
    );

    Ok(())
}
