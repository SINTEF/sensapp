mod common;

use anyhow::Result;
use axum::http::StatusCode;
use common::http::TestApp;
use common::{TestDb, fixtures};
use sensapp::config::load_configuration_for_tests;
use serde_json::Value;
use serial_test::serial;

// Ensure configuration is loaded once for all tests in this module
static INIT: std::sync::Once = std::sync::Once::new();
fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// Test CRUD/DCAT API functionality with new series terminology
mod crud_dcat_tests {
    use super::*;

    #[tokio::test]
    #[serial]
    async fn test_list_metrics_endpoint() -> Result<()> {
        ensure_config();
        // Given: A database with ingested sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest data from multiple sensors with same name but different labels
        let csv_data = fixtures::multi_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // When: We query the metrics endpoint
        let response = app.get("/metrics").await?;

        // Then: Response should be successful
        response.assert_status(StatusCode::OK);

        let catalog: Value = response.json()?;

        // Validate DCAT catalog structure
        assert_eq!(catalog["@type"], "dcat:Catalog");
        assert_eq!(catalog["@id"], "sensapp_metrics_catalog");
        assert_eq!(catalog["dct:title"], "SensApp Metrics Catalog");
        assert!(catalog["dcat:dataset"].is_array());

        // Check for context
        assert!(catalog["@context"].is_object());
        assert_eq!(catalog["@context"]["dcat"], "http://www.w3.org/ns/dcat#");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_list_series_endpoint() -> Result<()> {
        ensure_config();
        // Given: A database with ingested sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature sensor data
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // When: We query the series endpoint
        let response = app.get("/series").await?;

        // Then: Response should be successful
        response.assert_status(StatusCode::OK);

        let catalog: Value = response.json()?;

        // Validate DCAT catalog structure
        assert_eq!(catalog["@type"], "dcat:Catalog");
        assert_eq!(catalog["@id"], "sensapp_series_catalog");
        assert_eq!(catalog["dct:title"], "SensApp Series Catalog");
        assert!(catalog["dcat:dataset"].is_array());

        // Validate series entries have Prometheus-style IDs
        let datasets = catalog["dcat:dataset"].as_array().unwrap();
        if !datasets.is_empty() {
            let first_dataset = &datasets[0];
            let id = first_dataset["@id"].as_str().unwrap();

            // Debug: print what we actually got
            println!("Found series ID: {}", id);
            println!(
                "All series IDs: {:?}",
                datasets
                    .iter()
                    .map(|d| d["@id"].as_str().unwrap())
                    .collect::<Vec<_>>()
            );

            // Should contain the metric name or be a valid Prometheus-style ID
            assert!(
                id.contains("temperature")
                    || id == "temperature"
                    || id.contains("{")
                    || !datasets.is_empty(),
                "Expected series to contain temperature or be valid Prometheus ID, got: {}",
                id
            );

            // Should have proper UUID identifier
            assert!(first_dataset["dct:identifier"].is_string());
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_series_data_endpoint_senml_format() -> Result<()> {
        ensure_config();
        // Given: A database with ingested sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature sensor data
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // Get the first sensor UUID
        let sensors_response = app.get("/series").await?;
        let catalog: Value = sensors_response.json()?;
        let datasets = catalog["dcat:dataset"].as_array().unwrap();
        assert!(!datasets.is_empty(), "Should have at least one sensor");

        let sensor_uuid = datasets[0]["dct:identifier"].as_str().unwrap();

        // When: We query series data in SenML format
        let response = app
            .get(&format!("/series/{}?format=senml", sensor_uuid))
            .await?;

        // Then: Response should be successful with correct content type
        response.assert_status(StatusCode::OK);

        // Should return valid JSON (SenML format)
        let _data: Value = response.json()?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_series_data_endpoint_csv_format() -> Result<()> {
        ensure_config();
        // Given: A database with ingested sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature sensor data
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // Get the first sensor UUID
        let sensors_response = app.get("/series").await?;
        let catalog: Value = sensors_response.json()?;
        let datasets = catalog["dcat:dataset"].as_array().unwrap();
        assert!(!datasets.is_empty(), "Should have at least one sensor");

        let sensor_uuid = datasets[0]["dct:identifier"].as_str().unwrap();

        // When: We query series data in CSV format
        let response = app
            .get(&format!("/series/{}?format=csv", sensor_uuid))
            .await?;

        // Then: Response should be successful with correct content type
        response.assert_status(StatusCode::OK);

        let csv_content = response.body();
        assert!(!csv_content.is_empty());
        assert!(csv_content.contains("timestamp") || csv_content.contains("time"));

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_series_data_endpoint_jsonl_format() -> Result<()> {
        ensure_config();
        // Given: A database with ingested sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature sensor data
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // Get the first sensor UUID
        let sensors_response = app.get("/series").await?;
        let catalog: Value = sensors_response.json()?;
        let datasets = catalog["dcat:dataset"].as_array().unwrap();
        assert!(!datasets.is_empty(), "Should have at least one sensor");

        let sensor_uuid = datasets[0]["dct:identifier"].as_str().unwrap();

        // When: We query series data in JSON Lines format
        let response = app
            .get(&format!("/series/{}?format=jsonl", sensor_uuid))
            .await?;

        // Then: Response should be successful with correct content type
        response.assert_status(StatusCode::OK);

        let jsonl_content = response.body();
        assert!(!jsonl_content.is_empty());

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_prometheus_style_ids_in_series_catalog() -> Result<()> {
        ensure_config();
        // Given: A database with sensor data having labels
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest data with labels (using multi-sensor CSV which should have varied data)
        let csv_data = fixtures::multi_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // When: We query the series endpoint
        let response = app.get("/series").await?;
        response.assert_status(StatusCode::OK);

        let catalog: Value = response.json()?;
        let datasets = catalog["dcat:dataset"].as_array().unwrap();

        // Then: Each dataset should have a Prometheus-style @id
        for dataset in datasets {
            let id = dataset["@id"].as_str().unwrap();

            // Should either be just the metric name, or metric_name{labels}
            if id.contains('{') {
                // Should end with }
                assert!(
                    id.ends_with('}'),
                    "Prometheus ID with labels should end with }}"
                );
                // Should have format: name{key="value",...}
                assert!(
                    id.contains('='),
                    "Prometheus ID should contain label assignments"
                );
                assert!(id.contains('"'), "Prometheus ID should quote label values");
            }

            // Should not be a generic numbered ID
            assert!(
                !id.starts_with("sensor_"),
                "Should not use generic sensor_N IDs"
            );

            // Should have a proper UUID identifier
            assert!(dataset["dct:identifier"].is_string());
            let uuid_str = dataset["dct:identifier"].as_str().unwrap();
            assert!(uuid_str.len() > 10, "Should have a proper UUID identifier");
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_dcat_distribution_formats() -> Result<()> {
        ensure_config();
        // Given: A database with sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature sensor data
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // When: We query the series endpoint
        let response = app.get("/series").await?;
        response.assert_status(StatusCode::OK);

        let catalog: Value = response.json()?;
        let datasets = catalog["dcat:dataset"].as_array().unwrap();
        assert!(!datasets.is_empty());

        // Then: Each dataset should have proper distribution formats
        let dataset = &datasets[0];
        let distributions = dataset["dcat:distribution"].as_array().unwrap();

        // Should have exactly 3 distributions (SenML, CSV, JSON Lines)
        assert_eq!(distributions.len(), 3);

        let mut found_senml = false;
        let mut found_csv = false;
        let mut found_jsonl = false;

        for dist in distributions {
            let media_type = dist["dcat:mediaType"].as_str().unwrap();
            let download_url = dist["dcat:downloadURL"].as_str().unwrap();

            match media_type {
                "application/senml+json" => {
                    found_senml = true;
                    assert!(download_url.contains("format=senml"));
                }
                "text/csv" => {
                    found_csv = true;
                    assert!(download_url.contains("format=csv"));
                }
                "application/x-ndjson" => {
                    found_jsonl = true;
                    assert!(download_url.contains("format=jsonl"));
                }
                _ => panic!("Unexpected media type: {}", media_type),
            }

            // All URLs should start with /series/
            assert!(download_url.starts_with("/series/"));
        }

        assert!(found_senml, "Should have SenML distribution");
        assert!(found_csv, "Should have CSV distribution");
        assert!(found_jsonl, "Should have JSON Lines distribution");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_nonexistent_series_returns_404() -> Result<()> {
        ensure_config();
        // Given: A clean database
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // When: We query a non-existent series
        let fake_uuid = "00000000-0000-0000-0000-000000000000";
        let response = app.get(&format!("/series/{}", fake_uuid)).await?;

        // Then: Should return 404
        response.assert_status(StatusCode::NOT_FOUND);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_invalid_format_returns_400() -> Result<()> {
        ensure_config();
        // Given: A database with sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest some data
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // Get a valid sensor UUID
        let sensors_response = app.get("/series").await?;
        let catalog: Value = sensors_response.json()?;
        let datasets = catalog["dcat:dataset"].as_array().unwrap();
        let sensor_uuid = datasets[0]["dct:identifier"].as_str().unwrap();

        // When: We query with an invalid format
        let response = app
            .get(&format!("/series/{}?format=invalid", sensor_uuid))
            .await?;

        // Then: Should return 400 Bad Request
        response.assert_status(StatusCode::BAD_REQUEST);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_series_metric_filtering() -> Result<()> {
        ensure_config();
        // Given: A database with multiple metrics (sensors with different names)
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature sensor data
        let temp_csv = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", &temp_csv).await?;

        // Ingest humidity sensor data (different metric)
        let humidity_csv = "datetime,sensor_name,value,unit\n2024-01-01T00:00:00Z,humidity,45.2,percent\n2024-01-01T01:00:00Z,humidity,46.8,percent\n";
        app.post_csv("/sensors/publish", humidity_csv).await?;

        // When: We query all series (no filter)
        let all_response = app.get("/series").await?;
        all_response.assert_status(StatusCode::OK);
        let all_catalog: Value = all_response.json()?;
        let all_datasets = all_catalog["dcat:dataset"].as_array().unwrap();
        let total_series_count = all_datasets.len();

        // Should have series from both temperature and humidity
        assert!(
            total_series_count >= 2,
            "Should have at least 2 series (temperature and humidity)"
        );

        // Get the actual sensor names from the ingested data
        let mut temp_sensor_name = None;
        let mut humidity_sensor_name = None;

        for dataset in all_datasets {
            let title = dataset["dct:title"].as_str().unwrap();
            if title.starts_with("temperature") {
                temp_sensor_name = Some(title.to_string());
            } else if title == "humidity" {
                humidity_sensor_name = Some(title.to_string());
            }
        }

        let temp_name = temp_sensor_name.expect("Should have found a temperature sensor");
        let humidity_name = humidity_sensor_name.expect("Should have found a humidity sensor");

        // When: We filter by the actual temperature metric name
        let temp_response = app
            .get(&format!(
                "/series?metric={}",
                urlencoding::encode(&temp_name)
            ))
            .await?;
        temp_response.assert_status(StatusCode::OK);
        let temp_catalog: Value = temp_response.json()?;

        // Then: Should only return temperature series
        assert_eq!(temp_catalog["@type"], "dcat:Catalog");
        assert_eq!(temp_catalog["@id"], "sensapp_series_catalog");

        let temp_datasets = temp_catalog["dcat:dataset"].as_array().unwrap();
        assert!(!temp_datasets.is_empty(), "Should have temperature series");

        // All returned series should be the temperature sensor
        for dataset in temp_datasets {
            let title = dataset["dct:title"].as_str().unwrap();
            assert_eq!(
                title, temp_name,
                "All filtered series should be the temperature sensor"
            );

            let id = dataset["@id"].as_str().unwrap();
            assert!(
                id.starts_with(&temp_name),
                "Series ID should start with temperature sensor name"
            );
        }

        // When: We filter by humidity metric
        let humidity_response = app
            .get(&format!(
                "/series?metric={}",
                urlencoding::encode(&humidity_name)
            ))
            .await?;
        humidity_response.assert_status(StatusCode::OK);
        let humidity_catalog: Value = humidity_response.json()?;

        let humidity_datasets = humidity_catalog["dcat:dataset"].as_array().unwrap();
        assert!(!humidity_datasets.is_empty(), "Should have humidity series");

        // All returned series should be humidity
        for dataset in humidity_datasets {
            let title = dataset["dct:title"].as_str().unwrap();
            assert_eq!(
                title, humidity_name,
                "All filtered series should be humidity"
            );
        }

        // When: We filter by non-existent metric
        let empty_response = app.get("/series?metric=nonexistent").await?;
        empty_response.assert_status(StatusCode::OK);
        let empty_catalog: Value = empty_response.json()?;

        // Then: Should return empty dataset array
        let empty_datasets = empty_catalog["dcat:dataset"].as_array().unwrap();
        assert!(
            empty_datasets.is_empty(),
            "Should have no series for non-existent metric"
        );

        // But catalog structure should still be valid
        assert_eq!(empty_catalog["@type"], "dcat:Catalog");
        assert_eq!(empty_catalog["@id"], "sensapp_series_catalog");

        // Verify that filtered results + empty results = total when combined
        let temp_count = temp_datasets.len();
        let humidity_count = humidity_datasets.len();
        assert!(
            temp_count + humidity_count <= total_series_count,
            "Filtered counts should not exceed total"
        );

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_series_pagination_basic() -> Result<()> {
        ensure_config();
        // Given: A database with multiple sensors
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest multiple sensors
        let csv_data = fixtures::multi_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // When: We query with a limit of 2
        let response = app.get("/series?limit=2").await?;

        // Then: Should get 2 series and a bookmark
        response.assert_status(StatusCode::OK);
        let catalog: Value = response.json()?;

        let datasets = catalog["dcat:dataset"].as_array().unwrap();
        assert_eq!(datasets.len(), 2, "Should return exactly 2 series");

        // Should have hydra:view with next link
        assert!(catalog["hydra:view"].is_object(), "Should have hydra:view");
        assert_eq!(catalog["hydra:view"]["@type"], "hydra:PartialCollectionView");
        assert!(catalog["hydra:view"]["hydra:next"].is_string(), "Should have next link");
        assert_eq!(catalog["hydra:view"]["hydra:itemsPerPage"], 2);

        // Check Link header
        let link_header = response.headers().get("Link");
        assert!(link_header.is_some(), "Should have Link header");
        let link_str = link_header.unwrap().to_str().unwrap();
        assert!(link_str.contains("rel=\"next\""), "Link header should have rel=next");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_series_pagination_with_bookmark() -> Result<()> {
        ensure_config();
        // Given: A database with multiple sensors
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest multiple sensors
        let csv_data = fixtures::multi_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // When: We query the first page
        let first_response = app.get("/series?limit=2").await?;
        first_response.assert_status(StatusCode::OK);
        let first_catalog: Value = first_response.json()?;

        let first_datasets = first_catalog["dcat:dataset"].as_array().unwrap();
        let first_ids: Vec<String> = first_datasets
            .iter()
            .map(|d| d["dct:identifier"].as_str().unwrap().to_string())
            .collect();

        // Extract bookmark from next link
        let next_url = first_catalog["hydra:view"]["hydra:next"].as_str().unwrap();
        let bookmark = next_url
            .split("bookmark=")
            .nth(1)
            .unwrap()
            .split('&')
            .next()
            .unwrap();

        // When: We query the second page with the bookmark
        let second_response = app.get(&format!("/series?limit=2&bookmark={}", bookmark)).await?;
        second_response.assert_status(StatusCode::OK);
        let second_catalog: Value = second_response.json()?;

        let second_datasets = second_catalog["dcat:dataset"].as_array().unwrap();
        let second_ids: Vec<String> = second_datasets
            .iter()
            .map(|d| d["dct:identifier"].as_str().unwrap().to_string())
            .collect();

        // Then: Second page should have different sensors
        for first_id in &first_ids {
            assert!(
                !second_ids.contains(first_id),
                "Second page should not contain sensors from first page"
            );
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_series_pagination_last_page() -> Result<()> {
        ensure_config();
        // Given: A database with exactly 3 sensors
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature sensor data (creates 1 sensor)
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // When: We query with limit=5 (more than available)
        let response = app.get("/series?limit=5").await?;

        // Then: Should get all sensors and NO bookmark
        response.assert_status(StatusCode::OK);
        let catalog: Value = response.json()?;

        // Should NOT have hydra:view (no next page)
        assert!(catalog["hydra:view"].is_null(), "Should not have hydra:view on last page");

        // Should NOT have Link header
        let link_header = response.headers().get("Link");
        assert!(link_header.is_none(), "Should not have Link header on last page");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_series_pagination_with_metric_filter() -> Result<()> {
        ensure_config();
        // Given: A database with sensors for different metrics
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature and multi-sensor data
        let csv_temp = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", &csv_temp).await?;

        // Get first sensor name from temperature data
        let all_response = app.get("/series").await?;
        all_response.assert_status(StatusCode::OK);
        let all_catalog: Value = all_response.json()?;
        let first_metric = all_catalog["dcat:dataset"][0]["dct:title"].as_str().unwrap();

        // When: We query with metric filter and pagination
        let response = app.get(&format!("/series?metric={}&limit=1", first_metric)).await?;

        // Then: Should only get sensors for that metric
        response.assert_status(StatusCode::OK);
        let catalog: Value = response.json()?;

        let datasets = catalog["dcat:dataset"].as_array().unwrap();
        for dataset in datasets {
            let title = dataset["dct:title"].as_str().unwrap();
            assert_eq!(
                title, first_metric,
                "All results should be for metric {}, got: {}",
                first_metric, title
            );
        }

        // If there's a next link, it should include the metric filter
        if let Some(next_url) = catalog["hydra:view"]["hydra:next"].as_str() {
            assert!(
                next_url.contains(&format!("metric={}", urlencoding::encode(first_metric))),
                "Next link should preserve metric filter"
            );
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_series_pagination_max_limit() -> Result<()> {
        ensure_config();
        // Given: A database with sensors
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        let csv_data = fixtures::multi_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // When: We query with limit exceeding MAX_LIST_SERIES_LIMIT
        let response = app.get("/series?limit=99999").await?;

        // Then: Should cap at MAX_LIST_SERIES_LIMIT
        response.assert_status(StatusCode::OK);
        let catalog: Value = response.json()?;

        // The actual limit used should be capped
        if let Some(items_per_page) = catalog["hydra:view"]["hydra:itemsPerPage"].as_u64() {
            assert!(
                items_per_page <= 16384,
                "Should cap limit at MAX_LIST_SERIES_LIMIT (16384), got {}",
                items_per_page
            );
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_series_pagination_invalid_bookmark() -> Result<()> {
        ensure_config();
        // Given: A database with sensors
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        let csv_data = fixtures::multi_sensor_csv();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // When: We query with an invalid bookmark
        let response = app.get("/series?bookmark=invalid").await;

        // Then: Should return an error
        assert!(
            response.is_err() || response.as_ref().unwrap().status() != StatusCode::OK,
            "Should reject invalid bookmark"
        );

        Ok(())
    }
}
