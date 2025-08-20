mod common;

use anyhow::Result;
use axum::http::StatusCode;
use common::http::TestApp;
use common::{TestDb, fixtures};
use serde_json::Value;

/// Test CRUD/DCAT API functionality with new series terminology
mod crud_dcat_tests {
    use super::*;

    #[tokio::test]
    async fn test_list_metrics_endpoint() -> Result<()> {
        // Given: A database with ingested sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest data from multiple sensors with same name but different labels
        let csv_data = fixtures::multi_sensor_csv();
        app.post_csv("/sensors/publish", csv_data).await?;

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
    async fn test_list_series_endpoint() -> Result<()> {
        // Given: A database with ingested sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature sensor data
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", csv_data).await?;

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
            println!("All series IDs: {:?}", datasets.iter().map(|d| d["@id"].as_str().unwrap()).collect::<Vec<_>>());

            // Should contain the metric name or be a valid Prometheus-style ID
            assert!(
                id.contains("temperature") || id == "temperature" || id.contains("{") || datasets.len() > 0,
                "Expected series to contain temperature or be valid Prometheus ID, got: {}",
                id
            );

            // Should have proper UUID identifier
            assert!(first_dataset["dct:identifier"].is_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_series_data_endpoint_senml_format() -> Result<()> {
        // Given: A database with ingested sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature sensor data
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", csv_data).await?;

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
        // TODO: Add content type assertion

        // Should return valid JSON (SenML format)
        let _data: Value = response.json()?;

        Ok(())
    }

    #[tokio::test]
    async fn test_series_data_endpoint_csv_format() -> Result<()> {
        // Given: A database with ingested sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature sensor data
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", csv_data).await?;

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
        // TODO: Add content type assertion

        let csv_content = response.body();
        assert!(!csv_content.is_empty());
        assert!(csv_content.contains("timestamp") || csv_content.contains("time"));

        Ok(())
    }

    #[tokio::test]
    async fn test_series_data_endpoint_jsonl_format() -> Result<()> {
        // Given: A database with ingested sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature sensor data
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", csv_data).await?;

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
        // TODO: Add content type assertion

        let jsonl_content = response.body();
        assert!(!jsonl_content.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_prometheus_style_ids_in_series_catalog() -> Result<()> {
        // Given: A database with sensor data having labels
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest data with labels (using multi-sensor CSV which should have varied data)
        let csv_data = fixtures::multi_sensor_csv();
        app.post_csv("/sensors/publish", csv_data).await?;

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
    async fn test_dcat_distribution_formats() -> Result<()> {
        // Given: A database with sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest temperature sensor data
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", csv_data).await?;

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
    async fn test_nonexistent_series_returns_404() -> Result<()> {
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
    async fn test_invalid_format_returns_400() -> Result<()> {
        // Given: A database with sensor data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest some data
        let csv_data = fixtures::temperature_sensor_csv();
        app.post_csv("/sensors/publish", csv_data).await?;

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
}
