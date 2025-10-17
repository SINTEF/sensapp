mod common;

use anyhow::Result;
use axum::http::StatusCode;
use common::http::TestApp;
use common::TestDb;
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

mod health_check_tests {
    use super::*;

    #[tokio::test]
    #[serial]
    async fn test_liveness_endpoint() -> Result<()> {
        ensure_config();
        // Given: A running SensApp instance
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // When: We query the liveness endpoint
        let response = app.get("/health/live").await?;

        // Then: Response should be successful
        response.assert_status(StatusCode::OK);

        let health_response: Value = response.json()?;
        assert_eq!(health_response["status"], "ok");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_readiness_endpoint_with_healthy_database() -> Result<()> {
        ensure_config();
        // Given: A running SensApp instance with a healthy database
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // When: We query the readiness endpoint
        let response = app.get("/health/ready").await?;

        // Then: Response should indicate the service is ready
        response.assert_status(StatusCode::OK);

        let readiness_response: Value = response.json()?;
        assert_eq!(readiness_response["status"], "ready");
        assert_eq!(readiness_response["database"], "ok");
        assert!(readiness_response["error"].is_null());

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_readiness_endpoint_after_data_ingestion() -> Result<()> {
        ensure_config();
        // Given: A database with ingested data
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest some data
        let csv_data = "datetime,sensor_name,value,unit\n2024-01-01T00:00:00Z,temperature,23.5,celsius\n";
        app.post_csv("/sensors/publish", csv_data).await?;

        // When: We query the readiness endpoint
        let response = app.get("/health/ready").await?;

        // Then: Response should still indicate the service is ready
        response.assert_status(StatusCode::OK);

        let readiness_response: Value = response.json()?;
        assert_eq!(readiness_response["status"], "ready");
        assert_eq!(readiness_response["database"], "ok");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_liveness_is_independent_of_database_state() -> Result<()> {
        ensure_config();
        // Given: Any running SensApp instance
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // When: We query the liveness endpoint multiple times
        let response1 = app.get("/health/live").await?;
        let response2 = app.get("/health/live").await?;

        // Then: Both responses should always be successful
        // Liveness should not depend on database state
        response1.assert_status(StatusCode::OK);
        response2.assert_status(StatusCode::OK);

        let health1: Value = response1.json()?;
        let health2: Value = response2.json()?;
        assert_eq!(health1["status"], "ok");
        assert_eq!(health2["status"], "ok");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_health_endpoints_return_json() -> Result<()> {
        ensure_config();
        // Given: A running SensApp instance
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // When: We query both health endpoints
        let liveness_response = app.get("/health/live").await?;
        let readiness_response = app.get("/health/ready").await?;

        // Then: Both should return valid JSON with correct structure
        let liveness_json: Value = liveness_response.json()?;
        assert!(liveness_json.is_object());
        assert!(liveness_json.get("status").is_some());

        let readiness_json: Value = readiness_response.json()?;
        assert!(readiness_json.is_object());
        assert!(readiness_json.get("status").is_some());
        assert!(readiness_json.get("database").is_some());

        Ok(())
    }
}
