mod common;

#[cfg(feature = "clickhouse")]
mod clickhouse_http_lifecycle_tests {
    use super::common::db::DbHelpers;
    use super::common::http::TestApp;
    use super::common::{DatabaseType, TestDb};
    use anyhow::Result;
    use axum::http::StatusCode;
    use hifitime::Epoch;
    use sensapp::config::load_configuration_for_tests;
    use sensapp::storage::StorageInstance;
    use serde_json::Value;
    use serial_test::serial;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use uuid::Uuid;

    static INIT: std::sync::Once = std::sync::Once::new();

    fn ensure_config() {
        INIT.call_once(|| {
            load_configuration_for_tests().expect("Failed to load configuration for tests");
        });
    }

    fn unique_metric_name(prefix: &str) -> String {
        format!("{}_{}", prefix, Uuid::new_v4().simple())
    }

    fn iso8601_from_unix_seconds(seconds: i64) -> String {
        Epoch::from_unix_seconds(seconds as f64).to_rfc3339()
    }

    async fn clickhouse_app() -> Result<(Arc<dyn StorageInstance>, TestApp)> {
        let test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;
        Ok((storage, app))
    }

    #[tokio::test]
    #[serial]
    async fn test_clickhouse_http_csv_lifecycle() -> Result<()> {
        ensure_config();
        let (storage, app) = clickhouse_app().await?;

        let readiness = app.get("/health/ready").await?;
        readiness.assert_status(StatusCode::OK);
        let readiness_body: Value = readiness.json()?;
        assert_eq!(readiness_body["status"], "ready");

        let sensor_name = unique_metric_name("temperature_http");
        let now = Epoch::now()?.to_unix_seconds().floor() as i64;
        let csv_data = format!(
            "datetime,sensor_name,value,unit\n{0},{3},20.5,C\n{1},{3},21.0,C\n{2},{3},21.5,C",
            iso8601_from_unix_seconds(now - 120),
            iso8601_from_unix_seconds(now - 60),
            iso8601_from_unix_seconds(now),
            sensor_name,
        );

        app.post_csv("/sensors/publish", &csv_data)
            .await?
            .assert_status(StatusCode::OK)
            .assert_body_contains("ingested successfully");

        let series = app.get("/series").await?;
        series.assert_status(StatusCode::OK);
        series.assert_body_contains(&sensor_name);

        let metrics = app.get("/metrics").await?;
        metrics.assert_status(StatusCode::OK);
        metrics.assert_body_contains(&sensor_name);

        let sensor = DbHelpers::get_sensor_by_name(&storage, &sensor_name)
            .await?
            .expect("sensor should exist after CSV publish");

        let query_response = app
            .get(&format!("/api/v1/query?query={}&format=jsonl", sensor_name))
            .await?;
        query_response.assert_status(StatusCode::OK);
        query_response.assert_body_contains(&sensor_name);

        let export_response = app
            .get(&format!("/series/{}?format=csv", sensor.uuid))
            .await?;
        export_response.assert_status(StatusCode::OK);
        export_response.assert_content_type("text/csv");
        export_response.assert_body_contains("timestamp");

        let last_response = app.get(&format!("/series/{}/last", sensor.uuid)).await?;
        last_response.assert_status(StatusCode::OK);
        let last_payload: Value = last_response.json()?;
        assert_eq!(last_payload["sensor_name"], sensor_name);
        assert_eq!(last_payload["value"], 21.5);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_clickhouse_http_influx_lifecycle() -> Result<()> {
        ensure_config();
        let (_storage, app) = clickhouse_app().await?;

        let metric_name = unique_metric_name("power_http");
        let series_name = format!("{} value", metric_name);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after unix epoch");
        let first_timestamp_ns = now
            .checked_sub(Duration::from_secs(60))
            .expect("enough time since epoch")
            .as_nanos();
        let second_timestamp_ns = now.as_nanos();
        let influx_body = format!(
            "{0},site=lab value=42.5 {1}\n{0},site=lab value=43.0 {2}",
            metric_name, first_timestamp_ns, second_timestamp_ns
        );

        app.post_influxdb(
            "/api/v2/write?bucket=clickhouse_http_test&org=sensapp",
            &influx_body,
        )
        .await?
        .assert_status(StatusCode::NO_CONTENT);

        let query_response = app
            .get(&format!(
                "/api/v1/query?query=%7B__name__%3D%22{}%22%7D&format=csv",
                urlencoding::encode(&series_name)
            ))
            .await?;
        query_response.assert_status(StatusCode::OK);
        query_response.assert_content_type("text/csv");
        query_response.assert_body_contains(&series_name);

        let series_response = app.get("/series").await?;
        series_response.assert_status(StatusCode::OK);
        series_response.assert_body_contains(&series_name);

        Ok(())
    }
}
