/// HTTP testing utilities
use anyhow::Result;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::{get, post};
use sensapp::ingestors::http::crud::{get_series_data, list_series, list_metrics};
use sensapp::ingestors::http::state::HttpServerState;
use sensapp::storage::StorageInstance;
use std::sync::Arc;
use tower::ServiceExt; // for `oneshot` and `ready`

/// HTTP test client for making requests to our app
pub struct TestApp {
    app: axum::Router,
}

impl TestApp {
    /// Create a new test app with the provided storage
    pub async fn new(storage: Arc<dyn StorageInstance>) -> Self {
        let state = HttpServerState {
            name: Arc::new("SensApp Test".to_string()),
            storage,
        };

        // Create a minimal router for testing (without middleware that might interfere)
        // We'll define simple test handlers that delegate to the import functions
        let app = Router::new()
            .route("/sensors/publish", post(test_publish_csv_handler))
            .route("/metrics", get(list_metrics))
            .route("/series", get(list_series))
            .route("/series/{series_uuid}", get(get_series_data))
            .with_state(state);

        Self { app }
    }

    /// Send a POST request with CSV data
    pub async fn post_csv(&self, path: &str, csv_data: &str) -> Result<TestResponse> {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "text/csv")
            .body(Body::from(csv_data.to_string()))?;

        let response = self.app.clone().oneshot(request).await?;
        Ok(TestResponse::new(response).await)
    }

    /// Send a POST request with JSON data
    pub async fn post_json(&self, path: &str, json_data: &str) -> Result<TestResponse> {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(json_data.to_string()))?;

        let response = self.app.clone().oneshot(request).await?;
        Ok(TestResponse::new(response).await)
    }

    /// Send a GET request
    pub async fn get(&self, path: &str) -> Result<TestResponse> {
        let request = Request::builder()
            .method("GET")
            .uri(path)
            .body(Body::empty())?;

        let response = self.app.clone().oneshot(request).await?;
        Ok(TestResponse::new(response).await)
    }

    /// Send a POST request with SenML data
    pub async fn post_senml(&self, path: &str, senml_data: &str) -> Result<TestResponse> {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/senml+json")
            .body(Body::from(senml_data.to_string()))?;

        let response = self.app.clone().oneshot(request).await?;
        Ok(TestResponse::new(response).await)
    }

    /// Send a POST request with InfluxDB line protocol data
    pub async fn post_influxdb(&self, path: &str, influx_data: &str) -> Result<TestResponse> {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "text/plain")
            .body(Body::from(influx_data.to_string()))?;

        let response = self.app.clone().oneshot(request).await?;
        Ok(TestResponse::new(response).await)
    }
}

/// Test response wrapper for easier assertions
pub struct TestResponse {
    status: StatusCode,
    body: String,
}

impl TestResponse {
    async fn new(response: axum::response::Response) -> Self {
        let status = response.status();
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap_or_default();
        let body = String::from_utf8_lossy(&body_bytes).to_string();

        Self { status, body }
    }

    /// Get response status
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Get response body as string
    pub fn body(&self) -> &str {
        &self.body
    }

    /// Check if response was successful (2xx)
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Parse response body as JSON
    pub fn json<T>(&self) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        serde_json::from_str(&self.body).map_err(Into::into)
    }

    /// Assert status code
    pub fn assert_status(&self, expected: StatusCode) -> &Self {
        assert_eq!(
            self.status, expected,
            "Expected status {}, got {}. Body: {}",
            expected, self.status, self.body
        );
        self
    }

    /// Assert response is successful
    pub fn assert_success(&self) -> &Self {
        assert!(
            self.is_success(),
            "Expected success response, got {}. Body: {}",
            self.status,
            self.body
        );
        self
    }

    /// Assert response body contains text
    pub fn assert_body_contains(&self, text: &str) -> &Self {
        assert!(
            self.body.contains(text),
            "Expected body to contain '{}', but body was: {}",
            text,
            self.body
        );
        self
    }
}

/// Simple CSV publish handler for testing
async fn test_publish_csv_handler(
    axum::extract::State(state): axum::extract::State<HttpServerState>,
    body: Body,
) -> Result<String, (StatusCode, String)> {
    use futures::TryStreamExt;
    use sensapp::importers::csv::publish_csv_async;
    use std::io;

    // Convert the body to a CSV reader (same way as the actual server)
    let stream = body.into_data_stream();
    let stream = stream.map_err(io::Error::other);
    let reader = stream.into_async_read();

    let csv_reader = csv_async::AsyncReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',') // Use comma for tests, semicolon is the server default
        .create_reader(reader);

    publish_csv_async(csv_reader, 1000, state.storage.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok("ok".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response_helpers() {
        let response = TestResponse {
            status: StatusCode::OK,
            body: "test body".to_string(),
        };

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body(), "test body");
        assert!(response.is_success());
    }
}
