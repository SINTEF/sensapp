use crate::ingestors::http::server::build_app_routes;
use crate::ingestors::http::state::HttpServerState;
use crate::storage::StorageInstance;
/// HTTP testing utilities
use anyhow::Result;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderMap, Request, StatusCode};
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

        // Use the shared route builder from the main server
        // This ensures tests use the exact same routes as production
        let max_body_layer = DefaultBodyLimit::max(10 * 1024 * 1024); // 10MB for tests
        let app = build_app_routes(state, max_body_layer);

        Self { app }
    }

    /// Send a POST request with CSV data (uses infer mode by default)
    pub async fn post_csv(&self, path: &str, csv_data: &str) -> Result<TestResponse> {
        let request = Request::builder()
            .method("POST")
            .uri(format!("{}?mode=infer", path))
            .header("content-type", "text/csv")
            .body(Body::from(csv_data.to_string()))?;

        let response = self.app.clone().oneshot(request).await?;
        Ok(TestResponse::new(response).await)
    }

    /// Send a POST request with CSV data in strict mode
    pub async fn post_csv_strict(&self, path: &str, csv_data: &str) -> Result<TestResponse> {
        let request = Request::builder()
            .method("POST")
            .uri(format!("{}?mode=strict", path))
            .header("content-type", "text/csv")
            .body(Body::from(csv_data.to_string()))?;

        let response = self.app.clone().oneshot(request).await?;
        Ok(TestResponse::new(response).await)
    }

    /// Send a POST request with CSV data in infer mode (explicit)
    pub async fn post_csv_infer(&self, path: &str, csv_data: &str) -> Result<TestResponse> {
        let request = Request::builder()
            .method("POST")
            .uri(format!("{}?mode=infer", path))
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

    /// Send a POST request with binary data (e.g., Arrow files)
    pub async fn post_binary(
        &self,
        path: &str,
        content_type: &str,
        data: &[u8],
    ) -> Result<TestResponse> {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", content_type)
            .body(Body::from(data.to_vec()))?;

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

    /// Send a Prometheus remote read request (compressed protobuf)
    pub async fn post_prometheus_read(
        &self,
        path: &str,
        compressed_data: &[u8],
    ) -> Result<TestResponse> {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/x-protobuf")
            .header("content-encoding", "snappy")
            .header("x-prometheus-remote-read-version", "0.1.0")
            .body(Body::from(compressed_data.to_vec()))?;

        let response = self.app.clone().oneshot(request).await?;
        Ok(TestResponse::new(response).await)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub async fn post_prometheus_write(
        &self,
        path: &str,
        compressed_data: &[u8],
    ) -> Result<TestResponse> {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/x-protobuf")
            .header("content-encoding", "snappy")
            .header("x-prometheus-remote-write-version", "0.1.0")
            .body(Body::from(compressed_data.to_vec()))?;

        let response = self.app.clone().oneshot(request).await?;
        Ok(TestResponse::new(response).await)
    }
}

/// Test response wrapper for easier assertions
pub struct TestResponse {
    status: StatusCode,
    headers: HeaderMap,
    body_bytes: Vec<u8>,
    body: String,
}

impl TestResponse {
    async fn new(response: axum::response::Response) -> Self {
        let status = response.status();
        let headers = response.headers().clone();
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap_or_default()
            .to_vec();
        let body = String::from_utf8_lossy(&body_bytes).to_string();

        Self {
            status,
            headers,
            body_bytes,
            body,
        }
    }

    /// Get response status
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Get response body as string
    pub fn body(&self) -> &str {
        &self.body
    }

    /// Get response body as bytes
    pub fn body_bytes(&self) -> &[u8] {
        &self.body_bytes
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

    /// Get response headers
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Assert specific header value
    pub fn assert_header(&self, name: &str, expected: &str) -> &Self {
        let actual = self
            .headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<missing>");
        assert_eq!(
            actual, expected,
            "Expected header '{}' to be '{}', but was '{}'",
            name, expected, actual
        );
        self
    }

    /// Assert content-type header
    pub fn assert_content_type(&self, expected: &str) -> &Self {
        self.assert_header("content-type", expected)
    }
}

// Removed the test_publish_handler function entirely as it's no longer needed
// The real server endpoints will be used directly through the shared router

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response_helpers() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());

        let response = TestResponse {
            status: StatusCode::OK,
            headers,
            body_bytes: b"test body".to_vec(),
            body: "test body".to_string(),
        };

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body(), "test body");
        assert_eq!(response.body_bytes(), b"test body");
        assert!(response.is_success());

        // Test header functionality
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/json"
        );
        response.assert_content_type("application/json");
    }
}
