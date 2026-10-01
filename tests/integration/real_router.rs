//! The layers that protect the server, run through the real router (`build_router`): request
//! ids, the write concurrency limit, authentication order, the request timeout and the body
//! limit. A storage wrapper delays `publish` so that a write can be made to take time.

use crate::common::TestDb;
use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use jsonwebtoken::{EncodingKey, Header, encode};
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch::Batch;
use sensapp::datamodel::{Metric, SensAppDateTime, SensorData};
use sensapp::http::auth::AuthConfig;
use sensapp::http::metrics::HttpMetrics;
use sensapp::http::server::{RouterSettings, build_router};
use sensapp::http::state::HttpServerState;
use sensapp::storage::{LabelMatcher, ListSeriesResult, StorageInstance};
use serde::Serialize;
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

const SECRET: &str = "test-secret-0123456789abcdef012345";

/// A storage that makes every write take `publish_delay` before it reaches the real one, or
/// fail when `fail_publish` is set.
#[derive(Debug)]
struct SlowStorage {
    inner: Arc<dyn StorageInstance>,
    publish_delay: Duration,
    fail_publish: bool,
}

#[async_trait]
impl StorageInstance for SlowStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        self.inner.create_or_migrate().await
    }
    async fn publish(&self, batch: Arc<Batch>) -> Result<()> {
        tokio::time::sleep(self.publish_delay).await;
        if self.fail_publish {
            anyhow::bail!("the write failed for a reason of its own");
        }
        self.inner.publish(batch).await
    }
    async fn vacuum(&self) -> Result<()> {
        self.inner.vacuum().await
    }
    async fn delete_series(&self, sensor_uuid: &str) -> Result<bool> {
        self.inner.delete_series(sensor_uuid).await
    }
    async fn delete_series_samples(
        &self,
        sensor_uuid: &str,
        start_time: SensAppDateTime,
        end_time: SensAppDateTime,
    ) -> Result<Option<u64>> {
        self.inner
            .delete_series_samples(sensor_uuid, start_time, end_time)
            .await
    }
    async fn list_series(
        &self,
        metric_filter: Option<&str>,
        limit: Option<usize>,
        bookmark: Option<&str>,
    ) -> Result<ListSeriesResult> {
        self.inner.list_series(metric_filter, limit, bookmark).await
    }
    async fn list_metrics(&self) -> Result<Vec<Metric>> {
        self.inner.list_metrics().await
    }
    async fn query_sensor_data(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
    ) -> Result<Option<SensorData>> {
        self.inner
            .query_sensor_data(sensor_uuid, start_time, end_time, limit)
            .await
    }
    async fn query_sensors_by_labels(
        &self,
        matchers: &[LabelMatcher],
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
        numeric_only: bool,
    ) -> Result<Vec<SensorData>> {
        self.inner
            .query_sensors_by_labels(matchers, start_time, end_time, limit, numeric_only)
            .await
    }
    async fn health_check(&self) -> Result<()> {
        self.inner.health_check().await
    }
    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        self.inner.cleanup_test_data().await
    }
}

fn settings() -> RouterSettings {
    RouterSettings {
        max_body_bytes: 64 * 1024 * 1024,
        request_timeout: Duration::from_secs(30),
        max_concurrent_writes: 16,
    }
}

async fn router(
    publish_delay: Duration,
    auth: Option<AuthConfig>,
    settings: RouterSettings,
) -> Result<(TestDb, Router)> {
    router_with(publish_delay, false, auth, settings).await
}

async fn router_with(
    publish_delay: Duration,
    fail: bool,
    auth: Option<AuthConfig>,
    settings: RouterSettings,
) -> Result<(TestDb, Router)> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage: Arc<dyn StorageInstance> = Arc::new(SlowStorage {
        inner: test_db.storage(),
        publish_delay,
        fail_publish: fail,
    });
    let state = HttpServerState {
        name: Arc::new("SensApp Test".to_string()),
        storage,
        metrics: Arc::new(HttpMetrics::new()),
        influxdb_with_numeric: false,
        auth,
    };
    Ok((test_db, build_router(state, &settings)))
}

#[derive(Serialize)]
struct Claims {
    sub: String,
    exp: u64,
    scope: String,
}

fn token(scope: &str) -> String {
    encode(
        &Header::default(),
        &Claims {
            sub: "test".into(),
            exp: 4_102_444_800,
            scope: scope.into(),
        },
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .expect("token")
}

fn write_request(token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/v2/write?bucket=b&org=o")
        .header("content-type", "text/plain");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder
        .body(Body::from("real_router,k=v value=1.5 1700000000000000000"))
        .unwrap()
}

fn get_request(path: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(path)
        .body(Body::empty())
        .unwrap()
}

async fn send(router: &Router, request: Request<Body>) -> (StatusCode, HeaderMap, String) {
    let response = router.clone().oneshot(request).await.unwrap();
    let (parts, body) = response.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    (
        parts.status,
        parts.headers,
        String::from_utf8_lossy(&body).to_string(),
    )
}

/// Start a write and give it time to take its slot.
async fn start_slow_write(
    router: &Router,
    token: Option<&str>,
) -> tokio::task::JoinHandle<StatusCode> {
    let router = router.clone();
    let request = write_request(token);
    let handle = tokio::spawn(async move { send(&router, request).await.0 });
    tokio::time::sleep(Duration::from_millis(150)).await;
    handle
}

#[tokio::test]
#[serial]
async fn request_ids_are_generated_kept_and_present_on_errors() -> Result<()> {
    let (_db, router) = router(Duration::ZERO, None, settings()).await?;

    let (_, headers, _) = send(&router, get_request("/health/live")).await;
    let generated = headers["x-request-id"].to_str()?;
    assert!(uuid::Uuid::parse_str(generated).is_ok(), "{generated}");

    let mut request = get_request("/health/live");
    request
        .headers_mut()
        .insert("x-request-id", "my-id-42".parse()?);
    let (_, headers, _) = send(&router, request).await;
    assert_eq!(headers["x-request-id"], "my-id-42");

    let (status, headers, _) = send(&router, get_request("/no/such/route")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(headers.contains_key("x-request-id"));
    Ok(())
}

#[tokio::test]
#[serial]
async fn the_write_limit_sheds_writes_and_spares_everything_else() -> Result<()> {
    let settings = RouterSettings {
        max_concurrent_writes: 1,
        ..settings()
    };
    let (_db, router) = router(Duration::from_millis(600), None, settings).await?;

    let first = start_slow_write(&router, None).await;

    // The only slot is taken: the next write is turned away at once, with a hint and an id
    let started = std::time::Instant::now();
    let (status, headers, _) = send(&router, write_request(None)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "no queueing"
    );
    let retry_after: u64 = headers["retry-after"].to_str()?.parse()?;
    assert!((1..=30).contains(&retry_after), "{retry_after}");
    assert!(headers.contains_key("x-request-id"));

    // Reads, health and metrics are not limited
    for path in [
        "/health/live",
        "/health/ready",
        "/series",
        "/prometheus/metrics",
    ] {
        let (status, _, _) = send(&router, get_request(path)).await;
        assert_eq!(status, StatusCode::OK, "{path}");
    }

    assert_eq!(first.await?, StatusCode::NO_CONTENT);
    // The slot is free again
    let (status, _, _) = send(&router, write_request(None)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // The rejection is counted
    let (_, _, metrics) = send(&router, get_request("/prometheus/metrics")).await;
    assert!(
        metrics.contains(
            "sensapp_http_requests_total{method=\"POST\",path=\"/api/v2/write\",status=\"503\"} 1"
        ),
        "{metrics}"
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn the_write_limit_sits_behind_authentication() -> Result<()> {
    let settings = RouterSettings {
        max_concurrent_writes: 1,
        ..settings()
    };
    let auth = Some(AuthConfig::from_secret(SECRET)?);
    let (_db, router) = router(Duration::from_millis(600), auth, settings).await?;
    let writer = token("write");

    let first = start_slow_write(&router, Some(&writer)).await;

    // While the slot is taken: no token and a token without the write scope are refused
    // as such, they do not turn into a 503 and do not wait for a slot
    let (status, _, _) = send(&router, write_request(None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _, _) = send(&router, write_request(Some(&token("read")))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // An authorised writer is the one that is told to come back later
    let (status, headers, _) = send(&router, write_request(Some(&writer))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(headers.contains_key("retry-after"));

    assert_eq!(first.await?, StatusCode::NO_CONTENT);
    Ok(())
}

#[tokio::test]
#[serial]
async fn slow_requests_get_a_504() -> Result<()> {
    let settings = RouterSettings {
        request_timeout: Duration::from_millis(200),
        ..settings()
    };
    let (_db, router) = router(Duration::from_secs(2), None, settings).await?;

    let started = std::time::Instant::now();
    let (status, headers, _) = send(&router, write_request(None)).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(headers.contains_key("x-request-id"));

    // A request that is fast enough is not affected
    let (status, _, _) = send(&router, get_request("/health/live")).await;
    assert_eq!(status, StatusCode::OK);
    Ok(())
}

#[tokio::test]
#[serial]
async fn bodies_over_the_limit_get_a_413() -> Result<()> {
    let settings = RouterSettings {
        max_body_bytes: 64,
        ..settings()
    };
    let (_db, router) = router(Duration::ZERO, None, settings).await?;

    let request = Request::builder()
        .method("POST")
        .uri("/api/v2/write?bucket=b&org=o")
        .body(Body::from("x".repeat(1000)))?;
    let (status, _, _) = send(&router, request).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);

    // Within the limit
    let (status, _, _) = send(&router, write_request(None)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    Ok(())
}

/// What the tracing events of a test wrote, one string per line.
#[derive(Clone, Default)]
struct LogBuffer(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl LogBuffer {
    fn lines(&self) -> Vec<String> {
        String::from_utf8_lossy(&self.0.lock().unwrap())
            .lines()
            .map(str::to_string)
            .collect()
    }
}

/// Route the tracing events of the current thread (these tests run on one thread) to a buffer.
fn capture_logs() -> (LogBuffer, tracing::subscriber::DefaultGuard) {
    let buffer = LogBuffer::default();
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .finish();
    (buffer, tracing::subscriber::set_default(subscriber))
}

#[tokio::test]
#[serial]
async fn load_shedding_is_not_logged_as_an_error() -> Result<()> {
    let settings = RouterSettings {
        max_concurrent_writes: 1,
        ..settings()
    };
    let (_db, router) = router(Duration::from_millis(600), None, settings).await?;
    let (logs, _guard) = capture_logs();

    let first = start_slow_write(&router, None).await;
    for _ in 0..5 {
        let (status, _, _) = send(&router, write_request(None)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }
    assert_eq!(first.await?, StatusCode::NO_CONTENT);

    let errors: Vec<String> = logs
        .lines()
        .into_iter()
        .filter(|line| line.contains(" ERROR "))
        .collect();
    assert!(
        errors.is_empty(),
        "load shedding must not log errors: {errors:#?}"
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn timeouts_are_logged_as_warnings_with_the_request_id() -> Result<()> {
    let settings = RouterSettings {
        request_timeout: Duration::from_millis(200),
        ..settings()
    };
    let (_db, router) = router(Duration::from_secs(2), None, settings).await?;
    let (logs, _guard) = capture_logs();

    let mut request = write_request(None);
    request
        .headers_mut()
        .insert("x-request-id", "timeout-id-7".parse()?);
    let (status, _, _) = send(&router, request).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);

    let lines = logs.lines();
    assert!(
        lines
            .iter()
            .any(|line| line.contains(" WARN ") && line.contains("timeout-id-7")),
        "a warning with the request id was expected: {lines:#?}"
    );
    assert!(
        !lines.iter().any(|line| line.contains(" ERROR ")),
        "{lines:#?}"
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn real_server_errors_are_still_logged_as_errors() -> Result<()> {
    let (_db, router) = router_with(Duration::ZERO, true, None, settings()).await?;
    let (logs, _guard) = capture_logs();

    let mut request = write_request(None);
    request
        .headers_mut()
        .insert("x-request-id", "failing-id-9".parse()?);
    let (status, _, _) = send(&router, request).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let lines = logs.lines();
    assert!(
        lines
            .iter()
            .any(|line| line.contains(" ERROR ") && line.contains("failing-id-9")),
        "an error with the request id was expected: {lines:#?}"
    );
    Ok(())
}
