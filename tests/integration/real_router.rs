//! The layers that protect the server, run through the real router (`build_router`): request
//! ids, the write concurrency limit, authentication order, the request timeout and the body
//! limit. A storage wrapper delays `publish` so that a write can be made to take time.

use crate::common::TestDb;
use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch::Batch;
use sensapp::datamodel::{Metric, SensAppDateTime, SensorData};
use sensapp::http::auth::AuthConfig;
use sensapp::http::metrics::HttpMetrics;
use sensapp::http::server::{RouterSettings, build_router};
use sensapp::http::state::HttpServerState;
use sensapp::storage::{LabelMatcher, ListSeriesResult, StorageInstance};
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

/// A storage that makes every write, and the removal of duplicates, take `publish_delay` before
/// it reaches the real one, or fail a write when `fail_publish` is set.
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
    async fn deduplicate_samples(&self) -> Result<u64> {
        tokio::time::sleep(self.publish_delay).await;
        self.inner.deduplicate_samples().await
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
        write_timeout: Duration::from_secs(300),
        maintenance_timeout: Duration::from_secs(3600),
        max_concurrent_writes: 16,
        ui_dir: None,
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
        max_query_samples: sensapp::http::limits::DEFAULT_MAX_QUERY_SAMPLES,
        auth,
    };
    Ok((test_db, build_router(state, &settings)))
}

fn token(scope: &str) -> String {
    token_for_sensors(scope, None)
}

/// A token made the way `sensapp generate-token` makes it.
fn token_for_sensors(scope: &str, sensors: Option<Vec<String>>) -> String {
    AuthConfig::from_secret(SECRET)
        .expect("secret")
        .issue_token("test", scope, 3600, sensors)
        .expect("token")
        .token
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

    // The rejection is counted, and told apart from the other 503s by its own counter
    let (_, _, metrics) = send(&router, get_request("/prometheus/metrics")).await;
    assert!(
        metrics.contains("sensapp_http_writes_shed_total 1"),
        "{metrics}"
    );
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
        write_timeout: Duration::from_millis(200),
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
        write_timeout: Duration::from_millis(200),
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

fn vacuum_request(token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/v1/admin/vacuum");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

#[tokio::test]
#[serial]
async fn vacuum_needs_the_delete_scope_and_no_sensor_allow_list() -> Result<()> {
    let auth = Some(AuthConfig::from_secret(SECRET)?);
    let (_db, router) = router(Duration::ZERO, auth, settings()).await?;

    let (status, _, _) = send(&router, vacuum_request(None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // It removes rows: reading and writing are not enough
    for scope in ["read", "write", "read write"] {
        let (status, _, _) = send(&router, vacuum_request(Some(&token(scope)))).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "scope {scope}");
    }
    // A sensor-scoped token cannot run a database-wide operation
    let scoped = token_for_sensors("delete", Some(vec!["only_this".to_string()]));
    let (status, _, _) = send(&router, vacuum_request(Some(&scoped))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, _, body) = send(&router, vacuum_request(Some(&token("delete")))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: serde_json::Value = serde_json::from_str(&body)?;
    assert_eq!(json["status"], "ok");
    assert!(json.get("duplicates_removed").is_some(), "{body}");
    Ok(())
}

#[tokio::test]
#[serial]
async fn vacuum_removes_the_duplicates_of_a_retried_write() -> Result<()> {
    let (_db, router) = router(Duration::ZERO, None, settings()).await?;
    let run = uuid::Uuid::new_v4();
    let line = format!("vacuum_{run} value=1.5 1700000000000000000");
    let write = || {
        Request::builder()
            .method("POST")
            .uri("/api/v2/write?bucket=b&org=o")
            .body(Body::from(line.clone()))
            .unwrap()
    };

    // The same write, sent twice (a client that retried after a timeout)
    for _ in 0..2 {
        let (status, _, _) = send(&router, write()).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }
    let selector =
        urlencoding::encode(&format!("{{__name__=\"vacuum_{run} value\"}}[2000d]")).into_owned();
    // The query answers with the records of the series: one per sample
    let samples = |body: &str| -> usize {
        let json: serde_json::Value = serde_json::from_str(body).expect("json");
        json.as_array().expect("an array of records").len()
    };
    let (_, _, before) = send(
        &router,
        get_request(&format!("/api/v1/query?query={selector}")),
    )
    .await;

    let (status, _, body) = send(&router, vacuum_request(None)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: serde_json::Value = serde_json::from_str(&body)?;
    let (_, _, after) = send(
        &router,
        get_request(&format!("/api/v1/query?query={selector}")),
    )
    .await;

    assert_eq!(samples(&before), 2, "both writes are stored: {before}");
    match json["duplicates_removed"].as_u64() {
        // The backend removes duplicates: one is gone
        Some(removed) => {
            assert!(removed >= 1, "{body}");
            assert_eq!(samples(&after), 1, "{after}");
        }
        // The backend cannot: the answer says so with null and nothing changes
        None => assert_eq!(samples(&after), 2),
    }
    Ok(())
}

/// A large write is slower than a read, so it has a timeout of its own: the one of the other
/// requests does not apply to it, and its own bounds it.
#[tokio::test]
#[serial]
async fn writes_have_a_timeout_of_their_own() -> Result<()> {
    // Slower than the timeout of the other requests, faster than the one of the writes
    let settings = RouterSettings {
        request_timeout: Duration::from_millis(200),
        write_timeout: Duration::from_secs(10),
        ..settings()
    };
    let (_db, router) = router(Duration::from_millis(600), None, settings).await?;
    let (status, _, body) = send(&router, write_request(None)).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");

    // And the writes are not unbounded
    let settings = RouterSettings {
        request_timeout: Duration::from_secs(30),
        write_timeout: Duration::from_millis(200),
        ..self::settings()
    };
    let (_db, router) = self::router(Duration::from_secs(2), None, settings).await?;
    let started = std::time::Instant::now();
    let (status, _, _) = send(&router, write_request(None)).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert!(started.elapsed() < Duration::from_secs(1));
    // The other requests keep their own timeout
    let (status, _, _) = send(&router, get_request("/health/live")).await;
    assert_eq!(status, StatusCode::OK);
    Ok(())
}

#[tokio::test]
#[serial]
async fn the_vacuum_has_a_timeout_of_its_own() -> Result<()> {
    // Slower than the timeout of the other requests, faster than its own
    let settings = RouterSettings {
        request_timeout: Duration::from_millis(200),
        write_timeout: Duration::from_millis(200),
        maintenance_timeout: Duration::from_secs(10),
        ..settings()
    };
    let (_db, router) = router(Duration::from_millis(600), None, settings).await?;

    let (status, _, body) = send(&router, vacuum_request(None)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // The other requests keep the standard timeout
    let (status, _, _) = send(&router, write_request(None)).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);

    // And the vacuum is not unbounded
    let settings = RouterSettings {
        request_timeout: Duration::from_secs(30),
        maintenance_timeout: Duration::from_millis(200),
        ..self::settings()
    };
    let (_db, router) = self::router(Duration::from_secs(2), None, settings).await?;
    let (status, _, _) = send(&router, vacuum_request(None)).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    Ok(())
}

/// The UI files are public, they hold no data. Its API calls need the token as any other client.
#[tokio::test]
#[serial]
async fn the_ui_is_public_and_its_data_is_not() -> Result<()> {
    let dist = std::env::temp_dir().join(format!("sensapp-ui-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dist)?;
    std::fs::write(dist.join("index.html"), "<html>SensApp UI</html>")?;
    let settings = RouterSettings {
        ui_dir: Some(dist.clone()),
        ..settings()
    };
    let auth = AuthConfig::from_secret(SECRET)?;
    let (_db, router) = router(Duration::ZERO, Some(auth), settings).await?;

    let (status, headers, _) = send(&router, get_request("/")).await;
    assert_eq!(status, StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(headers["location"], "/ui/");

    let (status, _, body) = send(&router, get_request("/ui/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "<html>SensApp UI</html>");

    // The catalog behind it answers 401 without a token, which is what makes the UI ask for one
    let (status, _, body) = send(&router, get_request("/metrics")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(body.contains("Authorization"), "{body}");
    let request = Request::builder()
        .uri("/metrics")
        .header("authorization", format!("Bearer {}", token("read")))
        .body(Body::empty())?;
    let (status, _, _) = send(&router, request).await;
    assert_eq!(status, StatusCode::OK);

    std::fs::remove_dir_all(dist)?;
    Ok(())
}

/// Without the UI, `/` is the name of the instance as before.
#[tokio::test]
#[serial]
async fn the_root_is_the_name_of_the_instance_without_the_ui() -> Result<()> {
    let (_db, router) = router(Duration::ZERO, None, settings()).await?;
    let (status, _, body) = send(&router, get_request("/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "\"SensApp Test\"");
    let (status, _, _) = send(&router, get_request("/ui/")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

// ---------------------------------------------------------------------------
// Making tokens: POST /api/v1/admin/tokens
// ---------------------------------------------------------------------------

fn create_token_request(token: Option<&str>, body: serde_json::Value) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/v1/admin/tokens")
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

fn bearer_get(path: &str, token: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

fn token_wish(subject: &str, scope: &[&str], duration_seconds: u64) -> serde_json::Value {
    serde_json::json!({
        "subject": subject,
        "scope": scope,
        "duration_seconds": duration_seconds,
    })
}

#[tokio::test]
#[serial]
async fn an_admin_makes_tokens_that_do_what_they_were_made_for() -> Result<()> {
    let auth = Some(AuthConfig::from_secret(SECRET)?);
    let (_db, router) = router(Duration::ZERO, auth, settings()).await?;
    let admin = token("admin");

    let (status, headers, body) = send(
        &router,
        create_token_request(Some(&admin), token_wish("dashboard", &["read"], 3600)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // A token does not stay in a cache
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");

    let created: serde_json::Value = serde_json::from_str(&body)?;
    assert_eq!(created["subject"], "dashboard");
    assert_eq!(created["scope"], serde_json::json!(["read"]));
    assert!(created["sensors"].is_null());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let expires_at = created["expires_at"].as_u64().unwrap();
    assert!(
        expires_at.abs_diff(now + 3600) < 10,
        "{expires_at} vs {now}"
    );
    let made = created["token"].as_str().unwrap();

    // The token reads, and does not write or make tokens
    let (status, _, _) = send(&router, bearer_get("/metrics", made)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = send(&router, write_request(Some(made))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _, _) = send(
        &router,
        create_token_request(Some(made), token_wish("more", &["read"], 60)),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // The id of the answer is the one in the token, which the logs of its requests carry
    let validated = sensapp::http::auth::validate_token(
        &{
            let mut headers = HeaderMap::new();
            headers.insert("authorization", format!("Bearer {made}").parse()?);
            headers
        },
        &AuthConfig::from_secret(SECRET)?,
    )
    .expect("the made token validates");
    assert_eq!(validated.token_id.as_deref(), created["jti"].as_str());

    // The sensors of the request limit the token
    let mut wish = token_wish("one-sensor", &["read", "write"], 60);
    wish["sensors"] = serde_json::json!(["only,this", "only,this"]);
    let (status, _, body) = send(&router, create_token_request(Some(&admin), wish)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let created: serde_json::Value = serde_json::from_str(&body)?;
    // A comma is part of a name, and a repeated name is kept once
    assert_eq!(created["sensors"], serde_json::json!(["only,this"]));
    assert_eq!(created["scope"], serde_json::json!(["read", "write"]));
    Ok(())
}

#[tokio::test]
#[serial]
async fn only_admin_tokens_make_tokens_and_admin_tokens_do_nothing_else() -> Result<()> {
    let auth = Some(AuthConfig::from_secret(SECRET)?);
    let (_db, router) = router(Duration::ZERO, auth, settings()).await?;
    let wish = || token_wish("someone", &["read"], 60);

    let (status, _, _) = send(&router, create_token_request(None, wish())).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    for scope in ["read", "write", "delete", "read write delete"] {
        let (status, _, _) = send(&router, create_token_request(Some(&token(scope)), wish())).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "scope {scope}");
    }

    // Admin is not read, write or delete either
    let admin = token("admin");
    let (status, _, _) = send(&router, bearer_get("/metrics", &admin)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _, _) = send(&router, write_request(Some(&admin))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _, _) = send(&router, vacuum_request(Some(&admin))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    Ok(())
}

#[tokio::test]
#[serial]
async fn the_endpoint_checks_the_request() -> Result<()> {
    let auth = Some(AuthConfig::from_secret(SECRET)?.with_max_token_duration(3600));
    let (_db, router) = router(Duration::ZERO, auth, settings()).await?;
    let admin = token("admin");
    let ask = |wish: serde_json::Value| {
        let router = router.clone();
        let admin = admin.clone();
        async move {
            let (status, _, body) = send(&router, create_token_request(Some(&admin), wish)).await;
            (status, body)
        }
    };

    // An admin token is never made here, even among others: it takes the secret
    for scope in [&["admin"][..], &["read", "admin"][..]] {
        let (status, body) = ask(token_wish("x", scope, 60)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    }

    let invalid = [
        token_wish("", &["read"], 60),
        token_wish("   ", &["read"], 60),
        token_wish("x", &[], 60),
        token_wish("x", &["root"], 60),
        token_wish("x", &["read"], 0),
        // The configured maximum
        token_wish("x", &["read"], 3601),
        token_wish("x", &["read"], u64::MAX),
        {
            let mut wish = token_wish("x", &["read"], 60);
            wish["sensors"] = serde_json::json!([]);
            wish
        },
        {
            let mut wish = token_wish("x", &["read"], 60);
            wish["sensors"] = serde_json::json!([""]);
            wish
        },
    ];
    for wish in invalid {
        let (status, body) = ask(wish.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{wish} gave {body}");
    }
    let (status, body) = ask(token_wish("x", &["read"], 3600)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Not JSON of the right shape
    let (status, _, _) = send(
        &router,
        create_token_request(Some(&admin), serde_json::json!({"subject": "x"})),
    )
    .await;
    assert!(status.is_client_error());
    Ok(())
}

#[tokio::test]
#[serial]
async fn without_authentication_there_is_nothing_to_sign_with() -> Result<()> {
    let (_db, router) = router(Duration::ZERO, None, settings()).await?;
    let (status, _, body) = send(
        &router,
        create_token_request(None, token_wish("x", &["read"], 60)),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("disabled"), "{body}");
    Ok(())
}

#[tokio::test]
#[serial]
async fn creating_a_token_is_logged_without_the_token() -> Result<()> {
    let auth = Some(AuthConfig::from_secret(SECRET)?);
    let (_db, router) = router(Duration::ZERO, auth, settings()).await?;
    let (logs, _guard) = capture_logs();

    let (status, _, body) = send(
        &router,
        create_token_request(Some(&token("admin")), token_wish("audited", &["write"], 60)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let created: serde_json::Value = serde_json::from_str(&body)?;
    let made = created["token"].as_str().unwrap();
    let jti = created["jti"].as_str().unwrap();

    let lines = logs.lines();
    let line = lines
        .iter()
        .find(|line| line.contains("token created"))
        .unwrap_or_else(|| panic!("no log of the creation in {lines:#?}"));
    // Who asked, for whom, with what, and which token
    assert!(
        line.contains("creator=\"test\"") || line.contains("creator=Some(\"test\")"),
        "{line}"
    );
    assert!(line.contains("audited"), "{line}");
    assert!(line.contains(jti), "{line}");
    // The request span carries who asked
    assert!(line.contains("subject=\"test\""), "{line}");
    // No line shows the token, nor the header that carried the admin one
    for line in &lines {
        assert!(!line.contains(made), "{line}");
    }
    Ok(())
}
