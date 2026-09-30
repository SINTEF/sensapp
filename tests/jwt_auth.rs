//! Integration tests for optional JWT authentication.
//!
//! These tests verify that:
//! - Without auth configured (`auth: None`), all endpoints are open.
//! - With auth configured, read endpoints require a "read" scope token.
//! - With auth configured, write endpoints require a "write" scope token.
//! - With auth configured, delete endpoints require an explicit "delete" scope token.
//! - Expired and not-yet-valid tokens are rejected.
//! - Sensor allow lists are enforced.
//! - Health/docs/prometheus-metrics remain public even with auth enabled.

mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use common::TestDb;
use jsonwebtoken::{EncodingKey, Header, encode};
use sensapp::http::auth::AuthConfig;
use sensapp::http::crud::{
    delete_series, delete_series_samples, get_series_data, list_metrics, list_series,
};
use sensapp::http::health::{liveness, readiness};
use sensapp::http::metrics::{HttpMetrics, prometheus_metrics};
use sensapp::http::server::publish_senml_data;
use sensapp::http::simple_promql::simple_promql_query;
use sensapp::http::state::HttpServerState;
use sensapp::storage::StorageInstance;
use serde::Serialize;
use serial_test::serial;
use std::sync::Arc;
use tower::ServiceExt;

const TEST_SECRET: &str = "test-secret-0123456789abcdef012345";

// ---------------------------------------------------------------------------
// Test claims helper
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct TestClaims {
    sub: String,
    exp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    nbf: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    iat: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sensors: Option<Vec<String>>,
}

fn make_token(claims: &TestClaims) -> String {
    encode(
        &Header::default(),
        claims,
        &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
    )
    .expect("token encoding should succeed")
}

fn read_write_token() -> String {
    make_token(&TestClaims {
        sub: "test-user".into(),
        exp: 4_102_444_800, // year 2100
        nbf: Some(0),
        iat: Some(0),
        scope: Some("read write".into()),
        sensors: None,
    })
}

fn sensor_token(scope: &str, sensors: &[&str]) -> String {
    make_token(&TestClaims {
        sub: "sensor-client".into(),
        exp: 4_102_444_800,
        nbf: None,
        iat: None,
        scope: Some(scope.into()),
        sensors: Some(sensors.iter().map(|sensor| (*sensor).into()).collect()),
    })
}

fn read_only_token() -> String {
    make_token(&TestClaims {
        sub: "reader".into(),
        exp: 4_102_444_800,
        nbf: Some(0),
        iat: Some(0),
        scope: Some("read".into()),
        sensors: None,
    })
}

fn write_only_token() -> String {
    make_token(&TestClaims {
        sub: "writer".into(),
        exp: 4_102_444_800,
        nbf: Some(0),
        iat: Some(0),
        scope: Some("write".into()),
        sensors: None,
    })
}

fn expired_token() -> String {
    make_token(&TestClaims {
        sub: "old".into(),
        exp: 0, // already expired
        nbf: None,
        iat: None,
        scope: Some("read write".into()),
        sensors: None,
    })
}

fn not_yet_valid_token() -> String {
    make_token(&TestClaims {
        sub: "early".into(),
        exp: 4_102_444_800,
        nbf: Some(4_102_444_800), // not valid until year 2100
        iat: None,
        scope: Some("read write".into()),
        sensors: None,
    })
}

// ---------------------------------------------------------------------------
// Router builder – mirrors the real server's route groups
// ---------------------------------------------------------------------------

/// Unified test publish handler
async fn test_publish_handler(
    axum::extract::State(state): axum::extract::State<HttpServerState>,
    access: Option<axum::Extension<sensapp::http::auth::AccessContext>>,
    headers: axum::http::HeaderMap,
    body: Body,
) -> Result<String, (StatusCode, String)> {
    let state = state.with_access(access.map(|extension| extension.0));
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("text/csv");

    if content_type.contains("application/json") {
        let body_bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("body: {e}")))?;
        let json_str = String::from_utf8(body_bytes.to_vec())
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("utf8: {e}")))?;
        publish_senml_data(&json_str, state.storage.clone())
            .await
            .map_err(|e| {
                let message = format!("{e:?}");
                (e.into_response().status(), message)
            })?;
    }
    Ok("ok".to_string())
}

/// Build a test router with the same auth layering as the real server.
fn build_test_router(storage: Arc<dyn StorageInstance>, auth: Option<AuthConfig>) -> Router {
    use sensapp::http::auth::{require_delete_auth, require_read_auth, require_write_auth};

    let state = HttpServerState {
        name: Arc::new("SensApp Auth Test".into()),
        storage,
        metrics: Arc::new(HttpMetrics::new()),
        influxdb_with_numeric: false,
        auth: auth.clone(),
    };

    let public = Router::new()
        .route("/", get(|| async { "SensApp" }))
        .route("/prometheus/metrics", get(prometheus_metrics))
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness));

    let read_routes = Router::new()
        .route("/metrics", get(list_metrics))
        .route("/series", get(list_series))
        .route("/series/{series_uuid}", get(get_series_data))
        .route("/api/v1/query", get(simple_promql_query))
        .route_layer(axum::middleware::from_fn_with_state(
            auth.clone(),
            require_read_auth,
        ));

    let write_routes = Router::new()
        .route("/publish", post(test_publish_handler))
        .route_layer(axum::middleware::from_fn_with_state(
            auth.clone(),
            require_write_auth,
        ));

    let delete_routes = Router::new()
        .route("/series/{series_uuid}", delete(delete_series))
        .route(
            "/series/{series_uuid}/samples",
            delete(delete_series_samples),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            auth,
            require_delete_auth,
        ));

    public
        .merge(read_routes)
        .merge(write_routes)
        .merge(delete_routes)
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Tests: no auth configured (everything open)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn no_auth_read_endpoint_is_open() {
    let db = TestDb::new().await.expect("test db");
    let app = build_test_router(db.storage(), None);

    let resp = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    // Should succeed (200) or at least not be 401/403
    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
#[serial]
async fn no_auth_write_endpoint_is_open() {
    let db = TestDb::new().await.expect("test db");
    let app = build_test_router(db.storage(), None);

    let resp = app
        .oneshot(
            Request::post("/publish")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"[{"n":"temperature","u":"Cel","v":22.5,"t":1700000000}]"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(resp.status(), StatusCode::FORBIDDEN);
}

// ---------------------------------------------------------------------------
// Tests: auth configured — valid tokens
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn auth_read_endpoint_with_valid_token() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(
            Request::get("/metrics")
                .header("authorization", format!("Bearer {}", read_write_token()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
#[serial]
async fn auth_write_endpoint_with_valid_token() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(
            Request::post("/publish")
                .header("authorization", format!("Bearer {}", read_write_token()))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"[{"n":"temperature","u":"Cel","v":22.5,"t":1700000000}]"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(resp.status(), StatusCode::FORBIDDEN);
}

// ---------------------------------------------------------------------------
// Tests: auth configured — missing/invalid tokens
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn auth_read_endpoint_rejects_no_token() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial]
async fn auth_write_endpoint_rejects_no_token() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(
            Request::post("/publish")
                .header("content-type", "application/json")
                .body(Body::from("[]"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial]
async fn auth_rejects_expired_token() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(
            Request::get("/metrics")
                .header("authorization", format!("Bearer {}", expired_token()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial]
async fn auth_rejects_not_yet_valid_token() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(
            Request::get("/metrics")
                .header("authorization", format!("Bearer {}", not_yet_valid_token()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial]
async fn auth_rejects_wrong_secret() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    // Sign with a different secret
    let wrong_token = encode(
        &Header::default(),
        &TestClaims {
            sub: "hacker".into(),
            exp: 4_102_444_800,
            nbf: None,
            iat: None,
            scope: Some("read write".into()),
            sensors: None,
        },
        &EncodingKey::from_secret(b"wrong-secret-that-is-long-enough!!"),
    )
    .unwrap();

    let resp = app
        .oneshot(
            Request::get("/metrics")
                .header("authorization", format!("Bearer {wrong_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------------
// Tests: scope enforcement
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn read_only_token_can_read() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(
            Request::get("/metrics")
                .header("authorization", format!("Bearer {}", read_only_token()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
#[serial]
async fn read_only_token_cannot_write() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(
            Request::post("/publish")
                .header("authorization", format!("Bearer {}", read_only_token()))
                .header("content-type", "application/json")
                .body(Body::from("[]"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
#[serial]
async fn write_only_token_cannot_read() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(
            Request::get("/metrics")
                .header("authorization", format!("Bearer {}", write_only_token()))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
#[serial]
async fn write_only_token_can_write() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(
            Request::post("/publish")
                .header("authorization", format!("Bearer {}", write_only_token()))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"[{"n":"temperature","u":"Cel","v":22.5,"t":1700000000}]"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(resp.status(), StatusCode::FORBIDDEN);
}

// ---------------------------------------------------------------------------
// Tests: public endpoints stay open even with auth enabled
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn health_live_is_always_public() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(Request::get("/health/live").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn health_ready_is_always_public() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(Request::get("/health/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();

    // health/ready returns 200 or 503 depending on db health; never 401/403
    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
#[serial]
async fn prometheus_metrics_is_always_public() {
    let db = TestDb::new().await.expect("test db");
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(db.storage(), Some(auth));

    let resp = app
        .oneshot(
            Request::get("/prometheus/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(resp.status(), StatusCode::FORBIDDEN);
}

// ---------------------------------------------------------------------------
// Tests: token creation via AuthConfig
// ---------------------------------------------------------------------------

#[test]
fn auth_config_create_token_roundtrip() {
    let config = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let token = config
        .create_token("my-service", "read write", 3600, None)
        .expect("token creation");

    // Validate it by decoding
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {token}").parse().unwrap(),
    );

    let access = sensapp::http::auth::validate_token(&headers, &config).expect("should validate");
    assert_eq!(access.subject, "my-service");
    assert!(access.can_read);
    assert!(access.can_write);
}

#[test]
fn auth_config_create_token_with_sensor_filter() {
    let config = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let token = config
        .create_token(
            "sensor-reader",
            "read",
            3600,
            Some(vec!["cpu_temp".to_string(), "fan_speed".to_string()]),
        )
        .expect("token creation");

    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {token}").parse().unwrap(),
    );

    let access = sensapp::http::auth::validate_token(&headers, &config).expect("should validate");
    assert!(access.can_read);
    assert!(!access.can_write);
    assert!(access.can_access_sensor("cpu_temp"));
    assert!(access.can_access_sensor("fan_speed"));
    assert!(!access.can_access_sensor("humidity"));
}

#[test]
fn secret_too_short_is_rejected() {
    let result = AuthConfig::from_secret("short");
    assert!(result.is_err());
}

#[test]
fn malformed_bearer_prefix_rejected() {
    let config = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        "Token some-garbage".parse().unwrap(),
    );
    assert!(sensapp::http::auth::validate_token(&headers, &config).is_err());
}

#[tokio::test]
#[serial]
async fn sensor_allow_list_controls_writes_and_reads() {
    sensapp::config::load_configuration_for_tests().unwrap();
    let db = TestDb::new().await.expect("test db");
    let storage = db.storage();
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(storage.clone(), Some(auth));
    let token = sensor_token("read write", &["temperature"]);

    let allowed_write = app
        .clone()
        .oneshot(
            Request::post("/publish")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"[{"n":"temperature","v":21.5,"t":1700000000}]"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let allowed_status = allowed_write.status();
    let allowed_body = axum::body::to_bytes(allowed_write.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        allowed_status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&allowed_body)
    );

    let denied_write = app
        .clone()
        .oneshot(
            Request::post("/publish")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"[{"n":"humidity","v":55.0,"t":1700000000}]"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied_write.status(), StatusCode::FORBIDDEN);

    publish_senml_data(
        r#"[{"n":"humidity","v":55.0,"t":1700000000}]"#,
        storage.clone(),
    )
    .await
    .unwrap();

    let humidity_uuid = storage
        .list_series(Some("humidity"), None, None)
        .await
        .unwrap()
        .series[0]
        .uuid;

    for path in ["/metrics", "/series", "/api/v1/query?query=humidity"] {
        let response = app
            .clone()
            .oneshot(
                Request::get(path)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(
            !String::from_utf8_lossy(&bytes).contains("humidity"),
            "{path}"
        );
    }

    let denied_series = app
        .clone()
        .oneshot(
            Request::get(format!("/series/{humidity_uuid}"))
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied_series.status(), StatusCode::NOT_FOUND);

    let public_samples = app
        .clone()
        .oneshot(
            Request::get("/prometheus/metrics?include_latest_samples=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(public_samples.status(), StatusCode::UNAUTHORIZED);

    let scoped_samples = app
        .oneshot(
            Request::get("/prometheus/metrics?include_latest_samples=true")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(scoped_samples.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(scoped_samples.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8_lossy(&bytes);
    assert!(body.contains("temperature"));
    assert!(!body.contains("humidity"));
}

/// Cross-series aggregation must only combine the series the token may read.
#[tokio::test]
#[serial]
async fn sensor_allow_list_limits_cross_series_aggregation() {
    sensapp::config::load_configuration_for_tests().unwrap();
    let db = TestDb::new().await.expect("test db");
    let storage = db.storage();
    let auth = AuthConfig::from_secret(TEST_SECRET).unwrap();
    let app = build_test_router(storage.clone(), Some(auth));

    // The default query window is the last hour, so the samples must be recent
    let recent = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 60;
    publish_senml_data(
        &format!(
            r#"[{{"n":"temperature","v":21.5,"t":{recent}}},{{"n":"humidity","v":55.0,"t":{recent}}}]"#
        ),
        storage.clone(),
    )
    .await
    .unwrap();

    let query = urlencoding::encode(r#"sum({__name__=~"temperature|humidity"})"#).into_owned();
    let aggregate = |token: String, expression: String| {
        let app = app.clone();
        async move {
            let response = app
                .oneshot(
                    Request::get(format!("/api/v1/query?format=jsonl&query={expression}"))
                        .header("authorization", format!("Bearer {token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            String::from_utf8_lossy(&bytes)
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .collect::<Vec<_>>()
        }
    };

    // Unrestricted token: both sensors are combined
    let all = aggregate(read_write_token(), query.clone()).await;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0]["value"].as_f64(), Some(76.5));

    // Restricted token: humidity is invisible, so it is not part of the sum
    let scoped = aggregate(sensor_token("read", &["temperature"]), query.clone()).await;
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0]["value"].as_f64(), Some(21.5));

    // The count is scoped as well
    let count_query =
        urlencoding::encode(r#"count({__name__=~"temperature|humidity"})"#).into_owned();
    let scoped_count = aggregate(sensor_token("read", &["temperature"]), count_query).await;
    assert_eq!(scoped_count[0]["value"].as_f64(), Some(1.0));

    // A token allowed on no matching sensor sees nothing, not a zero
    let nothing = aggregate(sensor_token("read", &["pressure"]), query).await;
    assert!(nothing.is_empty());
}

// ---------------------------------------------------------------------------
// Tests: deleting series and samples
// ---------------------------------------------------------------------------

fn delete_token() -> String {
    make_token(&TestClaims {
        sub: "cleaner".into(),
        exp: 4_102_444_800,
        nbf: None,
        iat: None,
        scope: Some("delete".into()),
        sensors: None,
    })
}

/// Publish three temperature samples, one minute apart from 2023-11-14T22:13:20Z,
/// and return the UUID of the series.
async fn publish_temperature(storage: &Arc<dyn StorageInstance>, name: &str) -> uuid::Uuid {
    publish_senml_data(
        &format!(
            r#"[{{"n":"{name}","v":1.0,"t":1700000000}},{{"n":"{name}","v":2.0,"t":1700000060}},{{"n":"{name}","v":3.0,"t":1700000120}}]"#
        ),
        storage.clone(),
    )
    .await
    .unwrap();
    storage
        .list_series(Some(name), None, None)
        .await
        .unwrap()
        .series[0]
        .uuid
}

async fn stored_sample_count(storage: &Arc<dyn StorageInstance>, series: uuid::Uuid) -> usize {
    let Some(data) = storage
        .query_sensor_data(&series.to_string(), None, None, None)
        .await
        .unwrap()
    else {
        return 0;
    };
    match data.samples {
        sensapp::datamodel::TypedSamples::Float(samples) => samples.len(),
        other => panic!("unexpected samples {other:?}"),
    }
}

async fn send_delete(app: &Router, uri: &str, token: Option<&str>) -> (StatusCode, String) {
    let mut request = Request::delete(uri);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
#[serial]
async fn delete_requires_the_delete_scope() {
    sensapp::config::load_configuration_for_tests().unwrap();
    let db = TestDb::new().await.expect("test db");
    let storage = db.storage();
    let series = publish_temperature(&storage, "temperature").await;
    let app = build_test_router(
        storage.clone(),
        Some(AuthConfig::from_secret(TEST_SECRET).unwrap()),
    );

    let series_uri = format!("/series/{series}");
    let samples_uri =
        format!("/series/{series}/samples?start=2023-11-14T22:14:20Z&end=2023-11-14T22:15:20Z");

    // No token, and the default "read write" scope, cannot delete
    for uri in [&series_uri, &samples_uri] {
        let (status, _) = send_delete(&app, uri, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}");
        for token in [read_write_token(), read_only_token(), write_only_token()] {
            let (status, body) = send_delete(&app, uri, Some(&token)).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {body}");
        }
    }
    assert_eq!(stored_sample_count(&storage, series).await, 3);

    // The delete scope can, and the deletion is inclusive on both bounds
    let (status, body) = send_delete(&app, &samples_uri, Some(&delete_token())).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["deleted_samples"], 2);
    assert_eq!(body["series_uuid"], series.to_string());
    assert_eq!(stored_sample_count(&storage, series).await, 1);

    let (status, body) = send_delete(&app, &series_uri, Some(&delete_token())).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    assert_eq!(stored_sample_count(&storage, series).await, 0);
    assert!(
        storage
            .list_series(Some("temperature"), None, None)
            .await
            .unwrap()
            .series
            .is_empty()
    );

    // Already gone
    let (status, _) = send_delete(&app, &series_uri, Some(&delete_token())).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send_delete(&app, &samples_uri, Some(&delete_token())).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn delete_is_open_without_auth() {
    sensapp::config::load_configuration_for_tests().unwrap();
    let db = TestDb::new().await.expect("test db");
    let storage = db.storage();
    let series = publish_temperature(&storage, "temperature").await;
    let app = build_test_router(storage.clone(), None);

    let (status, body) = send_delete(&app, &format!("/series/{series}"), None).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    assert_eq!(stored_sample_count(&storage, series).await, 0);
}

#[tokio::test]
#[serial]
async fn delete_samples_validates_its_parameters() {
    sensapp::config::load_configuration_for_tests().unwrap();
    let db = TestDb::new().await.expect("test db");
    let storage = db.storage();
    let series = publish_temperature(&storage, "temperature").await;
    let app = build_test_router(storage.clone(), None);

    for uri in [
        // Both bounds are required, so a request cannot wipe a series by accident
        format!("/series/{series}/samples"),
        format!("/series/{series}/samples?start=2023-11-14T22:14:20Z"),
        format!("/series/{series}/samples?end=2023-11-14T22:14:20Z"),
        // Invalid or inverted bounds
        format!("/series/{series}/samples?start=yesterday&end=2023-11-14T22:14:20Z"),
        format!("/series/{series}/samples?start=2023-11-14T22:15:20Z&end=2023-11-14T22:14:20Z"),
        // Invalid UUID
        "/series/not-a-uuid/samples?start=2023-11-14T22:14:20Z&end=2023-11-14T22:15:20Z"
            .to_string(),
        "/series/not-a-uuid".to_string(),
    ] {
        let (status, body) = send_delete(&app, &uri, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
    }
    assert_eq!(stored_sample_count(&storage, series).await, 3);

    // start == end deletes the sample at that exact timestamp
    let (status, body) = send_delete(
        &app,
        &format!("/series/{series}/samples?start=2023-11-14T22:14:20Z&end=2023-11-14T22:14:20Z"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["deleted_samples"], 1);
    assert_eq!(stored_sample_count(&storage, series).await, 2);

    // A valid UUID nobody owns
    let (status, _) = send_delete(
        &app,
        &format!(
            "/series/{}/samples?start=2023-11-14T22:14:20Z&end=2023-11-14T22:15:20Z",
            uuid::Uuid::new_v4()
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn sensor_scoped_delete_token_only_deletes_its_sensors() {
    sensapp::config::load_configuration_for_tests().unwrap();
    let db = TestDb::new().await.expect("test db");
    let storage = db.storage();
    let temperature = publish_temperature(&storage, "temperature").await;
    let humidity = publish_temperature(&storage, "humidity").await;
    let app = build_test_router(
        storage.clone(),
        Some(AuthConfig::from_secret(TEST_SECRET).unwrap()),
    );
    let token = sensor_token("delete", &["temperature"]);

    // Other sensors look like they do not exist, as they do for reads
    let (status, _) = send_delete(&app, &format!("/series/{humidity}"), Some(&token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send_delete(
        &app,
        &format!("/series/{humidity}/samples?start=2023-11-14T22:13:20Z&end=2023-11-14T22:15:20Z"),
        Some(&token),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(stored_sample_count(&storage, humidity).await, 3);

    let (status, body) = send_delete(&app, &format!("/series/{temperature}"), Some(&token)).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    assert_eq!(stored_sample_count(&storage, temperature).await, 0);
    assert_eq!(stored_sample_count(&storage, humidity).await, 3);
}
