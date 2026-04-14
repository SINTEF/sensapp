//! Integration tests for optional JWT authentication.
//!
//! These tests verify that:
//! - Without auth configured (`auth: None`), all endpoints are open.
//! - With auth configured, read endpoints require a "read" scope token.
//! - With auth configured, write endpoints require a "write" scope token.
//! - Expired and not-yet-valid tokens are rejected.
//! - Sensor allow lists are enforced.
//! - Health/docs/prometheus-metrics remain public even with auth enabled.

mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::{get, post};
use common::TestDb;
use jsonwebtoken::{EncodingKey, Header, encode};
use sensapp::http::auth::AuthConfig;
use sensapp::http::crud::{get_series_data, list_metrics, list_series};
use sensapp::http::health::{liveness, readiness};
use sensapp::http::metrics::{HttpMetrics, prometheus_metrics};
use sensapp::http::server::publish_senml_data;
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
    headers: axum::http::HeaderMap,
    body: Body,
) -> Result<String, (StatusCode, String)> {
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
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:?}")))?;
    }
    Ok("ok".to_string())
}

/// Build a test router with the same auth layering as the real server.
fn build_test_router(storage: Arc<dyn StorageInstance>, auth: Option<AuthConfig>) -> Router {
    use sensapp::http::auth::{require_read_auth, require_write_auth};

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
        .route_layer(axum::middleware::from_fn_with_state(
            auth.clone(),
            require_read_auth,
        ));

    let write_routes = Router::new()
        .route("/publish", post(test_publish_handler))
        .route_layer(axum::middleware::from_fn_with_state(
            auth,
            require_write_auth,
        ));

    public
        .merge(read_routes)
        .merge(write_routes)
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
