use axum::extract::Request;
use axum::extract::State;
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::Response;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::app_error::AppError;

// ---------------------------------------------------------------------------
// Auth configuration
// ---------------------------------------------------------------------------

/// JWT authentication configuration.
///
/// When present in the server state, all protected endpoints require a valid
/// JWT bearer token. When absent (`None`), all endpoints are open.
#[derive(Clone)]
pub struct AuthConfig {
    decoding_key: Arc<DecodingKey>,
    encoding_key: Arc<EncodingKey>,
    validation: Arc<Validation>,
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthConfig(…)")
    }
}

impl AuthConfig {
    /// Create an AuthConfig from an HMAC secret (HS256).
    ///
    /// The secret must be at least 32 characters long.
    pub fn from_secret(secret: &str) -> Result<Self, anyhow::Error> {
        if secret.len() < 32 {
            anyhow::bail!("JWT secret must be at least 32 characters long");
        }

        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.set_required_spec_claims(&["exp", "sub"]);

        Ok(Self {
            decoding_key: Arc::new(DecodingKey::from_secret(secret.as_bytes())),
            encoding_key: Arc::new(EncodingKey::from_secret(secret.as_bytes())),
            validation: Arc::new(validation),
        })
    }

    /// Create a signed JWT token.
    pub fn create_token(
        &self,
        sub: &str,
        scope: &str,
        duration_seconds: u64,
        sensors: Option<Vec<String>>,
    ) -> Result<String, anyhow::Error> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        let claims = Claims {
            sub: sub.to_string(),
            exp: now + duration_seconds,
            iat: Some(now),
            nbf: Some(now),
            scope: Some(scope.to_string()),
            sensors,
        };

        let token = encode(&Header::default(), &claims, &self.encoding_key)?;
        Ok(token)
    }
}

// ---------------------------------------------------------------------------
// JWT Claims
// ---------------------------------------------------------------------------

/// JWT claims for SensApp authentication.
///
/// # Required fields
/// - `sub`: Subject — identifies who the token was issued to
/// - `exp`: Expiration time (Unix timestamp) — tokens without this are rejected
///
/// # Optional fields
/// - `iat`: Issued-at time (informational)
/// - `nbf`: Not-before time — if present, the token is rejected before this time
/// - `scope`: Space-separated scopes: `"read"`, `"write"`, or `"read write"` (default: `"read write"`)
/// - `sensors`: Allow list of sensor names; if absent all sensors are accessible
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Claims {
    /// Subject — identifies who/what the token was issued to.
    pub sub: String,
    /// Expiration time (Unix timestamp). Required.
    pub exp: u64,
    /// Issued-at time (Unix timestamp).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iat: Option<u64>,
    /// Not-before time (Unix timestamp). Validated when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbf: Option<u64>,
    /// Space-separated scopes: "read", "write", or "read write".
    /// Defaults to "read write" if absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// Optional list of allowed sensor name patterns.
    /// If absent, all sensors are accessible.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sensors: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Access context
// ---------------------------------------------------------------------------

/// Validated access context extracted from a JWT token.
#[derive(Clone, Debug)]
pub struct AccessContext {
    pub subject: String,
    pub can_read: bool,
    pub can_write: bool,
    pub sensor_allow_list: Option<Vec<String>>,
}

impl AccessContext {
    /// Check if access to a specific sensor is allowed by the token.
    ///
    /// When no allow list is set, all sensors are accessible.
    /// Otherwise the sensor name must match one of the entries exactly.
    pub fn can_access_sensor(&self, sensor_name: &str) -> bool {
        match &self.sensor_allow_list {
            Some(allow_list) => allow_list.iter().any(|allowed| allowed == sensor_name),
            None => true,
        }
    }
}

// ---------------------------------------------------------------------------
// Token validation
// ---------------------------------------------------------------------------

/// Extract and validate a JWT from the `Authorization: Bearer <token>` header.
pub fn validate_token(headers: &HeaderMap, config: &AuthConfig) -> Result<AccessContext, AppError> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(AppError::Unauthorized(
            "Missing or invalid Authorization header".into(),
        ))?;

    let token_data = decode::<Claims>(token, &config.decoding_key, &config.validation)
        .map_err(|e| AppError::Unauthorized(format!("Invalid token: {}", e)))?;

    let claims = token_data.claims;

    let scope = claims.scope.unwrap_or_else(|| "read write".to_string());
    let can_read = scope
        .split_whitespace()
        .any(|s| s.eq_ignore_ascii_case("read"));
    let can_write = scope
        .split_whitespace()
        .any(|s| s.eq_ignore_ascii_case("write"));

    Ok(AccessContext {
        subject: claims.sub,
        can_read,
        can_write,
        sensor_allow_list: claims.sensors,
    })
}

// ---------------------------------------------------------------------------
// Axum middleware
// ---------------------------------------------------------------------------

/// Middleware that requires a valid JWT with **read** scope.
///
/// When `auth` is `None` (security disabled), all requests pass through.
pub async fn require_read_auth(
    State(auth): State<Option<AuthConfig>>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    if let Some(config) = &auth {
        let access = validate_token(request.headers(), config)?;
        if !access.can_read {
            return Err(AppError::Forbidden("Read access required".into()));
        }
    }
    Ok(next.run(request).await)
}

/// Middleware that requires a valid JWT with **write** scope.
///
/// When `auth` is `None` (security disabled), all requests pass through.
pub async fn require_write_auth(
    State(auth): State<Option<AuthConfig>>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    if let Some(config) = &auth {
        let access = validate_token(request.headers(), config)?;
        if !access.can_write {
            return Err(AppError::Forbidden("Write access required".into()));
        }
    }
    Ok(next.run(request).await)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    const TEST_SECRET: &str = "test-secret-0123456789abcdef012345";

    fn make_config() -> AuthConfig {
        AuthConfig::from_secret(TEST_SECRET).expect("test config")
    }

    fn bearer_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header should build"),
        );
        headers
    }

    fn encode_test_claims(claims: &Claims) -> String {
        encode(
            &Header::default(),
            claims,
            &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
        )
        .expect("token should encode")
    }

    #[test]
    fn secret_too_short_is_rejected() {
        let result = AuthConfig::from_secret("too-short");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("at least 32 characters")
        );
    }

    #[test]
    fn accepts_valid_token() {
        let token = encode_test_claims(&Claims {
            sub: "test-user".to_string(),
            exp: 4_102_444_800,
            iat: Some(1_000_000_000),
            nbf: Some(1_000_000_000),
            scope: Some("read write".to_string()),
            sensors: None,
        });

        let access =
            validate_token(&bearer_headers(&token), &make_config()).expect("token should be valid");

        assert_eq!(access.subject, "test-user");
        assert!(access.can_read);
        assert!(access.can_write);
        assert!(access.can_access_sensor("anything"));
    }

    #[test]
    fn rejects_expired_token() {
        let token = encode_test_claims(&Claims {
            sub: "test-user".to_string(),
            exp: 0,
            iat: None,
            nbf: None,
            scope: Some("read write".to_string()),
            sensors: None,
        });

        let result = validate_token(&bearer_headers(&token), &make_config());
        assert!(result.is_err());
    }

    #[test]
    fn rejects_not_yet_valid_token() {
        let token = encode_test_claims(&Claims {
            sub: "test-user".to_string(),
            exp: 4_102_444_800,
            iat: None,
            nbf: Some(4_102_444_800), // far in the future
            scope: Some("read write".to_string()),
            sensors: None,
        });

        let result = validate_token(&bearer_headers(&token), &make_config());
        assert!(result.is_err());
    }

    #[test]
    fn rejects_wrong_secret() {
        let wrong_key = EncodingKey::from_secret(b"wrong-secret-that-is-long-enough!!");
        let token = encode(
            &Header::default(),
            &Claims {
                sub: "test-user".to_string(),
                exp: 4_102_444_800,
                iat: None,
                nbf: None,
                scope: None,
                sensors: None,
            },
            &wrong_key,
        )
        .unwrap();

        let result = validate_token(&bearer_headers(&token), &make_config());
        assert!(result.is_err());
    }

    #[test]
    fn rejects_missing_authorization_header() {
        let result = validate_token(&HeaderMap::new(), &make_config());
        assert!(result.is_err());
    }

    #[test]
    fn rejects_non_bearer_scheme() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic dXNlcjpwYXNz"),
        );
        let result = validate_token(&headers, &make_config());
        assert!(result.is_err());
    }

    #[test]
    fn scope_read_only() {
        let token = encode_test_claims(&Claims {
            sub: "reader".to_string(),
            exp: 4_102_444_800,
            iat: None,
            nbf: None,
            scope: Some("read".to_string()),
            sensors: None,
        });

        let access =
            validate_token(&bearer_headers(&token), &make_config()).expect("should decode");
        assert!(access.can_read);
        assert!(!access.can_write);
    }

    #[test]
    fn scope_write_only() {
        let token = encode_test_claims(&Claims {
            sub: "writer".to_string(),
            exp: 4_102_444_800,
            iat: None,
            nbf: None,
            scope: Some("write".to_string()),
            sensors: None,
        });

        let access =
            validate_token(&bearer_headers(&token), &make_config()).expect("should decode");
        assert!(!access.can_read);
        assert!(access.can_write);
    }

    #[test]
    fn missing_scope_defaults_to_read_write() {
        let token = encode_test_claims(&Claims {
            sub: "admin".to_string(),
            exp: 4_102_444_800,
            iat: None,
            nbf: None,
            scope: None,
            sensors: None,
        });

        let access =
            validate_token(&bearer_headers(&token), &make_config()).expect("should decode");
        assert!(access.can_read);
        assert!(access.can_write);
    }

    #[test]
    fn sensor_allow_list_filters() {
        let token = encode_test_claims(&Claims {
            sub: "limited".to_string(),
            exp: 4_102_444_800,
            iat: None,
            nbf: None,
            scope: None,
            sensors: Some(vec!["temperature".to_string(), "humidity".to_string()]),
        });

        let access =
            validate_token(&bearer_headers(&token), &make_config()).expect("should decode");
        assert!(access.can_access_sensor("temperature"));
        assert!(access.can_access_sensor("humidity"));
        assert!(!access.can_access_sensor("pressure"));
    }

    #[test]
    fn no_sensor_allow_list_allows_all() {
        let token = encode_test_claims(&Claims {
            sub: "admin".to_string(),
            exp: 4_102_444_800,
            iat: None,
            nbf: None,
            scope: None,
            sensors: None,
        });

        let access =
            validate_token(&bearer_headers(&token), &make_config()).expect("should decode");
        assert!(access.can_access_sensor("anything"));
        assert!(access.can_access_sensor("temperature"));
    }

    #[test]
    fn create_token_produces_valid_jwt() {
        let config = make_config();
        let token = config
            .create_token("my-service", "read write", 3600, None)
            .expect("should create token");

        let access =
            validate_token(&bearer_headers(&token), &config).expect("token should validate");
        assert_eq!(access.subject, "my-service");
        assert!(access.can_read);
        assert!(access.can_write);
    }

    #[test]
    fn create_token_with_sensor_filter() {
        let config = make_config();
        let token = config
            .create_token(
                "sensor-reader",
                "read",
                3600,
                Some(vec!["cpu".to_string(), "mem".to_string()]),
            )
            .expect("should create token");

        let access =
            validate_token(&bearer_headers(&token), &config).expect("token should validate");
        assert_eq!(access.subject, "sensor-reader");
        assert!(access.can_read);
        assert!(!access.can_write);
        assert!(access.can_access_sensor("cpu"));
        assert!(access.can_access_sensor("mem"));
        assert!(!access.can_access_sensor("disk"));
    }
}
