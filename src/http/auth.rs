use axum::extract::Request;
use axum::extract::State;
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::Response;
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

use super::app_error::AppError;

/// Issuer and audience of the tokens SensApp makes, checked on the tokens it receives: a token
/// of another system, signed with a secret that was reused, is refused.
pub const TOKEN_ISSUER: &str = "sensapp";
pub const TOKEN_AUDIENCE: &str = "sensapp";

const MIN_SECRET_LENGTH: usize = 32;

/// Longest token the HTTP endpoint makes unless configured otherwise: one year.
pub const DEFAULT_MAX_TOKEN_DURATION_SECONDS: u64 = 365 * 24 * 3600;

// ---------------------------------------------------------------------------
// Auth configuration
// ---------------------------------------------------------------------------

/// JWT authentication configuration.
///
/// When present in the server state, all protected endpoints require a valid
/// JWT bearer token. When absent (`None`), all endpoints are open.
///
/// Tokens are signed with one secret and name it in their `kid` header, so that the secrets of
/// a rotation (the new one signs, the previous ones still verify) are told apart.
#[derive(Clone)]
pub struct AuthConfig {
    signing_kid: Arc<str>,
    encoding_key: Arc<EncodingKey>,
    /// The key of the signing secret, which verifies the tokens that have no `kid`.
    decoding_key: Arc<DecodingKey>,
    /// Every key that verifies tokens by `kid`: the signing secret and the previous ones.
    decoding_keys: Arc<HashMap<String, DecodingKey>>,
    validation: Arc<Validation>,
    /// Longest validity of the tokens the HTTP endpoint makes.
    max_token_duration_seconds: u64,
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthConfig(…)")
    }
}

/// Name of a secret: the first 4 bytes of its SHA-256, in hexadecimal. It is in the header of
/// the tokens, which anyone can read, and does not give the secret away.
fn key_id(secret: &str) -> String {
    Sha256::digest(secret.as_bytes())
        .iter()
        .take(4)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn check_secret_length(secret: &str) -> Result<(), anyhow::Error> {
    if secret.len() < MIN_SECRET_LENGTH {
        anyhow::bail!("JWT secret must be at least {MIN_SECRET_LENGTH} characters long");
    }
    Ok(())
}

/// A token that was just made.
#[derive(Clone, Debug)]
pub struct IssuedToken {
    pub token: String,
    /// Unique id of the token, in its `jti` claim and in the logs.
    pub jti: String,
    /// Expiration time, Unix timestamp.
    pub expires_at: u64,
}

impl AuthConfig {
    /// Create an AuthConfig from an HMAC secret (HS256).
    ///
    /// The secret must be at least 32 characters long.
    pub fn from_secret(secret: &str) -> Result<Self, anyhow::Error> {
        check_secret_length(secret)?;

        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.set_required_spec_claims(&["exp", "sub", "iss", "aud"]);
        validation.set_issuer(&[TOKEN_ISSUER]);
        validation.set_audience(&[TOKEN_AUDIENCE]);

        let kid = key_id(secret);
        let decoding_key = DecodingKey::from_secret(secret.as_bytes());
        Ok(Self {
            signing_kid: kid.as_str().into(),
            encoding_key: Arc::new(EncodingKey::from_secret(secret.as_bytes())),
            decoding_key: Arc::new(decoding_key.clone()),
            decoding_keys: Arc::new(HashMap::from([(kid, decoding_key)])),
            validation: Arc::new(validation),
            max_token_duration_seconds: DEFAULT_MAX_TOKEN_DURATION_SECONDS,
        })
    }

    /// Cap the validity of the tokens made through the HTTP endpoint (the command line has the
    /// secret, so it is not capped by this).
    pub fn with_max_token_duration(mut self, seconds: u64) -> Self {
        self.max_token_duration_seconds = seconds;
        self
    }

    pub fn max_token_duration_seconds(&self) -> u64 {
        self.max_token_duration_seconds
    }

    /// Also verify the tokens signed with these previous secrets, which never sign. This is how
    /// a secret is rotated without refusing the tokens of the clients at once: the new secret
    /// signs, the old one is kept here until its tokens have expired, then dropped.
    pub fn with_previous_secrets(mut self, secrets: &[String]) -> Result<Self, anyhow::Error> {
        let keys = Arc::make_mut(&mut self.decoding_keys);
        for secret in secrets {
            check_secret_length(secret).map_err(|_| {
                anyhow::anyhow!(
                    "SENSAPP_JWT_PREVIOUS_SECRETS: each secret must be at least \
                     {MIN_SECRET_LENGTH} characters long"
                )
            })?;
            keys.insert(key_id(secret), DecodingKey::from_secret(secret.as_bytes()));
        }
        Ok(self)
    }

    /// Create a signed JWT token, with a `jti` of its own.
    pub fn issue_token(
        &self,
        sub: &str,
        scope: &str,
        duration_seconds: u64,
        sensors: Option<Vec<String>>,
    ) -> Result<IssuedToken, anyhow::Error> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let expires_at = now
            .checked_add(duration_seconds)
            .ok_or_else(|| anyhow::anyhow!("The duration of the token is too long"))?;
        let jti = uuid::Uuid::new_v4().to_string();

        let claims = Claims {
            sub: sub.to_string(),
            exp: expires_at,
            iat: Some(now),
            nbf: Some(now),
            scope: Some(scope.to_string()),
            sensors,
            iss: Some(TOKEN_ISSUER.to_string()),
            aud: Some(TOKEN_AUDIENCE.to_string()),
            jti: Some(jti.clone()),
        };

        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(self.signing_kid.to_string());
        let token = encode(&header, &claims, &self.encoding_key)?;
        Ok(IssuedToken {
            token,
            jti,
            expires_at,
        })
    }
}

// ---------------------------------------------------------------------------
// Startup mode
// ---------------------------------------------------------------------------

/// How the server authenticates, decided once at startup by [`resolve_auth_mode`].
#[derive(Clone, PartialEq, Eq)]
pub enum AuthMode {
    /// The operator configured the secret.
    Secret(String),
    /// No secret was configured on a loopback address: this one is made for the run, and lost
    /// when the process stops.
    Ephemeral(String),
    /// `SENSAPP_AUTH_DISABLED`: every endpoint is open.
    Disabled,
}

impl std::fmt::Debug for AuthMode {
    // Never print a secret, even by mistake in a log
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            AuthMode::Secret(_) => "AuthMode::Secret(…)",
            AuthMode::Ephemeral(_) => "AuthMode::Ephemeral(…)",
            AuthMode::Disabled => "AuthMode::Disabled",
        })
    }
}

/// Make a random secret of 48 characters (288 bits), which `sensapp generate-secret` prints.
pub fn generate_secret() -> Result<String, anyhow::Error> {
    use base64::Engine;
    let mut bytes = [0u8; 36];
    getrandom::fill(&mut bytes)
        .map_err(|e| anyhow::anyhow!("Cannot get random bytes for a secret: {e}"))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

/// Decide how to authenticate. SensApp is not open unless asked to be:
///
/// - a configured secret always wins, even when `auth_disabled` is also set;
/// - without a secret, `auth_disabled` opens everything, explicitly;
/// - without a secret on a loopback address (a developer's machine, where only the local user
///   can connect) a secret is made for this run;
/// - without a secret on any other address (a container, a server) it is an error: a secret made
///   per process would differ between instances, and silently open is what this prevents.
pub fn resolve_auth_mode(
    secret: Option<&str>,
    auth_disabled: bool,
    endpoint: std::net::IpAddr,
) -> Result<AuthMode, anyhow::Error> {
    if let Some(secret) = secret {
        return Ok(AuthMode::Secret(secret.to_string()));
    }
    if auth_disabled {
        return Ok(AuthMode::Disabled);
    }
    if endpoint.is_loopback() {
        return Ok(AuthMode::Ephemeral(generate_secret()?));
    }
    anyhow::bail!(
        "SensApp listens on {endpoint}, which other machines can reach, and has no \
         authentication configured. Choose one:\n  \
         - set SENSAPP_JWT_SECRET to a secret of at least 32 characters (`sensapp generate-secret` makes one),\n  \
         - listen on 127.0.0.1 for a local run, where SensApp makes a secret and prints an admin token,\n  \
         - set SENSAPP_AUTH_DISABLED=true to run with every endpoint open."
    )
}

// ---------------------------------------------------------------------------
// JWT Claims
// ---------------------------------------------------------------------------

/// JWT claims for SensApp authentication.
///
/// # Required fields
/// - `sub`: Subject — identifies who the token was issued to
/// - `exp`: Expiration time (Unix timestamp) — tokens without this are rejected
/// - `iss` and `aud`: both `sensapp`
///
/// # Optional fields
/// - `iat`: Issued-at time (informational)
/// - `nbf`: Not-before time — if present, the token is rejected before this time
/// - `jti`: Unique id of the token, in the logs
/// - `scope`: Space-separated scopes among `"read"`, `"write"`, `"delete"` and `"admin"`
///   (default: `"read write"`). `"delete"` allows removing series and samples and `"admin"`
///   allows making tokens; neither is ever part of the default.
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
    /// Space-separated scopes among "read", "write", "delete" and "admin".
    /// Defaults to "read write" if absent, which includes neither "delete" nor "admin".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// Optional list of allowed sensor names.
    /// If absent, all sensors are accessible.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sensors: Option<Vec<String>>,
    /// Issuer, `sensapp`. Required, and checked by the validation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iss: Option<String>,
    /// Audience, `sensapp`. Required, and checked by the validation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<String>,
    /// Unique id of the token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jti: Option<String>,
}

// ---------------------------------------------------------------------------
// Scopes
// ---------------------------------------------------------------------------

/// What a token may do. Each one is explicit: none implies another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    Read,
    Write,
    Delete,
    /// Make tokens (`POST /api/v1/admin/tokens`), nothing else.
    Admin,
}

impl Scope {
    pub fn name(self) -> &'static str {
        match self {
            Scope::Read => "read",
            Scope::Write => "write",
            Scope::Delete => "delete",
            Scope::Admin => "admin",
        }
    }
}

// ---------------------------------------------------------------------------
// Token requests
// ---------------------------------------------------------------------------

/// Why a request for a token is refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TokenRequestError {
    #[error("{0}")]
    Invalid(String),
    #[error("Admin tokens are only made with `sensapp generate-token`")]
    AdminNotAllowed,
}

/// What may be asked for, which depends on who asks: the command line, run with the secret, may
/// make anything, the HTTP endpoint does not make admin tokens and caps the duration.
#[derive(Clone, Copy, Debug)]
pub struct TokenRules {
    pub max_duration_seconds: u64,
    pub allow_admin: bool,
}

const MAX_SUBJECT_LENGTH: usize = 128;
const MAX_SENSOR_NAME_LENGTH: usize = 256;
const MAX_SENSORS: usize = 1000;

/// A request for a token, checked: the same rules for the command line and the HTTP endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenRequest {
    pub subject: String,
    /// Space-separated, in the form of the `scope` claim.
    pub scope: String,
    pub sensors: Option<Vec<String>>,
    pub duration_seconds: u64,
}

impl TokenRequest {
    /// Check a request. `scope` is a list of `read`, `write`, `delete`, `admin` and `readwrite`
    /// (shorthand for `read write`), separated by commas or spaces.
    pub fn new(
        subject: &str,
        scope: &str,
        sensors: Option<Vec<String>>,
        duration_seconds: u64,
        rules: &TokenRules,
    ) -> Result<Self, TokenRequestError> {
        let invalid = |message: String| TokenRequestError::Invalid(message);

        let subject = subject.trim();
        if subject.is_empty() {
            return Err(invalid("The subject cannot be empty".into()));
        }
        if subject.chars().count() > MAX_SUBJECT_LENGTH {
            return Err(invalid(format!(
                "The subject is longer than {MAX_SUBJECT_LENGTH} characters"
            )));
        }

        let scope = parse_scope(scope)?;
        if !rules.allow_admin && scope.split(' ').any(|item| item == Scope::Admin.name()) {
            return Err(TokenRequestError::AdminNotAllowed);
        }

        if duration_seconds == 0 {
            return Err(invalid("The duration must be at least one second".into()));
        }
        if duration_seconds > rules.max_duration_seconds {
            return Err(invalid(format!(
                "The duration cannot be longer than {} seconds",
                rules.max_duration_seconds
            )));
        }

        let sensors = match sensors {
            None => None,
            Some(names) => {
                if names.is_empty() {
                    return Err(invalid(
                        "The list of sensors is empty: leave it out to allow every sensor".into(),
                    ));
                }
                if names.len() > MAX_SENSORS {
                    return Err(invalid(format!("More than {MAX_SENSORS} sensors")));
                }
                let mut cleaned: Vec<String> = Vec::with_capacity(names.len());
                for name in names {
                    let name = name.trim();
                    if name.is_empty() {
                        return Err(invalid("A sensor name is empty".into()));
                    }
                    if name.chars().count() > MAX_SENSOR_NAME_LENGTH {
                        return Err(invalid(format!(
                            "A sensor name is longer than {MAX_SENSOR_NAME_LENGTH} characters"
                        )));
                    }
                    if !cleaned.iter().any(|known| known == name) {
                        cleaned.push(name.to_string());
                    }
                }
                Some(cleaned)
            }
        };

        Ok(Self {
            subject: subject.to_string(),
            scope,
            sensors,
            duration_seconds,
        })
    }

    /// Sign the request.
    pub fn issue(&self, auth: &AuthConfig) -> Result<IssuedToken, anyhow::Error> {
        auth.issue_token(
            &self.subject,
            &self.scope,
            self.duration_seconds,
            self.sensors.clone(),
        )
    }
}

/// Parse a list of scopes, comma or space separated: `read`, `write`, `delete`, `admin`, with
/// `readwrite` as a shorthand for `read write`. Returns them space-separated, each once.
pub fn parse_scope(raw: &str) -> Result<String, TokenRequestError> {
    let mut scopes: Vec<&str> = Vec::new();
    for item in raw.split([',', ' ']).filter(|item| !item.is_empty()) {
        let expanded: &[&str] = match item {
            "read" => &["read"],
            "write" => &["write"],
            "delete" => &["delete"],
            "admin" => &["admin"],
            "readwrite" => &["read", "write"],
            other => {
                return Err(TokenRequestError::Invalid(format!(
                    "Unknown scope: {other}. Use read, write, delete, admin, or readwrite"
                )));
            }
        };
        for scope in expanded {
            if !scopes.contains(scope) {
                scopes.push(scope);
            }
        }
    }
    if scopes.is_empty() {
        return Err(TokenRequestError::Invalid(
            "At least one scope is required".into(),
        ));
    }
    Ok(scopes.join(" "))
}

// ---------------------------------------------------------------------------
// Access context
// ---------------------------------------------------------------------------

/// Validated access context extracted from a JWT token.
#[derive(Clone, Debug)]
pub struct AccessContext {
    pub subject: String,
    /// Unique id of the token, when it has one.
    pub token_id: Option<String>,
    pub can_read: bool,
    pub can_write: bool,
    pub can_delete: bool,
    pub can_admin: bool,
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

    pub fn has_scope(&self, scope: Scope) -> bool {
        match scope {
            Scope::Read => self.can_read,
            Scope::Write => self.can_write,
            Scope::Delete => self.can_delete,
            Scope::Admin => self.can_admin,
        }
    }

    /// Put who is asking in the log span of the request, to tell who did what.
    pub fn record_in_current_span(&self) {
        let span = tracing::Span::current();
        span.record("subject", self.subject.as_str());
        if let Some(token_id) = &self.token_id {
            span.record("token_id", token_id.as_str());
        }
    }
}

// ---------------------------------------------------------------------------
// Token validation
// ---------------------------------------------------------------------------

/// Extract and validate a JWT from the `Authorization: Bearer <token>` header. The
/// `Authorization: Token <token>` of the InfluxDB clients (Telegraf) is accepted as well.
pub fn validate_token(headers: &HeaderMap, config: &AuthConfig) -> Result<AccessContext, AppError> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("Token "))
        })
        .ok_or(AppError::Unauthorized(
            "Missing or invalid Authorization header".into(),
        ))?;

    // The `kid` tells which secret signed the token. Without one it is the current secret's.
    let token_header = decode_header(token)
        .map_err(|e| AppError::Unauthorized(format!("Invalid token: {}", e)))?;
    let key = match &token_header.kid {
        Some(kid) => config.decoding_keys.get(kid).ok_or_else(|| {
            AppError::Unauthorized("Invalid token: signed with an unknown secret".into())
        })?,
        None => &config.decoding_key,
    };

    let token_data = decode::<Claims>(token, key, &config.validation)
        .map_err(|e| AppError::Unauthorized(format!("Invalid token: {}", e)))?;

    let claims = token_data.claims;

    let scope = claims.scope.unwrap_or_else(|| "read write".to_string());
    let has = |wanted: Scope| {
        scope
            .split_whitespace()
            .any(|s| s.eq_ignore_ascii_case(wanted.name()))
    };

    Ok(AccessContext {
        can_read: has(Scope::Read),
        can_write: has(Scope::Write),
        can_delete: has(Scope::Delete),
        can_admin: has(Scope::Admin),
        subject: claims.sub,
        token_id: claims.jti,
        sensor_allow_list: claims.sensors,
    })
}

// ---------------------------------------------------------------------------
// Axum middleware
// ---------------------------------------------------------------------------

/// Require a valid JWT with the scope, and hand the access context to the handlers.
///
/// When `auth` is `None` (security disabled), all requests pass through.
async fn require_scope(
    auth: Option<AuthConfig>,
    mut request: Request,
    next: Next,
    scope: Scope,
) -> Result<Response, AppError> {
    if let Some(config) = &auth {
        let access = validate_token(request.headers(), config)?;
        if !access.has_scope(scope) {
            return Err(AppError::Forbidden(format!(
                "{} access required",
                scope.name()
            )));
        }
        access.record_in_current_span();
        request.extensions_mut().insert(access);
    }
    Ok(next.run(request).await)
}

/// Middleware that requires a valid JWT with **read** scope.
pub async fn require_read_auth(
    State(auth): State<Option<AuthConfig>>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    require_scope(auth, request, next, Scope::Read).await
}

/// Middleware that requires a valid JWT with **write** scope.
pub async fn require_write_auth(
    State(auth): State<Option<AuthConfig>>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    require_scope(auth, request, next, Scope::Write).await
}

/// Middleware that requires a valid JWT with **delete** scope.
///
/// The `delete` scope is never implied by the default `read write` scope.
pub async fn require_delete_auth(
    State(auth): State<Option<AuthConfig>>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    require_scope(auth, request, next, Scope::Delete).await
}

/// Middleware that requires a valid JWT with **admin** scope, to make tokens. The `admin` scope
/// gives neither read, write nor delete.
pub async fn require_admin_auth(
    State(auth): State<Option<AuthConfig>>,
    request: Request,
    next: Next,
) -> Result<Response, AppError> {
    require_scope(auth, request, next, Scope::Admin).await
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

    /// A valid base for the tests, which set what they test.
    fn default_claims() -> Claims {
        Claims {
            sub: String::new(),
            exp: 0,
            iat: None,
            nbf: None,
            scope: None,
            sensors: None,
            iss: Some(TOKEN_ISSUER.to_string()),
            aud: Some(TOKEN_AUDIENCE.to_string()),
            jti: None,
        }
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
            ..default_claims()
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
            ..default_claims()
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
            ..default_claims()
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
                ..default_claims()
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
            ..default_claims()
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
            ..default_claims()
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
            ..default_claims()
        });

        let access =
            validate_token(&bearer_headers(&token), &make_config()).expect("should decode");
        assert!(access.can_read);
        assert!(access.can_write);
        assert!(!access.can_delete, "delete must not be part of the default");
    }

    #[test]
    fn scope_delete_is_explicit() {
        for (scope, expected) in [
            ("delete", true),
            ("read write delete", true),
            ("read DELETE", true),
            ("read write", false),
            ("deleted", false),
        ] {
            let token = encode_test_claims(&Claims {
                sub: "cleaner".to_string(),
                exp: 4_102_444_800,
                iat: None,
                nbf: None,
                scope: Some(scope.to_string()),
                sensors: None,
                ..default_claims()
            });
            let access =
                validate_token(&bearer_headers(&token), &make_config()).expect("should decode");
            assert_eq!(access.can_delete, expected, "scope {scope:?}");
        }
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
            ..default_claims()
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
            ..default_claims()
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
            .issue_token("my-service", "read write", 3600, None)
            .expect("should create token")
            .token;

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
            .issue_token(
                "sensor-reader",
                "read",
                3600,
                Some(vec!["cpu".to_string(), "mem".to_string()]),
            )
            .expect("should create token")
            .token;

        let access =
            validate_token(&bearer_headers(&token), &config).expect("token should validate");
        assert_eq!(access.subject, "sensor-reader");
        assert!(access.can_read);
        assert!(!access.can_write);
        assert!(access.can_access_sensor("cpu"));
        assert!(access.can_access_sensor("mem"));
        assert!(!access.can_access_sensor("disk"));
    }

    // -- token model --

    fn config_with(secret: &str) -> AuthConfig {
        AuthConfig::from_secret(secret).expect("config")
    }

    const OLD_SECRET: &str = "old-secret-0123456789abcdef0123456789";

    fn header_value(scheme: &str, token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("{scheme} {token}")).expect("header should build"),
        );
        headers
    }

    #[test]
    fn issued_tokens_carry_an_id_and_a_key_id() {
        let config = make_config();
        let first = config.issue_token("a", "read", 60, None).unwrap();
        let second = config.issue_token("a", "read", 60, None).unwrap();
        assert_ne!(first.jti, second.jti);

        let header = decode_header(&first.token).unwrap();
        assert_eq!(header.kid.as_deref(), Some(key_id(TEST_SECRET).as_str()));
        // The key id is not the secret
        assert!(!first.token.contains(TEST_SECRET));

        let access = validate_token(&bearer_headers(&first.token), &config).unwrap();
        assert_eq!(access.token_id.as_deref(), Some(first.jti.as_str()));
    }

    #[test]
    fn the_token_scheme_of_influxdb_clients_is_accepted() {
        let config = make_config();
        let token = config
            .issue_token("telegraf", "write", 60, None)
            .unwrap()
            .token;
        let access = validate_token(&header_value("Token", &token), &config).unwrap();
        assert_eq!(access.subject, "telegraf");
        assert!(access.can_write);
        // Other schemes still are not
        assert!(validate_token(&header_value("Basic", &token), &config).is_err());
        assert!(validate_token(&header_value("Token", "nonsense"), &config).is_err());
    }

    #[test]
    fn rejects_another_issuer_or_audience() {
        let mut claims = default_claims();
        claims.sub = "x".into();
        claims.exp = 4_102_444_800;
        claims.scope = Some("read".into());

        let mut other_issuer = claims.clone();
        other_issuer.iss = Some("another-system".into());
        assert!(
            validate_token(
                &bearer_headers(&encode_test_claims(&other_issuer)),
                &make_config()
            )
            .is_err()
        );

        let mut other_audience = claims.clone();
        other_audience.aud = Some("another-system".into());
        assert!(
            validate_token(
                &bearer_headers(&encode_test_claims(&other_audience)),
                &make_config()
            )
            .is_err()
        );

        let mut no_issuer = claims.clone();
        no_issuer.iss = None;
        assert!(
            validate_token(
                &bearer_headers(&encode_test_claims(&no_issuer)),
                &make_config()
            )
            .is_err()
        );

        // The base is valid, so only the issuer or the audience made the others fail
        assert!(
            validate_token(
                &bearer_headers(&encode_test_claims(&claims)),
                &make_config()
            )
            .is_ok()
        );
    }

    #[test]
    fn a_rotated_secret_still_verifies_the_tokens_of_the_previous_one() {
        let old = config_with(OLD_SECRET);
        let old_token = old.issue_token("client", "read", 60, None).unwrap().token;

        // Rotation: the new secret signs, the old one only verifies
        let rotated = make_config()
            .with_previous_secrets(&[OLD_SECRET.to_string()])
            .unwrap();
        assert!(validate_token(&bearer_headers(&old_token), &rotated).is_ok());
        let new_token = rotated
            .issue_token("client", "read", 60, None)
            .unwrap()
            .token;
        assert_eq!(
            decode_header(&new_token).unwrap().kid.as_deref(),
            Some(key_id(TEST_SECRET).as_str())
        );
        assert!(validate_token(&bearer_headers(&new_token), &rotated).is_ok());

        // Once the old secret is dropped, its tokens are refused, and so is an unknown key id
        let dropped = make_config();
        assert!(validate_token(&bearer_headers(&old_token), &dropped).is_err());
        assert!(validate_token(&bearer_headers(&new_token), &old).is_err());
    }

    #[test]
    fn a_token_without_key_id_is_checked_against_the_current_secret() {
        let mut claims = default_claims();
        claims.sub = "by-hand".into();
        claims.exp = 4_102_444_800;
        let token = encode_test_claims(&claims);
        assert!(decode_header(&token).unwrap().kid.is_none());
        assert!(validate_token(&bearer_headers(&token), &make_config()).is_ok());

        // ...and not against a previous one
        let rotated = config_with(OLD_SECRET)
            .with_previous_secrets(&[TEST_SECRET.to_string()])
            .unwrap();
        assert!(validate_token(&bearer_headers(&token), &rotated).is_err());
    }

    #[test]
    fn previous_secrets_must_be_long_enough() {
        let result = make_config().with_previous_secrets(&["short".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn admin_is_a_scope_of_its_own() {
        let config = make_config();
        let admin = config.issue_token("root", "admin", 60, None).unwrap().token;
        let access = validate_token(&bearer_headers(&admin), &config).unwrap();
        assert!(access.can_admin);
        assert!(!access.can_read && !access.can_write && !access.can_delete);

        for scope in ["read write", "read write delete", "readwrite"] {
            let token = config.issue_token("user", scope, 60, None).unwrap().token;
            assert!(
                !validate_token(&bearer_headers(&token), &config)
                    .unwrap()
                    .can_admin
            );
        }
        // No scope claim means the default, without admin
        let mut claims = default_claims();
        claims.sub = "x".into();
        claims.exp = 4_102_444_800;
        let access =
            validate_token(&bearer_headers(&encode_test_claims(&claims)), &config).unwrap();
        assert!(!access.can_admin);
    }

    // -- token requests --

    const CLI_RULES: TokenRules = TokenRules {
        max_duration_seconds: 10 * 365 * 24 * 3600,
        allow_admin: true,
    };
    const ENDPOINT_RULES: TokenRules = TokenRules {
        max_duration_seconds: 365 * 24 * 3600,
        allow_admin: false,
    };

    #[test]
    fn parses_scope_combinations() {
        assert_eq!(parse_scope("read").unwrap(), "read");
        assert_eq!(parse_scope("readwrite").unwrap(), "read write");
        assert_eq!(parse_scope("read write").unwrap(), "read write");
        assert_eq!(parse_scope("read,write").unwrap(), "read write");
        assert_eq!(parse_scope("delete").unwrap(), "delete");
        assert_eq!(parse_scope("admin").unwrap(), "admin");
        assert_eq!(
            parse_scope("readwrite,delete").unwrap(),
            "read write delete"
        );
        assert_eq!(parse_scope("read,read").unwrap(), "read");
    }

    #[test]
    fn rejects_unknown_or_empty_scopes() {
        assert!(parse_scope("root").is_err());
        assert!(parse_scope("read,root").is_err());
        assert!(parse_scope("").is_err());
        assert!(parse_scope(" , ").is_err());
    }

    #[test]
    fn a_valid_request_is_cleaned() {
        let request = TokenRequest::new(
            "  edge-7 ",
            "readwrite",
            Some(vec![" temp ".into(), "humidity".into(), "temp".into()]),
            3600,
            &ENDPOINT_RULES,
        )
        .unwrap();
        assert_eq!(request.subject, "edge-7");
        assert_eq!(request.scope, "read write");
        assert_eq!(
            request.sensors,
            Some(vec!["temp".to_string(), "humidity".to_string()])
        );
        assert_eq!(request.duration_seconds, 3600);
    }

    #[test]
    fn requests_are_checked() {
        let ask = |subject: &str, scope: &str, sensors: Option<Vec<String>>, duration: u64| {
            TokenRequest::new(subject, scope, sensors, duration, &ENDPOINT_RULES)
        };
        assert!(ask("", "read", None, 60).is_err());
        assert!(ask("  ", "read", None, 60).is_err());
        assert!(ask(&"x".repeat(129), "read", None, 60).is_err());
        assert!(ask(&"x".repeat(128), "read", None, 60).is_ok());
        assert!(ask("a", "read", None, 0).is_err());
        assert!(ask("a", "read", None, ENDPOINT_RULES.max_duration_seconds).is_ok());
        assert!(ask("a", "read", None, ENDPOINT_RULES.max_duration_seconds + 1).is_err());
        assert!(ask("a", "read", Some(vec![]), 60).is_err());
        assert!(ask("a", "read", Some(vec!["".into()]), 60).is_err());
        assert!(ask("a", "read", Some(vec!["x".repeat(257)]), 60).is_err());
        assert!(ask("a", "read", Some(vec!["s".into(); 1001]), 60).is_err());
    }

    #[test]
    fn only_the_command_line_makes_admin_tokens() {
        assert_eq!(
            TokenRequest::new("a", "read,admin", None, 60, &ENDPOINT_RULES),
            Err(TokenRequestError::AdminNotAllowed)
        );
        assert!(TokenRequest::new("a", "admin", None, 60, &CLI_RULES).is_ok());
    }

    #[test]
    fn a_request_is_signed_as_asked() {
        let config = make_config();
        let request = TokenRequest::new(
            "edge-7",
            "write",
            Some(vec!["temp".into()]),
            120,
            &CLI_RULES,
        )
        .unwrap();
        let issued = request.issue(&config).unwrap();
        let access = validate_token(&bearer_headers(&issued.token), &config).unwrap();
        assert_eq!(access.subject, "edge-7");
        assert!(access.can_write && !access.can_read);
        assert!(access.can_access_sensor("temp") && !access.can_access_sensor("other"));
    }

    #[test]
    fn a_huge_duration_does_not_overflow() {
        assert!(
            make_config()
                .issue_token("a", "read", u64::MAX, None)
                .is_err()
        );
    }

    // -- startup mode --

    const LOOPBACK: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
    const EXPOSED: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED);

    #[test]
    fn a_configured_secret_wins_everywhere() {
        for endpoint in [LOOPBACK, EXPOSED] {
            for disabled in [false, true] {
                let mode = resolve_auth_mode(Some(TEST_SECRET), disabled, endpoint).unwrap();
                assert_eq!(mode, AuthMode::Secret(TEST_SECRET.to_string()));
            }
        }
    }

    #[test]
    fn disabling_authentication_is_an_explicit_choice() {
        for endpoint in [LOOPBACK, EXPOSED] {
            let mode = resolve_auth_mode(None, true, endpoint).unwrap();
            assert_eq!(mode, AuthMode::Disabled);
        }
    }

    #[test]
    fn loopback_without_secret_makes_one_for_the_run() {
        for endpoint in [
            LOOPBACK,
            std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
        ] {
            let AuthMode::Ephemeral(secret) = resolve_auth_mode(None, false, endpoint).unwrap()
            else {
                panic!("expected an ephemeral secret on {endpoint}");
            };
            // It is usable as a signing secret
            AuthConfig::from_secret(&secret).expect("the made secret is accepted");
        }
    }

    #[test]
    fn exposed_address_without_secret_refuses_to_start() {
        for endpoint in [
            EXPOSED,
            "192.168.1.10".parse().unwrap(),
            "::".parse().unwrap(),
        ] {
            let error = resolve_auth_mode(None, false, endpoint).expect_err("must refuse");
            let message = error.to_string();
            assert!(message.contains("SENSAPP_JWT_SECRET"), "{message}");
            assert!(message.contains("SENSAPP_AUTH_DISABLED"), "{message}");
            assert!(message.contains("generate-secret"), "{message}");
        }
    }

    #[test]
    fn generated_secrets_are_long_and_different() {
        let first = generate_secret().unwrap();
        let second = generate_secret().unwrap();
        assert_eq!(first.len(), 48);
        assert_ne!(first, second);
        assert!(
            first
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        );
    }

    #[test]
    fn the_mode_never_prints_its_secret() {
        let mode = AuthMode::Secret(TEST_SECRET.to_string());
        assert!(!format!("{mode:?}").contains(TEST_SECRET));
    }
}
