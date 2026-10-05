use anyhow::Error;
use confique::Config;
use std::{
    net::IpAddr,
    sync::{Arc, OnceLock},
};

const MAX_HTTP_BODY_LIMIT_BYTES: u64 = 128 * 1024 * 1024 * 1024;

#[derive(Debug, Config)]
pub struct SensAppConfig {
    #[config(env = "SENSAPP_PORT", default = 3000)]
    pub port: u16,
    #[config(env = "SENSAPP_ENDPOINT", default = "127.0.0.1")]
    pub endpoint: IpAddr,

    #[config(env = "SENSAPP_HTTP_BODY_LIMIT", default = "64MiB")]
    pub http_body_limit: String,

    /// Time a request may take before it is answered with a 504, the reads and everything except
    /// the writes and the maintenance. A read of raw samples is bounded (100,000 samples), but an
    /// aggregation scans the series: it takes seconds on a series of 100 million samples.
    #[config(env = "SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS", default = 120)]
    pub http_server_timeout_seconds: u64,

    /// Time a write request (`/publish`, InfluxDB and Prometheus writes) may take before it is
    /// answered with a 504. The work of a write grows with its body, which
    /// `SENSAPP_HTTP_BODY_LIMIT` bounds, and a backfill is slower than a read: 64 MiB of line
    /// protocol into TimescaleDB took about a minute in the slowest case we measured.
    #[config(env = "SENSAPP_HTTP_WRITE_TIMEOUT_SECONDS", default = 300)]
    pub http_write_timeout_seconds: u64,

    /// Time the maintenance request (`POST /api/v1/admin/vacuum`) may take before it is answered
    /// with a 504. Removing duplicates scans every value table, so it is much longer than the
    /// timeout of the other requests.
    #[config(env = "SENSAPP_HTTP_MAINTENANCE_TIMEOUT_SECONDS", default = 3600)]
    pub http_maintenance_timeout_seconds: u64,

    /// Maximum number of write requests handled at the same time (`/publish`, InfluxDB and
    /// Prometheus writes, admin). Extra writes get `503` with `Retry-After`. `0` disables the limit.
    #[config(env = "SENSAPP_HTTP_MAX_CONCURRENT_WRITES", default = 16)]
    pub http_max_concurrent_writes: usize,

    /// Serve the web UI under `/ui/`, and redirect `/` to it. The UI is only the static files of
    /// `frontend/`: the data stays behind the same authentication as the API.
    #[config(env = "SENSAPP_UI_ENABLED", default = true)]
    pub ui_enabled: bool,

    /// Directory with the built UI (`frontend/dist`). The container image sets it to where it
    /// ships the files. When it has no `index.html` the UI is not served and a warning is logged.
    #[config(env = "SENSAPP_UI_DIR", default = "frontend/dist")]
    pub ui_dir: String,

    #[config(env = "SENSAPP_MAX_INFERENCES_ROWS", default = 128)]
    pub max_inference_rows: usize,

    #[config(env = "SENSAPP_BATCH_SIZE", default = 8192)]
    pub batch_size: usize,

    /// Do not write the samples that are stored already (same series, same timestamp, same value),
    /// and write the repeated samples of a request once. Off by default: it costs a few
    /// milliseconds per write, see docs/DATA_LIFECYCLE.md. The server refuses to start when the
    /// storage backend cannot do it.
    #[config(env = "SENSAPP_DEDUPLICATE_ON_INGEST", default = false)]
    pub deduplicate_on_ingest: bool,

    #[config(env = "SENSAPP_SENSOR_SALT", default = "sensapp")]
    pub sensor_salt: String,

    #[config(
        env = "SENSAPP_STORAGE_CONNECTION_STRING",
        default = "postgres://postgres:postgres@localhost:5432/sensapp"
    )]
    pub storage_connection_string: String,

    #[config(env = "SENSAPP_SENTRY_DSN")]
    pub sentry_dsn: Option<String>,

    #[config(env = "SENSAPP_INFLUXDB_WITH_NUMERIC", default = false)]
    pub influxdb_with_numeric: bool,

    /// Secret that signs and verifies the JWTs. Must be at least 32 characters long
    /// (`sensapp generate-secret` makes one). When set, the protected endpoints require a valid
    /// JWT bearer token.
    ///
    /// When unset, SensApp does not run open by default: on a loopback address it makes a random
    /// secret for this run and prints an admin token, on any other address it refuses to start,
    /// unless `SENSAPP_AUTH_DISABLED` is true.
    #[config(env = "SENSAPP_JWT_SECRET")]
    pub jwt_secret: Option<String>,

    /// Previous secrets, separated by commas, that still verify tokens but never sign them: the
    /// way to rotate `SENSAPP_JWT_SECRET`. Put the new secret in `SENSAPP_JWT_SECRET` and the
    /// old one here until its tokens have expired, then remove it. Dropping a secret refuses
    /// every token it signed, which is also how tokens are revoked.
    #[config(env = "SENSAPP_JWT_PREVIOUS_SECRETS")]
    pub jwt_previous_secrets: Option<String>,

    /// Run without authentication: every endpoint is open. The explicit opt-out for demos and
    /// networks that authenticate in front of SensApp. Ignored when `SENSAPP_JWT_SECRET` is set.
    #[config(env = "SENSAPP_AUTH_DISABLED", default = false)]
    pub auth_disabled: bool,
}

impl SensAppConfig {
    pub fn load() -> Result<SensAppConfig, Error> {
        // Get settings file path from environment variable or use default
        let settings_file =
            std::env::var("SENSAPP_SETTINGS_FILE").unwrap_or_else(|_| "settings.toml".to_string());

        let c = SensAppConfig::builder().env().file(settings_file).load()?;

        Ok(c)
    }

    /// The secrets of `SENSAPP_JWT_PREVIOUS_SECRETS`, one per comma-separated item.
    pub fn previous_jwt_secrets(&self) -> Vec<String> {
        self.jwt_previous_secrets
            .as_deref()
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|secret| !secret.is_empty())
            .map(str::to_string)
            .collect()
    }

    pub fn parse_http_body_limit(&self) -> Result<usize, Error> {
        let size = byte_unit::Byte::parse_str(self.http_body_limit.clone(), true)?.as_u64();
        if size > MAX_HTTP_BODY_LIMIT_BYTES {
            anyhow::bail!("Body size is too big: > 128GB");
        }
        Ok(size as usize)
    }
}

static SENSAPP_CONFIG: OnceLock<Arc<SensAppConfig>> = OnceLock::new();

pub fn get() -> Result<Arc<SensAppConfig>, Error> {
    SENSAPP_CONFIG.get().cloned().ok_or_else(|| {
        Error::msg(
            "Configuration not loaded. Please call load_configuration() before using the configuration",
        )
    })
}

pub fn load_configuration() -> Result<(), Error> {
    // Check if the configuration has already been loaded
    if SENSAPP_CONFIG.get().is_some() {
        return Ok(());
    }

    // Load configuration
    let config = SensAppConfig::load()?;
    SENSAPP_CONFIG.get_or_init(|| Arc::new(config));

    Ok(())
}

use std::sync::Mutex;

// Used by integration tests - must be always available for test compilation
static TEST_CONFIG_INIT: Mutex<()> = Mutex::new(());

/// Test-only function to ensure configuration is loaded exactly once per test run
/// Available for both unit tests and integration tests
pub fn load_configuration_for_tests() -> Result<(), Error> {
    let _guard = TEST_CONFIG_INIT.lock().unwrap();

    // If config is already loaded, return success
    if SENSAPP_CONFIG.get().is_some() {
        return Ok(());
    }

    // Load default configuration for tests
    let config = SensAppConfig::load()?;
    SENSAPP_CONFIG.get_or_init(|| Arc::new(config));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_config() {
        let config = SensAppConfig::load().unwrap();

        assert_eq!(config.port, 3000);
        assert_eq!(config.endpoint, IpAddr::from([127, 0, 0, 1]));
        assert!(config.ui_enabled);
        assert_eq!(config.ui_dir, "frontend/dist");

        temp_env::with_var("SENSAPP_PORT", Some("8080"), || {
            let config = SensAppConfig::load().unwrap();
            assert_eq!(config.port, 8080);
        });
    }

    #[test]
    fn test_previous_jwt_secrets() {
        temp_env::with_var("SENSAPP_JWT_PREVIOUS_SECRETS", None::<&str>, || {
            assert!(
                SensAppConfig::load()
                    .unwrap()
                    .previous_jwt_secrets()
                    .is_empty()
            );
        });
        temp_env::with_var("SENSAPP_JWT_PREVIOUS_SECRETS", Some(" one ,two,, "), || {
            let secrets = SensAppConfig::load().unwrap().previous_jwt_secrets();
            assert_eq!(secrets, vec!["one".to_string(), "two".to_string()]);
        });
    }

    #[test]
    fn test_parse_http_body_limit() {
        let config = SensAppConfig::load().unwrap();
        assert_eq!(config.http_body_limit, "64MiB");
        assert_eq!(config.parse_http_body_limit().unwrap(), 67108864);

        temp_env::with_var("SENSAPP_HTTP_BODY_LIMIT", Some("12345"), || {
            let config = SensAppConfig::load().unwrap();
            assert_eq!(config.parse_http_body_limit().unwrap(), 12345);
        });

        temp_env::with_var("SENSAPP_HTTP_BODY_LIMIT", Some("64m"), || {
            let config = SensAppConfig::load().unwrap();
            assert_eq!(config.parse_http_body_limit().unwrap(), 64000000);
        });

        temp_env::with_var("SENSAPP_HTTP_BODY_LIMIT", Some("64mb"), || {
            let config = SensAppConfig::load().unwrap();
            assert_eq!(config.parse_http_body_limit().unwrap(), 64000000);
        });

        temp_env::with_var("SENSAPP_HTTP_BODY_LIMIT", Some("64MiB"), || {
            let config = SensAppConfig::load().unwrap();
            assert_eq!(config.parse_http_body_limit().unwrap(), 67108864);
        });

        temp_env::with_var("SENSAPP_HTTP_BODY_LIMIT", Some("1.5gb"), || {
            let config = SensAppConfig::load().unwrap();
            assert_eq!(config.parse_http_body_limit().unwrap(), 1500000000);
        });

        temp_env::with_var("SENSAPP_HTTP_BODY_LIMIT", Some("1tb"), || {
            let config = SensAppConfig::load().unwrap();
            assert!(config.parse_http_body_limit().is_err());
        });

        temp_env::with_var("SENSAPP_HTTP_BODY_LIMIT", Some("-5mb"), || {
            let config = SensAppConfig::load().unwrap();
            assert!(config.parse_http_body_limit().is_err());
        });
    }

    #[test]
    fn test_load_configuration() {
        load_configuration().unwrap();
        load_configuration().unwrap();
        assert!(SENSAPP_CONFIG.get().is_some());

        let config = get().unwrap();
        let config_again = get().unwrap();
        assert!(Arc::ptr_eq(&config, &config_again));
    }

    #[test]
    fn test_custom_settings_file() {
        // Test that SENSAPP_SETTINGS_FILE environment variable works
        temp_env::with_var("SENSAPP_SETTINGS_FILE", Some("settings.toml"), || {
            let config = SensAppConfig::load();
            assert!(config.is_ok(), "Should load settings.toml when specified");
        });

        // Test with non-existent file
        temp_env::with_var("SENSAPP_SETTINGS_FILE", Some("non-existent.toml"), || {
            let config = SensAppConfig::load();
            // This will still work because confique allows missing files
            // and will use environment variables and defaults
            assert!(config.is_ok(), "Should handle missing file gracefully");
        });
    }
}
