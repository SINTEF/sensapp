use anyhow::Error;
use confique::Config;
use std::{
    net::IpAddr,
    sync::{Arc, OnceLock},
};

use self::mqtt::MqttConfig;
pub mod mqtt;

#[derive(Debug, Config)]
pub struct SensAppConfig {
    #[config(env = "SENSAPP_INSTANCE_ID", default = 0)]
    pub instance_id: u16,

    #[config(env = "SENSAPP_PORT", default = 3000)]
    pub port: u16,
    #[config(env = "SENSAPP_ENDPOINT", default = "127.0.0.1")]
    pub endpoint: IpAddr,

    #[config(env = "SENSAPP_HTTP_BODY_LIMIT", default = "10mb")]
    pub http_body_limit: String,

    #[config(env = "SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS", default = 30)]
    pub http_server_timeout_seconds: u64,

    #[config(env = "SENSAPP_MAX_INFERENCES_ROWS", default = 128)]
    #[allow(dead_code)]
    pub max_inference_rows: usize,

    #[config(env = "SENSAPP_BATCH_SIZE", default = 8192)]
    pub batch_size: usize,

    #[config(env = "SENSAPP_SENSOR_SALT", default = "sensapp")]
    pub sensor_salt: String,

    #[config(
        env = "SENSAPP_STORAGE_CONNECTION_STRING",
        default = "postgres://postgres:postgres@localhost:5432/sensapp"
    )]
    pub storage_connection_string: String,

    #[config(env = "SENSAPP_MQTT")]
    pub mqtt: Option<Vec<MqttConfig>>,

    #[config(env = "SENSAPP_SENTRY_DSN")]
    pub sentry_dsn: Option<String>,

    #[config(env = "SENSAPP_STORAGE_SYNC_TIMEOUT_SECONDS", default = 15)]
    pub storage_sync_timeout_seconds: u64,
}

impl SensAppConfig {
    pub fn load() -> Result<SensAppConfig, Error> {
        let c = SensAppConfig::builder()
            .env()
            .file("settings.toml")
            .load()?;

        Ok(c)
    }

    pub fn parse_http_body_limit(&self) -> Result<usize, Error> {
        let size = byte_unit::Byte::parse_str(self.http_body_limit.clone(), true)?.as_u64();
        if size > 128 * 1024 * 1024 * 1024 {
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
#[allow(dead_code)] // Used by integration tests, not visible in cargo check
static TEST_CONFIG_INIT: Mutex<()> = Mutex::new(());

/// Test-only function to ensure configuration is loaded exactly once per test run
/// Available for both unit tests and integration tests
#[allow(dead_code)] // Used by integration tests, not visible in cargo check
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

        temp_env::with_var("SENSAPP_PORT", Some("8080"), || {
            let config = SensAppConfig::load().unwrap();
            assert_eq!(config.port, 8080);
        });
    }

    #[test]
    fn test_parse_http_body_limit() {
        let config = SensAppConfig::load().unwrap();
        assert_eq!(config.parse_http_body_limit().unwrap(), 10000000);

        temp_env::with_var("SENSAPP_HTTP_BODY_LIMIT", Some("12345"), || {
            let config = SensAppConfig::load().unwrap();
            assert_eq!(config.parse_http_body_limit().unwrap(), 12345);
        });

        temp_env::with_var("SENSAPP_HTTP_BODY_LIMIT", Some("10m"), || {
            let config = SensAppConfig::load().unwrap();
            assert_eq!(config.parse_http_body_limit().unwrap(), 10000000);
        });

        temp_env::with_var("SENSAPP_HTTP_BODY_LIMIT", Some("10mb"), || {
            let config = SensAppConfig::load().unwrap();
            assert_eq!(config.parse_http_body_limit().unwrap(), 10000000);
        });

        temp_env::with_var("SENSAPP_HTTP_BODY_LIMIT", Some("10MiB"), || {
            let config = SensAppConfig::load().unwrap();
            assert_eq!(config.parse_http_body_limit().unwrap(), 10485760);
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
        assert!(SENSAPP_CONFIG.get().is_none());
        load_configuration().unwrap();
        assert!(SENSAPP_CONFIG.get().is_some());

        let config = get().unwrap();
        assert_eq!(config.port, 3000);
    }
}
