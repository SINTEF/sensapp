use anyhow::Error;
use confique::Config;
use std::{
    net::IpAddr,
    sync::{Arc, OnceLock},
};

#[derive(Debug, Config)]
pub struct SensAppConfig {
    #[config(env = "SENSAPP_PORT", default = 3000)]
    pub port: u16,
    #[config(env = "SENSAPP_ENDPOINT", default = "127.0.0.1")]
    pub endpoint: IpAddr,

    #[config(env = "SENSAPP_HTTP_BODY_LIMIT", default = "10mb")]
    pub http_body_limit: String,

    #[config(env = "SENSAPP_MAX_INFERENCES_ROWS", default = 128)]
    pub max_inference_rows: usize,

    #[config(env = "SENSAPP_BATCH_SIZE", default = 8192)]
    pub batch_size: usize,

    #[config(env = "SENSAPP_SENSOR_SALT", default = "sensapp")]
    pub sensor_salt: String,

    #[config(env = "SENSAPP_SQLITE_CONNECTION_STRING")]
    pub sqlite_connection_string: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_config() {
        let config = SensAppConfig::load().unwrap();

        assert_eq!(config.port, 3000);
        assert_eq!(config.endpoint, IpAddr::from([127, 0, 0, 1]));

        // set env PORT
        std::env::set_var("SENSAPP_PORT", "8080");
        let config = SensAppConfig::load().unwrap();
        assert_eq!(config.port, 8080);
    }

    #[test]
    fn test_parse_http_body_limit() {
        let config = SensAppConfig::load().unwrap();
        assert_eq!(config.parse_http_body_limit().unwrap(), 10000000);

        std::env::set_var("SENSAPP_HTTP_BODY_LIMIT", "12345");
        let config = SensAppConfig::load().unwrap();
        assert_eq!(config.parse_http_body_limit().unwrap(), 12345);

        std::env::set_var("SENSAPP_HTTP_BODY_LIMIT", "10m");
        let config = SensAppConfig::load().unwrap();
        assert_eq!(config.parse_http_body_limit().unwrap(), 10000000);

        std::env::set_var("SENSAPP_HTTP_BODY_LIMIT", "10mb");
        let config = SensAppConfig::load().unwrap();
        assert_eq!(config.parse_http_body_limit().unwrap(), 10000000);

        std::env::set_var("SENSAPP_HTTP_BODY_LIMIT", "10MiB");
        let config = SensAppConfig::load().unwrap();
        assert_eq!(config.parse_http_body_limit().unwrap(), 10485760);

        std::env::set_var("SENSAPP_HTTP_BODY_LIMIT", "1.5gb");
        let config = SensAppConfig::load().unwrap();
        assert_eq!(config.parse_http_body_limit().unwrap(), 1500000000);

        std::env::set_var("SENSAPP_HTTP_BODY_LIMIT", "1tb");
        let config = SensAppConfig::load().unwrap();
        assert!(config.parse_http_body_limit().is_err());

        std::env::set_var("SENSAPP_HTTP_BODY_LIMIT", "-5mb");
        let config = SensAppConfig::load().unwrap();
        assert!(config.parse_http_body_limit().is_err());
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
