use serde::Deserialize;
use serde_inline_default::serde_inline_default;

#[cfg(feature = "mqtt")]
#[derive(Debug, Deserialize, Clone)]
pub struct MqttSubscription {
    pub topic: String,
    pub qos: u8,
}

#[cfg(feature = "mqtt")]
#[serde_inline_default]
#[derive(Debug, Deserialize, Clone)]
pub struct MqttConfig {
    pub url: String,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,

    #[serde_inline_default(30)]
    pub keep_alive_seconds: u64,
    // pub topics: Vec<String>,
    // pub qos: u8,
    // pub retain: bool,
    // pub clean_session: bool,
    // pub keep_alive: u16,
}
