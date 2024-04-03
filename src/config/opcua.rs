use opcua::client;
use opcua::client::prelude::{
    ClientBuilder, ClientEndpoint, ClientUserToken, ANONYMOUS_USER_TOKEN_ID,
};
use opcua::types::{MessageSecurityMode, UserTokenPolicy};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};
use serde_bytes::ByteBuf;
use serde_inline_default::serde_inline_default;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Deserialize, Clone)]
pub struct OpcuaUserTokenConfig {
    pub id: String,
    pub user: String,
    pub password: Option<String>,
    pub cert_path: Option<String>,
    pub private_key_path: Option<String>,
}

impl From<OpcuaUserTokenConfig> for ClientUserToken {
    fn from(config: OpcuaUserTokenConfig) -> ClientUserToken {
        ClientUserToken {
            user: config.user,
            password: config.password,
            cert_path: config.cert_path,
            private_key_path: config.private_key_path,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum OpcuaIdentifier {
    Int(u32),
    String(String),
    Tagged(TaggedOpaqueIdentifier),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum TaggedOpaqueIdentifier {
    Int { identifier: u32 },
    String { identifier: String },
    Guid { identifier: uuid::Uuid },
    Binary { identifier: ByteBuf },
}

impl From<OpcuaIdentifier> for opcua::types::Identifier {
    fn from(identifier: OpcuaIdentifier) -> opcua::types::Identifier {
        match identifier {
            OpcuaIdentifier::Int(id) => opcua::types::Identifier::Numeric(id),
            OpcuaIdentifier::String(id) => opcua::types::Identifier::String(id.into()),
            OpcuaIdentifier::Tagged(tagged) => match tagged {
                TaggedOpaqueIdentifier::Int { identifier } => {
                    opcua::types::Identifier::Numeric(identifier)
                }
                TaggedOpaqueIdentifier::String { identifier } => {
                    opcua::types::Identifier::String(identifier.into())
                }
                TaggedOpaqueIdentifier::Guid { identifier } => {
                    opcua::types::Identifier::Guid(identifier.into())
                }
                TaggedOpaqueIdentifier::Binary { identifier } => {
                    opcua::types::Identifier::ByteString(identifier.into_vec().into())
                }
            },
        }
    }
}

#[serde_inline_default]
#[derive(Debug, Deserialize, Clone)]
pub struct OpcuaSubscriptionConfig {
    // The subscription namespace
    pub namespace: u16,

    // Prefix of the names, optional
    pub name_prefix: Option<String>,

    // The subscription identifiers
    pub identifiers: Vec<OpcuaIdentifier>,

    // Publishing interval
    #[serde_inline_default(1000.0f64)]
    pub publishing_interval: f64,

    // Lifetime count
    #[serde_inline_default(10u32)]
    pub lifetime_count: u32,

    // Keep alive count
    #[serde_inline_default(32u32)]
    pub max_keep_alive_count: u32,

    // Max notifications per publish
    #[serde_inline_default(0u32)]
    pub max_notifications_per_publish: u32,

    // Priority
    #[serde_inline_default(0u8)]
    pub priority: u8,
}

#[serde_inline_default]
#[derive(Debug, Deserialize, Clone)]
pub struct OpcuaConfig {
    // Enable OPCUA logging
    #[serde_inline_default(true)]
    pub logging: bool,

    // The application name
    #[serde_inline_default("unnamed sensapp opcua".to_string())]
    pub application_name: String,

    // The application uri
    #[serde_inline_default("urn:localhost:OPCUA:unnamed_sensapp".to_string())]
    pub application_uri: String,

    // The product uri
    #[serde_inline_default("urn:localhost:OPCUA:unnamed_sensapp".to_string())]
    pub product_uri: String,

    #[serde_inline_default("opc.tcp://localhost:4840".to_string())]
    pub endpoint: String,

    // Whether the client should generate its own key pair if there is none
    // found in the pki directory.
    pub create_sample_keypair: Option<bool>,

    // Custom client certificate path, in .der or .pem format.
    // Must be a partial path relative to the PKI directory.
    pub certificate_path: Option<String>,

    // Custom private key path.
    // Must be a partial path relative to the PKI directory.
    pub private_key_path: Option<String>,

    // Whether the client should automatically trust servers.
    // Set to true only for testing and development, of course.
    pub trust_server_certs: Option<bool>,

    // Whether the client should verify the server certs.
    // Set to false only for testing and development, of course.
    pub verify_server_certs: Option<bool>,

    // PKI directory where client's own key pair is stored
    // and where `/trusted` and `/rejected` server certificates are stored.
    #[serde_inline_default("pki".to_string())]
    pub pki_dir: String,

    // Preferred locales of the client.
    pub preferred_locales: Option<Vec<String>>,

    // User Token.
    pub user_token: Option<OpcuaUserTokenConfig>,

    // Session retry limit.
    #[serde_inline_default(3u32)]
    pub session_retry_limit: u32,

    // Session retry interval in milliseconds.
    pub session_retry_interval: Option<u32>,

    // Session itemout in milliseconds.
    pub session_timeout: Option<u32>,

    // Ignore when the clocks are out of sync.
    pub ignore_clock_skew: Option<bool>,

    // Set multithread executor
    // Disabled for now as it's unclear whether we need it.
    // #[serde_inline_default(false)]
    // pub multithread_executor: bool,

    // Maximum message size
    pub max_message_size: Option<usize>,

    // Maximum chunk count
    pub max_chunk_count: Option<usize>,

    // Security policy
    // Can be None, Basic128Rsa15, Basic256, Basic256Sha256,
    // Aes128-Sha256-RsaOaep, Aes256-Sha256-RsaPss.
    pub security_policy: String,

    // Message security mode
    // Can be None, Sign, SignAndEncrypt.
    pub security_mode: String,

    // Opcua subscriptions
    pub subscriptions: Vec<OpcuaSubscriptionConfig>,
}

impl From<OpcuaConfig> for ClientBuilder {
    fn from(config: OpcuaConfig) -> ClientBuilder {
        let (user_token_id, user_token) = match config.user_token {
            Some(user_token) => {
                let id = user_token.id.clone();
                let token: ClientUserToken = user_token.into();
                (id, Some(token))
            }
            None => (ANONYMOUS_USER_TOKEN_ID.to_string(), None),
        };

        let client_endpoint = ClientEndpoint {
            url: config.endpoint,
            security_policy: config.security_policy,
            security_mode: config.security_mode,
            user_token_id: user_token_id.clone(),
        };

        let mut client_builder = ClientBuilder::new()
            .application_name(config.application_name)
            .application_uri(config.application_uri)
            .product_uri(config.product_uri)
            .pki_dir(config.pki_dir)
            .session_retry_limit(config.session_retry_limit as i32)
            // We support only one endpoint for now.
            .endpoint("default", client_endpoint)
            .default_endpoint("default");

        if let Some(user_token) = user_token {
            client_builder = client_builder.user_token(user_token_id, user_token);
        }

        if let Some(create_sample_keypair) = config.create_sample_keypair {
            client_builder = client_builder.create_sample_keypair(create_sample_keypair);
        }

        if let Some(certificate_path) = config.certificate_path {
            client_builder = client_builder.certificate_path(certificate_path);
        }

        if let Some(private_key_path) = config.private_key_path {
            client_builder = client_builder.private_key_path(private_key_path);
        }

        if let Some(trust_server_certs) = config.trust_server_certs {
            client_builder = client_builder.trust_server_certs(trust_server_certs);
        }

        if let Some(verify_server_certs) = config.verify_server_certs {
            client_builder = client_builder.verify_server_certs(verify_server_certs);
        }

        if let Some(preferred_locales) = config.preferred_locales {
            client_builder = client_builder.preferred_locales(preferred_locales);
        }

        /*if let Some(user_token) = config.user_token {
            let id = user_token.id.clone();
            client_builder = client_builder.user_token(id, user_token.into());
        }*/

        if let Some(session_retry_interval) = config.session_retry_interval {
            client_builder = client_builder.session_retry_interval(session_retry_interval);
        }

        if let Some(session_timeout) = config.session_timeout {
            client_builder = client_builder.session_timeout(session_timeout);
        }

        if let Some(ignore_clock_skew) = config.ignore_clock_skew {
            if ignore_clock_skew {
                client_builder = client_builder.ignore_clock_skew();
            }
        }

        if let Some(max_message_size) = config.max_message_size {
            client_builder = client_builder.max_message_size(max_message_size);
        }

        if let Some(max_chunk_count) = config.max_chunk_count {
            client_builder = client_builder.max_chunk_count(max_chunk_count);
        }

        client_builder
    }
}
