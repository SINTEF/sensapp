use opcua::client::prelude::{
    ClientBuilder, ClientEndpoint, ClientUserToken, ANONYMOUS_USER_TOKEN_ID,
};
use serde::Deserialize;
use serde_bytes::ByteBuf;
use serde_inline_default::serde_inline_default;

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
pub struct OpcuaAutoDiscovery {
    #[serde_inline_default(true)]
    pub enabled: bool,

    // Start with the root node by default
    pub start_node: Option<OpcuaIdentifier>,

    // List of nodes to exclude from the discovery.
    #[serde_inline_default(Vec::new())]
    pub excluded_nodes: Vec<OpcuaIdentifier>,

    // Maximum discovery depth
    #[serde_inline_default(32)]
    pub max_depth: usize,

    // Maximum number of nodes to discover.
    #[serde_inline_default(1024)]
    pub max_nodes: usize,

    // Regular expression to filter out nodes based on their browse name.
    pub node_browse_name_exclude_regex: Option<String>,

    // Regular expression to select variables based on their node id identifier.
    // If it's not a string, it uses the string representation of the identifier.
    pub variable_identifier_include_regex: Option<String>,

    // Allow browsing accross namespaces
    #[serde_inline_default(false)]
    pub discover_across_namespaces: bool,

    // Filter out variables that have sub nodes.
    #[serde_inline_default(true)]
    pub skip_variables_with_children: bool,
}

#[serde_inline_default]
#[derive(Debug, Deserialize, Clone)]
pub struct OpcuaSubscriptionConfig {
    // The subscription namespace
    pub namespace: u16,

    // Prefix of the names, optional
    pub name_prefix: Option<String>,

    // The subscription identifiers
    #[serde_inline_default(Vec::new())]
    pub identifiers: Vec<OpcuaIdentifier>,

    // Autodiscovery feature
    pub autodiscovery: Option<OpcuaAutoDiscovery>,

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

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, str::FromStr};

    use super::*;
    use opcua::{client::prelude::ClientBuilder, types::Identifier};
    use serde_json::json;

    #[test]
    fn test_opcua_config_to_client_builder() {
        // Create a sample OpcuaConfig
        let opcua_config: OpcuaConfig = serde_json::from_value(json!({
            "logging": true,
            "application_name": "Test Application",
            "application_uri": "urn:test:application",
            "product_uri": "urn:test:product",
            "endpoint": "opc.tcp://test-server:4840",
            "create_sample_keypair": true,
            "certificate_path": "client_cert.der",
            "private_key_path": "client_key.pem",
            "trust_server_certs": true,
            "verify_server_certs": false,
            "pki_dir": "test_pki",
            "preferred_locales": ["en-US", "de-DE"],
            "user_token": {
                "id": "test_user",
                "user": "testuser",
                "password": "testpassword" // gitleaks:allow
            },
            "session_retry_limit": 5,
            "session_retry_interval": 1000,
            "session_timeout": 30000,
            "ignore_clock_skew": true,
            "max_message_size": 1048576,
            "max_chunk_count": 1024,
            "security_policy": "Basic256",
            "security_mode": "SignAndEncrypt",
            "subscriptions": []
        }))
        .expect("Failed to parse OpcuaConfig");

        // Convert OpcuaConfig to ClientBuilder
        let client_builder: ClientBuilder = opcua_config.into();

        let config = client_builder.config();

        // Assert the properties of the ClientBuilder
        assert_eq!(config.application_name, "Test Application");
        assert_eq!(config.application_uri, "urn:test:application");
        assert_eq!(config.product_uri, "urn:test:product");
        assert_eq!(config.pki_dir, PathBuf::from_str("test_pki").unwrap());
        assert_eq!(config.session_retry_limit, 5);

        // Assert the endpoints
        let endpoints = config.endpoints;
        assert_eq!(endpoints.len(), 1);
        let endpoint = endpoints.get("default").unwrap();
        assert_eq!(endpoint.url, "opc.tcp://test-server:4840");
        assert_eq!(endpoint.security_policy, "Basic256");
        assert_eq!(endpoint.security_mode, "SignAndEncrypt");
        assert_eq!(endpoint.user_token_id, "test_user");

        // Assert the user token
        let user_tokens = config.user_tokens;
        assert_eq!(user_tokens.len(), 1);
        let user_token = user_tokens.get("test_user").unwrap();
        assert_eq!(user_token.user, "testuser");
        assert_eq!(user_token.password, Some("testpassword".to_string()));

        // Assert other properties
        assert!(config.create_sample_keypair);
        assert_eq!(
            config.certificate_path,
            Some(PathBuf::from("client_cert.der"))
        );
        assert_eq!(
            config.private_key_path,
            Some(PathBuf::from("client_key.pem"))
        );
        assert!(config.trust_server_certs);
        assert!(!config.verify_server_certs);
        assert_eq!(
            config.preferred_locales,
            vec!["en-US".to_string(), "de-DE".to_string()]
        );
        assert_eq!(config.session_retry_interval, 1000);
        assert_eq!(config.session_timeout, 30000);
        assert!(config.performance.ignore_clock_skew);
        assert_eq!(config.decoding_options.max_message_size, 1048576);
        assert_eq!(config.decoding_options.max_chunk_count, 1024);
    }

    #[test]
    fn test_opcua_config_to_client_builder_with_defaults() {
        // Create a minimal OpcuaConfig with only the required fields
        let opcua_config: OpcuaConfig = serde_json::from_value(json!({
            "endpoint": "opc.tcp://test-server:4840",
            "security_policy": "None",
            "security_mode": "None",
            "subscriptions": []
        }))
        .expect("Failed to parse OpcuaConfig");

        // Convert OpcuaConfig to ClientBuilder
        let client_builder: ClientBuilder = opcua_config.into();
        let config = client_builder.config();

        // Assert the default values of the ClientBuilder
        assert_eq!(config.application_name, "unnamed sensapp opcua");
        assert_eq!(
            config.application_uri,
            "urn:localhost:OPCUA:unnamed_sensapp"
        );
        assert_eq!(config.product_uri, "urn:localhost:OPCUA:unnamed_sensapp");
        assert_eq!(config.pki_dir, PathBuf::from_str("pki").unwrap());
        assert_eq!(config.session_retry_limit, 3);

        // Assert the endpoints
        let endpoints = config.endpoints;
        assert_eq!(endpoints.len(), 1);
        let endpoint = endpoints.get("default").unwrap();
        assert_eq!(endpoint.url, "opc.tcp://test-server:4840");
        assert_eq!(endpoint.security_policy, "None");
        assert_eq!(endpoint.security_mode, "None");
        assert_eq!(endpoint.user_token_id, ANONYMOUS_USER_TOKEN_ID);
    }

    #[test]
    fn test_opcua_identifier_conversion() {
        // Test case 1: OpcuaIdentifier::Int
        let identifier_int: OpcuaIdentifier = serde_json::from_value(json!(42)).unwrap();
        let expected_int = Identifier::Numeric(42);
        assert_eq!(Identifier::from(identifier_int), expected_int);

        // Test case 2: OpcuaIdentifier::String
        let identifier_string: OpcuaIdentifier = serde_json::from_value(json!("example")).unwrap();
        let expected_string = Identifier::String("example".into());
        assert_eq!(Identifier::from(identifier_string), expected_string);

        // Test case 3: OpcuaIdentifier::Tagged::Int
        let identifier_tagged_int: OpcuaIdentifier =
            serde_json::from_value(json!({"type": "Int", "identifier": 123})).unwrap();
        let expected_tagged_int = Identifier::Numeric(123);
        assert_eq!(Identifier::from(identifier_tagged_int), expected_tagged_int);

        // Test case 4: OpcuaIdentifier::Tagged::String
        let identifier_tagged_string: OpcuaIdentifier = serde_json::from_value(json!({
            "type": "String",
            "identifier": "tagged_example"
        }))
        .unwrap();
        let expected_tagged_string = Identifier::String("tagged_example".into());
        assert_eq!(
            Identifier::from(identifier_tagged_string),
            expected_tagged_string
        );

        // Test case 5: OpcuaIdentifier::Tagged::Guid
        let identifier_tagged_guid: OpcuaIdentifier = serde_json::from_value(json!({
            "type": "Guid",
            "identifier": "00000000-0000-0000-0000-000000000000"
        }))
        .unwrap();
        let expected_tagged_guid = Identifier::Guid(uuid::Uuid::nil().into());
        assert_eq!(
            Identifier::from(identifier_tagged_guid),
            expected_tagged_guid
        );

        // Test case 6: OpcuaIdentifier::Tagged::Binary
        let identifier_tagged_binary: OpcuaIdentifier = serde_json::from_value(json!({
            "type": "Binary",
            "identifier": [1, 2, 3]
        }))
        .unwrap();
        let expected_tagged_binary = Identifier::ByteString(vec![1, 2, 3].into());
        assert_eq!(
            Identifier::from(identifier_tagged_binary),
            expected_tagged_binary
        );
    }
}
