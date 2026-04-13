#![forbid(unsafe_code)]
use crate::config::load_configuration;
use crate::http::auth::AuthConfig;
use crate::http::metrics::HttpMetrics;
use crate::http::server::run_http_server;
use crate::http::state::HttpServerState;
use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use storage::storage_factory::create_storage_from_connection_string;
use tracing::Level;
use tracing::event;
mod config;
mod datamodel;
mod exporters;
mod http;
mod importers;
mod infer;
mod parsing;
mod storage;

fn main() -> Result<()> {
    // Handle `generate-token` subcommand before any server setup
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && args[1] == "generate-token" {
        return generate_token_command(&args[2..]);
    }

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|e| anyhow::anyhow!("Failed to install CryptoProvider: {:?}", e))?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to create Tokio runtime")?;

    runtime.block_on(async_main())
}

async fn async_main() -> Result<()> {
    // Initialize tracing subscriber for HTTP request logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=info".into()),
        )
        .init();

    // Load configuration
    load_configuration().context("Failed to load configuration")?;
    let config = config::get().context("Failed to get configuration")?;

    // Initialize Sentry if DSN is provided
    let _sentry = config.sentry_dsn.as_ref().map(|dsn| {
        sentry::init((
            dsn.clone(),
            sentry::ClientOptions {
                release: sentry::release_name!(),
                debug: true,
                ..Default::default()
            },
        ))
    });

    sinteflake::set_instance_id(config.instance_id).context("Failed to set instance ID")?;
    sinteflake::set_instance_id_async(config.instance_id)
        .await
        .context("Failed to set async instance ID")?;

    // Initialize storage backend
    println!(
        "🗄️  Connecting to storage: {}",
        config.storage_connection_string
    );
    let storage = create_storage_from_connection_string(&config.storage_connection_string)
        .await
        .context("Failed to create storage backend")?;

    // Initialize database schema
    storage
        .create_or_migrate()
        .await
        .context("Failed to create or migrate database schema")?;
    println!("✅ Storage backend initialized successfully");

    // Exit the program if a panic occurs
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    let endpoint = config.endpoint;
    let port = config.port;
    let address = SocketAddr::from((endpoint, port));

    println!("📡 Starting HTTP server on http://{}...", address);
    // Build optional JWT authentication config
    let auth = match &config.jwt_secret {
        Some(secret) => {
            let auth_config = AuthConfig::from_secret(secret)
                .context("Failed to configure JWT authentication")?;
            println!("🔐 JWT authentication enabled");
            Some(auth_config)
        }
        None => {
            println!("🔓 No JWT secret configured — all endpoints are open");
            None
        }
    };

    match run_http_server(
        HttpServerState {
            name: Arc::new("SensApp".to_string()),
            storage,
            metrics: Arc::new(HttpMetrics::new()),
            influxdb_with_numeric: config.influxdb_with_numeric,
            auth,
        },
        address,
    )
    .await
    {
        Ok(_) => {
            event!(Level::INFO, "HTTP server stopped gracefully");
            println!("✅ HTTP server stopped gracefully");
            Ok(())
        }
        Err(err) => {
            event!(Level::ERROR, "HTTP server failed to start: {}", err);
            Err(err)
        }
    }
}

/// CLI subcommand: generate a signed JWT token.
///
/// Usage: sensapp generate-token <subject> [OPTIONS]
///   --scope <read|write|readwrite>  (default: "read write")
///   --duration <seconds>            (default: 3600)
///   --sensors <name1,name2,...>      (optional sensor allow list)
fn generate_token_command(args: &[String]) -> Result<()> {
    load_configuration().context("Failed to load configuration")?;
    let config = config::get().context("Failed to get configuration")?;

    let secret = config
        .jwt_secret
        .as_deref()
        .context("SENSAPP_JWT_SECRET must be set to generate tokens")?;

    let auth_config =
        AuthConfig::from_secret(secret).context("Failed to configure JWT authentication")?;

    if args.is_empty() {
        eprintln!("Usage: sensapp generate-token <subject> [OPTIONS]");
        eprintln!();
        eprintln!("Options:");
        eprintln!("  --scope <read|write|readwrite>  Token scope (default: \"read write\")");
        eprintln!("  --duration <seconds>            Token validity duration (default: 3600)");
        eprintln!("  --sensors <name1,name2,...>      Restrict to specific sensors");
        eprintln!();
        eprintln!("Environment:");
        eprintln!("  SENSAPP_JWT_SECRET              Required. The shared secret for signing.");
        std::process::exit(1);
    }

    let subject = &args[0];
    let mut scope = "read write".to_string();
    let mut duration: u64 = 3600;
    let mut sensors: Option<Vec<String>> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--scope" => {
                i += 1;
                let raw = args.get(i).context("--scope requires a value")?;
                scope = match raw.as_str() {
                    "read" => "read".to_string(),
                    "write" => "write".to_string(),
                    "readwrite" | "read write" | "read,write" => "read write".to_string(),
                    other => anyhow::bail!("Unknown scope: {other}. Use read, write, or readwrite"),
                };
            }
            "--duration" => {
                i += 1;
                let raw = args.get(i).context("--duration requires a value")?;
                duration = raw
                    .parse()
                    .context("--duration must be a number of seconds")?;
            }
            "--sensors" => {
                i += 1;
                let raw = args.get(i).context("--sensors requires a value")?;
                sensors = Some(raw.split(',').map(|s| s.trim().to_string()).collect());
            }
            other => {
                anyhow::bail!("Unknown option: {other}");
            }
        }
        i += 1;
    }

    let token = auth_config
        .create_token(subject, &scope, duration, sensors)
        .context("Failed to create token")?;

    println!("{token}");
    Ok(())
}
