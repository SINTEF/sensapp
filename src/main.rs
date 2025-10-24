#![forbid(unsafe_code)]
use crate::config::load_configuration;
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

    println!("📡 Starting HTTP server on {}...", address);
    match run_http_server(
        HttpServerState {
            name: Arc::new("SensApp".to_string()),
            storage,
            influxdb_with_numeric: config.influxdb_with_numeric,
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
