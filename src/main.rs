#![forbid(unsafe_code)]
use anyhow::{Context, Result};
use crate::config::load_configuration;
use crate::ingestors::http::server::run_http_server;
use crate::ingestors::http::state::HttpServerState;
use std::net::SocketAddr;
use std::sync::Arc;
use storage::storage_factory::create_storage_from_connection_string;
//use storage::duckdb::DuckDBStorage;
//use storage::postgresql::postgresql::PostgresStorage;
#[cfg(feature = "sqlite")]
use storage::sqlite::sqlite::SqliteStorage;
use tracing::Level;
use tracing::event;
mod config;
mod datamodel;
mod exporters;
mod importers;
mod infer;
mod ingestors;
mod parsing;
mod storage;

fn main() -> Result<()> {
    let _sentry = sentry::init((
        "https://94bc3d0bd0424707898d420ed4ad6a3d@feil.sintef.cloud/5",
        sentry::ClientOptions {
            release: sentry::release_name!(),
            debug: true,
            ..Default::default()
        },
    ));

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

    // sentry::capture_message("Hello, Sentry 2!", sentry::Level::Info);

    // Load configuration
    load_configuration().context("Failed to load configuration")?;
    let config = config::get().context("Failed to get configuration")?;

    sinteflake::set_instance_id(config.instance_id)
        .context("Failed to set instance ID")?;
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

    // Start MQTT clients if configured
    if let Some(mqtt_configs) = config.mqtt.as_ref() {
        println!("📡 Starting {} MQTT client(s)...", mqtt_configs.len());
        for (i, mqtt_config) in mqtt_configs.iter().enumerate() {
            let cloned_config = mqtt_config.clone();
            let cloned_storage = storage.clone();
            tokio::spawn(async move {
                if let Err(e) = ingestors::mqtt::mqtt_client(cloned_config, cloned_storage).await {
                    eprintln!("❌ MQTT client {} failed: {}", i + 1, e);
                    event!(Level::ERROR, "MQTT client {} failed: {}", i + 1, e);
                }
            });
            println!("✅ MQTT client {} started", i + 1);
        }
    } else {
        println!("ℹ️  No MQTT configuration found, skipping MQTT clients");
    }

    let endpoint = config.endpoint;
    let port = config.port;
    let address = SocketAddr::from((endpoint, port));

    println!("📡 Starting HTTP server on {}...", address);
    match run_http_server(
        HttpServerState {
            name: Arc::new("SensApp".to_string()),
            storage,
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
            event!(Level::ERROR, "HTTP server failed: {}", err);
            eprintln!("❌ HTTP server failed to start: {}", err);
            Err(err.into())
        }
    }
}

// async fn handler() -> &'static str {
//     "Hello, world!"
// }

// async fn publish_stream_handler(body: axum::body::Body) -> Result<String, (StatusCode, String)> {
//     let mut count = 0usize;
//     let mut stream = body.into_data_stream();

//     loop {
//         let chunk = stream.try_next().await;
//         match chunk {
//             Ok(bytes) => match bytes {
//                 Some(bytes) => count += bytes.into_iter().filter(|b| *b == b'\n').count(),
//                 None => break,
//             },
//             Err(_) => {
//                 return Err((
//                     StatusCode::INTERNAL_SERVER_ERROR,
//                     "Error reading body".to_string(),
//                 ))
//             }
//         }
//     }

//     Ok(count.to_string())
// }

// async fn publish_csv(body: axum::body::Body) -> Result<String, (StatusCode, String)> {
//     let stream = body.into_data_stream();
//     let stream = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));
//     let reader = stream.into_async_read();
//     let mut csv_reader = csv_async::AsyncReaderBuilder::new()
//         .has_headers(true)
//         .delimiter(b';')
//         .create_reader(reader);

//     println!("{:?}", csv_reader.has_headers());
//     println!("{:?}", csv_reader.headers().await.unwrap());
//     let mut records = csv_reader.records();

//     println!("Reading CSV");
//     while let Some(record) = records.next().await {
//         let record = record.unwrap();
//         println!("{:?}", record);
//     }
//     println!("Done reading CSV");

//     Ok("ok".to_string())
// }
