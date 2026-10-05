#![forbid(unsafe_code)]
use anyhow::{Context, Result};
use sensapp::config::{self, load_configuration};
use sensapp::http::auth::{
    AuthConfig, AuthMode, TokenRequest, TokenRules, generate_secret, resolve_auth_mode,
};
use sensapp::http::metrics::HttpMetrics;
use sensapp::http::server::run_http_server;
use sensapp::http::state::HttpServerState;
use sensapp::storage::storage_factory::create_storage_from_connection_string;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::Level;
use tracing::event;

fn main() -> Result<()> {
    // Handle `generate-token` subcommand before any server setup
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && args[1] == "generate-token" {
        return generate_token_command(&args[2..]);
    }
    if args.len() > 1 && args[1] == "generate-secret" {
        println!("{}", generate_secret()?);
        return Ok(());
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

    // Decide how to authenticate before anything else: SensApp refuses to start, before it
    // connects to the storage, when it would be open by mistake
    let auth_mode = resolve_auth_mode(
        config.jwt_secret.as_deref(),
        config.auth_disabled,
        config.endpoint,
    )
    .context("Authentication is not configured")?;

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

    // Initialize storage backend
    println!("🗄️  Connecting to storage...");
    let storage = create_storage_from_connection_string(&config.storage_connection_string)
        .await
        .context("Failed to create storage backend")?;
    if config.deduplicate_on_ingest {
        storage.set_deduplicate_on_ingest(true).await.context(
            "SENSAPP_DEDUPLICATE_ON_INGEST is set, but this storage backend cannot deduplicate \
             samples at ingestion (PostgreSQL, TimescaleDB, SQLite and DuckDB can)",
        )?;
        println!("🧹 Deduplication at ingestion is on");
    }

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
    let auth = build_auth(&auth_mode, &config, address)?;

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

/// How long the token printed at the start of a run with a made secret is valid. The secret is
/// lost with the process, so the CLI cannot mint another one: it has to last a working day.
const EPHEMERAL_TOKEN_SECONDS: u64 = 24 * 3600;

/// Build the authentication of the server from the decided mode, and tell the operator about it.
fn build_auth(
    mode: &AuthMode,
    config: &config::SensAppConfig,
    address: SocketAddr,
) -> Result<Option<AuthConfig>> {
    let previous_secrets = config.previous_jwt_secrets();
    match mode {
        AuthMode::Secret(secret) => {
            if config.auth_disabled {
                println!("⚠️  SENSAPP_AUTH_DISABLED is ignored: SENSAPP_JWT_SECRET is set");
            }
            let auth_config = AuthConfig::from_secret(secret)
                .context("Failed to configure JWT authentication")?
                .with_previous_secrets(&previous_secrets)?
                .with_max_token_duration(config.token_max_duration_seconds);
            println!("🔐 JWT authentication enabled");
            if !previous_secrets.is_empty() {
                println!(
                    "🔑 {} previous secret(s) still verify tokens",
                    previous_secrets.len()
                );
            }
            Ok(Some(auth_config))
        }
        AuthMode::Ephemeral(secret) => {
            let auth_config = AuthConfig::from_secret(secret)
                .context("Failed to configure JWT authentication")?;
            if !previous_secrets.is_empty() {
                println!(
                    "⚠️  SENSAPP_JWT_PREVIOUS_SECRETS is ignored: there is no SENSAPP_JWT_SECRET"
                );
            }
            // Every scope, as this token is the only way in: the secret is not known to the CLI
            let token = auth_config
                .issue_token(
                    "local-dev",
                    "read write delete admin",
                    EPHEMERAL_TOKEN_SECONDS,
                    None,
                )
                .context("Failed to create the local token")?
                .token;
            println!("🔐 JWT authentication enabled, with a secret made for this run");
            println!("   It is lost when SensApp stops, and tokens made before are refused.");
            println!("   Set SENSAPP_JWT_SECRET to keep it (`sensapp generate-secret` makes one).");
            println!("   Token for this run, valid for 24 hours:");
            println!("   {token}");
            if config.ui_enabled {
                println!("   Open the UI signed in: http://{address}/ui/#token={token}");
            }
            Ok(Some(auth_config))
        }
        AuthMode::Disabled => {
            println!(
                "🔓 SENSAPP_AUTH_DISABLED is set: every endpoint is open, to anyone who can reach this server"
            );
            Ok(None)
        }
    }
}

/// CLI subcommand: generate a signed JWT token.
///
/// Usage: sensapp generate-token <subject> [OPTIONS]
///   --scope <read|write|delete|admin|readwrite,...>  (default: "read write")
///   --duration <seconds>            (default: 3600)
///   --sensors <name1,name2,...>      (optional sensor allow list, comma separated)
///   --sensor <name>                  (one sensor name as it is, commas included; repeat it)
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
        eprintln!(
            "  --scope <scopes>                Comma-separated read, write, delete, admin, or readwrite (default: \"read write\")"
        );
        eprintln!("  --duration <seconds>            Token validity duration (default: 3600)");
        eprintln!(
            "  --sensors <name1,name2,...>      Restrict to specific sensors, comma separated"
        );
        eprintln!(
            "  --sensor <name>                 Restrict to one sensor, named as it is (a comma is part of the name). Repeat it"
        );
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
                scope = args.get(i).context("--scope requires a value")?.clone();
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
                sensors.get_or_insert_with(Vec::new).extend(
                    raw.split(',')
                        .map(str::trim)
                        .filter(|name| !name.is_empty())
                        .map(str::to_string),
                );
            }
            "--sensor" => {
                i += 1;
                let name = args.get(i).context("--sensor requires a name")?;
                sensors.get_or_insert_with(Vec::new).push(name.clone());
            }
            other => {
                anyhow::bail!("Unknown option: {other}");
            }
        }
        i += 1;
    }

    // The secret is what gives the right to make any token, so the command line has no cap but a
    // sane one, and is the only way to make an admin token
    let rules = TokenRules {
        max_duration_seconds: 10 * 365 * 24 * 3600,
        allow_admin: true,
    };
    let request = TokenRequest::new(subject, &scope, sensors, duration, &rules)?;
    println!("{}", request.issue(&auth_config)?.token);
    Ok(())
}
