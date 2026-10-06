#![forbid(unsafe_code)]
use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
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
    // The subcommands are handled before any server setup
    match Cli::parse().command {
        Some(Command::GenerateToken(args)) => return generate_token_command(args),
        Some(Command::GenerateSecret) => {
            println!("{}", generate_secret()?);
            return Ok(());
        }
        // `sensapp` alone runs the server too
        Some(Command::Serve) | None => {}
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
            max_query_samples: config.http_max_query_samples,
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

/// SensApp, a sensor data platform. Without a subcommand, it runs the server, configured with the
/// `SENSAPP_*` environment variables (see docs/CONFIGURATION.md).
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the server (what `sensapp` does alone), configured with the SENSAPP_* environment
    /// variables or a settings.toml, see docs/CONFIGURATION.md
    Serve,
    /// Generate a signed JWT token. Needs SENSAPP_JWT_SECRET, the shared secret for signing
    GenerateToken(GenerateTokenArgs),
    /// Print a new random secret, to use as SENSAPP_JWT_SECRET
    GenerateSecret,
}

#[derive(Args)]
struct GenerateTokenArgs {
    /// Who the token is for
    subject: String,
    /// Comma-separated read, write, delete, admin, or readwrite
    #[arg(long, default_value = "read write")]
    scope: String,
    /// Token validity duration, in seconds
    #[arg(long, default_value_t = 3600)]
    duration: u64,
    /// Restrict to specific sensors, comma separated. Can be repeated
    #[arg(long, value_delimiter = ',', value_name = "NAME,...")]
    sensors: Vec<String>,
    /// Restrict to one sensor, named as it is (a comma is part of the name). Can be repeated
    #[arg(long, value_name = "NAME")]
    sensor: Vec<String>,
}

fn generate_token_command(args: GenerateTokenArgs) -> Result<()> {
    load_configuration().context("Failed to load configuration")?;
    let config = config::get().context("Failed to get configuration")?;

    let secret = config
        .jwt_secret
        .as_deref()
        .context("SENSAPP_JWT_SECRET must be set to generate tokens")?;

    let auth_config =
        AuthConfig::from_secret(secret).context("Failed to configure JWT authentication")?;

    // An allow list given but empty (`--sensors ""`) allows nothing, which is not the same as no
    // allow list at all
    let restricted = !args.sensors.is_empty() || !args.sensor.is_empty();
    let sensors = restricted.then(|| {
        args.sensors
            .iter()
            .map(|name| name.trim())
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .chain(args.sensor.iter().cloned())
            .collect::<Vec<String>>()
    });
    let (subject, scope, duration) = (&args.subject, &args.scope, args.duration);

    // The secret is what gives the right to make any token, so the command line has no cap but a
    // sane one, and is the only way to make an admin token
    let rules = TokenRules {
        max_duration_seconds: 10 * 365 * 24 * 3600,
        allow_admin: true,
    };
    let request = TokenRequest::new(subject, scope, sensors, duration, &rules)?;
    println!("{}", request.issue(&auth_config)?.token);
    Ok(())
}
