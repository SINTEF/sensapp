#![forbid(unsafe_code)]
use anyhow::{Context, Result};
use sensapp::config::{self, load_configuration};
use sensapp::http::auth::{AuthConfig, AuthMode, generate_secret, resolve_auth_mode};
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
    let auth = build_auth(&auth_mode, config.auth_disabled, address, config.ui_enabled)?;

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
    auth_disabled: bool,
    address: SocketAddr,
    ui_enabled: bool,
) -> Result<Option<AuthConfig>> {
    match mode {
        AuthMode::Secret(secret) => {
            if auth_disabled {
                println!("⚠️  SENSAPP_AUTH_DISABLED is ignored: SENSAPP_JWT_SECRET is set");
            }
            let auth_config = AuthConfig::from_secret(secret)
                .context("Failed to configure JWT authentication")?;
            println!("🔐 JWT authentication enabled");
            Ok(Some(auth_config))
        }
        AuthMode::Ephemeral(secret) => {
            let auth_config = AuthConfig::from_secret(secret)
                .context("Failed to configure JWT authentication")?;
            let token = auth_config
                .create_token(
                    "local-dev",
                    "read write delete",
                    EPHEMERAL_TOKEN_SECONDS,
                    None,
                )
                .context("Failed to create the local token")?;
            println!("🔐 JWT authentication enabled, with a secret made for this run");
            println!("   It is lost when SensApp stops, and tokens made before are refused.");
            println!("   Set SENSAPP_JWT_SECRET to keep it (`sensapp generate-secret` makes one).");
            println!("   Token for this run, valid for 24 hours:");
            println!("   {token}");
            if ui_enabled {
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
///   --scope <read|write|delete|readwrite,...>  (default: "read write")
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
        eprintln!(
            "  --scope <scopes>                Comma-separated read, write, delete, or readwrite (default: \"read write\")"
        );
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
                scope = parse_scope_argument(raw)?;
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

/// Parse the `--scope` value: comma or space separated `read`, `write`, `delete`,
/// with `readwrite` as a shorthand for `read write`.
fn parse_scope_argument(raw: &str) -> Result<String> {
    let mut scopes: Vec<&str> = Vec::new();
    for item in raw.split([',', ' ']).filter(|item| !item.is_empty()) {
        let expanded: &[&str] = match item {
            "read" => &["read"],
            "write" => &["write"],
            "delete" => &["delete"],
            "readwrite" => &["read", "write"],
            other => anyhow::bail!("Unknown scope: {other}. Use read, write, delete, or readwrite"),
        };
        for scope in expanded {
            if !scopes.contains(scope) {
                scopes.push(scope);
            }
        }
    }
    if scopes.is_empty() {
        anyhow::bail!("--scope requires at least one scope");
    }
    Ok(scopes.join(" "))
}

#[cfg(test)]
mod scope_argument_tests {
    use super::parse_scope_argument;

    #[test]
    fn parses_scope_combinations() {
        assert_eq!(parse_scope_argument("read").unwrap(), "read");
        assert_eq!(parse_scope_argument("readwrite").unwrap(), "read write");
        assert_eq!(parse_scope_argument("read write").unwrap(), "read write");
        assert_eq!(parse_scope_argument("read,write").unwrap(), "read write");
        assert_eq!(parse_scope_argument("delete").unwrap(), "delete");
        assert_eq!(
            parse_scope_argument("readwrite,delete").unwrap(),
            "read write delete"
        );
        assert_eq!(parse_scope_argument("read,read").unwrap(), "read");
    }

    #[test]
    fn rejects_unknown_or_empty_scopes() {
        assert!(parse_scope_argument("admin").is_err());
        assert!(parse_scope_argument("read,admin").is_err());
        assert!(parse_scope_argument("").is_err());
    }
}
