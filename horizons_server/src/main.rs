use clap::Parser;
use horizons_server::cli::{Cli, Commands};
use std::net::SocketAddr;

#[tokio::main]
#[tracing::instrument(level = "info")]
async fn main() -> anyhow::Result<()> {
    // Global tracing + optional OTLP/console exporting.
    // If no exporter env vars are set, this falls back to JSON stdout logs.
    let o11y = horizons_core::o11y::init_global_from_env()?;
    let cli = Cli::parse();

    let cmd = cli.command.unwrap_or(Commands::Serve {
        host: "0.0.0.0".to_string(),
        port: 8000,
        data_dir: ".horizons_dev".into(),
    });

    match cmd {
        Commands::Serve {
            host,
            port,
            data_dir,
        } => {
            let addr: SocketAddr = format!("{host}:{port}").parse()?;
            let state = horizons_server::dev_backends::build_dev_state(data_dir).await?;
            horizons_server::server::serve(addr, state).await?;
        }
        Commands::Migrate { database_url } => {
            // Central DB (Postgres) migrations.
            let db_url = database_url
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .or_else(|| std::env::var("HORIZONS_CENTRAL_DB_URL").ok());
            if let Some(url) = db_url.as_deref() {
                let cfg = horizons_core::onboard::config::PostgresConfig {
                    url: url.to_string(),
                    max_connections: 5,
                    acquire_timeout: std::time::Duration::from_secs(10),
                };
                let db = horizons_core::onboard::postgres::PostgresCentralDb::connect(&cfg).await?;
                db.migrate().await?;
                tracing::info!("central db migrations applied");
            } else {
                tracing::info!("no central db configured; skipping migrations");
            }

            // Events store migrations (uses the same Postgres URL as the central DB in dev).
            if let Some(url) = db_url.as_deref() {
                let store = horizons_core::events::store::PgEventStore::connect(url).await?;
                store.migrate().await?;
                tracing::info!("events store migrations applied");
            }
        }
        Commands::ValidateGraph { path } => {
            let bytes = tokio::fs::read(&path).await?;
            let s = String::from_utf8(bytes)?;
            let v: serde_json::Value = match path.extension().and_then(|e| e.to_str()) {
                Some("yaml") | Some("yml") => {
                    let y: serde_yaml::Value = serde_yaml::from_str(&s)?;
                    serde_json::to_value(y)?
                }
                _ => serde_json::from_str(&s)?,
            };

            let ir: horizons_graph::ir::GraphIr = serde_json::from_value(v)?;
            let validation =
                horizons_graph::ir::validate_graph_ir(&ir, horizons_graph::ir::Strictness::Strict);
            if validation.ok() {
                println!("ok");
            } else {
                for d in validation.diagnostics {
                    println!("{:?}: {}", d.severity, d.message);
                }
                anyhow::bail!("graph validation failed");
            }
        }
        Commands::ListGraphs => {
            for id in horizons_graph::registry::list_builtin_graphs() {
                println!("{id}");
            }
        }
        Commands::Config => {
            // Keep this intentionally simple: print relevant env + inferred settings.
            fn redact(s: &str) -> String {
                if s.len() <= 8 {
                    return "***".to_string();
                }
                format!("{}***{}", &s[..4], &s[s.len() - 4..])
            }

            let cfg = serde_json::json!({
                "DATABASE_URL": std::env::var("DATABASE_URL").ok().map(|v| redact(&v)),
                "HORIZONS_CENTRAL_DB_URL": std::env::var("HORIZONS_CENTRAL_DB_URL").ok().map(|v| redact(&v)),
                "REDIS_URL": std::env::var("REDIS_URL").ok().map(|v| redact(&v)),
                "HORIZONS_REDIS_URL": std::env::var("HORIZONS_REDIS_URL").ok().map(|v| redact(&v)),
                "HORIZONS_MCP_CONFIG": std::env::var("HORIZONS_MCP_CONFIG").ok().map(|_| "<set>".to_string()),
                "HORIZONS_VECTOR_DB_URL": std::env::var("HORIZONS_VECTOR_DB_URL").ok().map(|v| redact(&v)),
            });
            println!("{}", serde_json::to_string_pretty(&cfg)?);
        }
        Commands::Check => {
            // Postgres.
            let db_url = std::env::var("DATABASE_URL")
                .or_else(|_| std::env::var("HORIZONS_CENTRAL_DB_URL"))
                .ok();
            if let Some(url) = db_url {
                match sqlx::PgPool::connect(&url).await {
                    Ok(pool) => match sqlx::query("SELECT 1").execute(&pool).await {
                        Ok(_) => println!("postgres: ok"),
                        Err(e) => println!("postgres: error ({e})"),
                    },
                    Err(e) => println!("postgres: error ({e})"),
                }
            } else {
                println!("postgres: not configured");
            }

            // Redis.
            let redis_url = std::env::var("REDIS_URL")
                .or_else(|_| std::env::var("HORIZONS_REDIS_URL"))
                .ok();
            if let Some(url) = redis_url {
                match redis::Client::open(url) {
                    Ok(client) => match client.get_multiplexed_async_connection().await {
                        Ok(mut conn) => {
                            let pong: redis::RedisResult<String> =
                                redis::cmd("PING").query_async(&mut conn).await;
                            match pong {
                                Ok(_) => println!("redis: ok"),
                                Err(e) => println!("redis: error ({e})"),
                            }
                        }
                        Err(e) => println!("redis: error ({e})"),
                    },
                    Err(e) => println!("redis: error ({e})"),
                }
            } else {
                println!("redis: not configured");
            }

            // Helix + S3 are backend-implementation-defined in v0.1.x; report configuration only.
            let helix = std::env::var("HORIZONS_HELIX_URL").ok();
            println!(
                "helix: {}",
                helix
                    .as_deref()
                    .map(|_| "configured")
                    .unwrap_or("not configured")
            );
            let s3 = std::env::var("HORIZONS_S3_BUCKET").ok();
            println!(
                "s3: {}",
                s3.as_deref()
                    .map(|_| "configured")
                    .unwrap_or("not configured")
            );
        }
    }

    if let Some(rt) = o11y {
        rt.shutdown().await?;
    }

    Ok(())
}
