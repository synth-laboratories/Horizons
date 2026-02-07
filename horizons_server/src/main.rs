use clap::Parser;
use horizons_server::cli::{Cli, Commands};
use std::net::SocketAddr;
use std::sync::Arc;

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
        Commands::CreateApiKey {
            data_dir,
            org_id,
            name,
            scopes,
            expires_in_days,
            actor_user_id,
            actor_email,
            org_name,
        } => {
            use chrono::{Duration, Utc};
            use horizons_core::models::{AgentIdentity, OrgId};
            use horizons_core::onboard::models::{ApiKeyRecord, OrgRecord};
            use horizons_core::onboard::traits::{CentralDb, ListQuery};
            use ulid::Ulid;
            use uuid::Uuid;

            let central_db_url = std::env::var("DATABASE_URL")
                .or_else(|_| std::env::var("HORIZONS_CENTRAL_DB_URL"))
                .ok();

            let central_db: Arc<dyn CentralDb> = if let Some(url) = central_db_url {
                let cfg = horizons_core::onboard::config::PostgresConfig {
                    url,
                    max_connections: 5,
                    acquire_timeout: std::time::Duration::from_secs(10),
                };
                let db = horizons_core::onboard::postgres::PostgresCentralDb::connect(&cfg).await?;
                db.migrate().await?;
                Arc::new(db)
            } else {
                Arc::new(
                    horizons_core::onboard::sqlite::SqliteCentralDb::new(
                        data_dir.join("central.db"),
                    )
                    .await?,
                )
            };

            let org_id = OrgId(org_id);
            if central_db.get_org(org_id).await?.is_none() {
                central_db
                    .upsert_org(&OrgRecord {
                        org_id,
                        name: org_name,
                        created_at: Utc::now(),
                    })
                    .await?;
            }

            // Enforce (org_id, name) uniqueness with a friendlier error before DB constraints.
            let existing = central_db
                .list_api_keys(
                    org_id,
                    ListQuery {
                        limit: 1000,
                        offset: 0,
                    },
                )
                .await?;
            if existing.iter().any(|k| k.name == name) {
                anyhow::bail!("api key name already exists for org: {name}");
            }

            let key_id = Uuid::new_v4();
            let secret = format!("{}{}", Ulid::new(), Ulid::new());
            let token = horizons_server::auth::format_api_key_token(key_id, &secret);
            let secret_hash = horizons_server::auth::sha256_hex(secret.as_bytes());
            let created_at = Utc::now();
            let expires_at = expires_in_days.map(|d| created_at + Duration::days(d as i64));

            let actor = match actor_user_id {
                Some(user_id) => AgentIdentity::User {
                    user_id,
                    email: actor_email,
                },
                None => AgentIdentity::System {
                    name: "api_key".to_string(),
                },
            };

            let rec = ApiKeyRecord {
                key_id,
                org_id,
                name,
                actor,
                scopes,
                secret_hash,
                created_at,
                expires_at,
                last_used_at: None,
            };
            central_db.upsert_api_key(&rec).await?;

            println!("token: {token}");
            println!("org_id: {}", rec.org_id);
            println!("key_id: {}", rec.key_id);
            if let Some(exp) = rec.expires_at {
                println!("expires_at: {exp}");
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
