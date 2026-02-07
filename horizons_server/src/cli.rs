use clap::{Parser, Subcommand};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "horizons", version, about = "Horizons AI Platform")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Start the HTTP server (default if no subcommand given).
    Serve {
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
        #[arg(long, default_value = "8000")]
        port: u16,

        /// Local dev data directory (SQLite dbs + file store).
        #[arg(long, env = "HORIZONS_DEV_DATA_DIR", default_value = ".horizons_dev")]
        data_dir: PathBuf,
    },

    /// Run database migrations for the central DB + events store (when configured).
    Migrate {
        /// Central Postgres URL override (else use DATABASE_URL / HORIZONS_CENTRAL_DB_URL).
        #[arg(long)]
        database_url: Option<String>,
    },

    /// Create an API key for Bearer-token auth (stored in the Central DB).
    ///
    /// Uses Postgres if `DATABASE_URL`/`HORIZONS_CENTRAL_DB_URL` is set, otherwise uses the
    /// dev SQLite central DB at `{data_dir}/central.db`.
    CreateApiKey {
        /// Local dev data directory (SQLite central DB + file store).
        #[arg(long, env = "HORIZONS_DEV_DATA_DIR", default_value = ".horizons_dev")]
        data_dir: PathBuf,

        /// Tenant org id (UUID).
        #[arg(long)]
        org_id: Uuid,

        /// Friendly name for the key (unique within org).
        #[arg(long)]
        name: String,

        /// Comma-delimited scopes (optional in v0.x; stored for future enforcement).
        #[arg(long, value_delimiter = ',')]
        scopes: Vec<String>,

        /// Optional expiry in days from now.
        #[arg(long)]
        expires_in_days: Option<u64>,

        /// If set, key actor will be attributed to this user id (else `system`).
        #[arg(long)]
        actor_user_id: Option<Uuid>,

        /// Optional email when `--actor-user-id` is set.
        #[arg(long)]
        actor_email: Option<String>,

        /// If the org does not exist, create it with this name.
        #[arg(long, default_value = "default")]
        org_name: String,
    },

    /// Validate a graph definition file (YAML/JSON) against the Horizons graph IR schema.
    ValidateGraph {
        /// Path to YAML/JSON graph file.
        path: PathBuf,
    },

    /// List built-in graphs from the registry.
    ListGraphs,

    /// Print current configuration (redacted secrets).
    Config,

    /// Health check configured backends.
    Check,
}
