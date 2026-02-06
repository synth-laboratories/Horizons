use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
