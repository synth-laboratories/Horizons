use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "horizons_rs", version, about = "Horizons HTTP server (Axum)")]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the Axum server.
    Serve {
        /// Bind address (default: 0.0.0.0:8000).
        #[arg(long, default_value = "0.0.0.0:8000")]
        addr: SocketAddr,

        /// Local dev data directory (SQLite dbs + file store).
        #[arg(long, env = "HORIZONS_DEV_DATA_DIR", default_value = ".horizons_dev")]
        data_dir: PathBuf,
    },

    /// Apply database migrations for configured backends.
    ///
    /// The current v0.0.0 dev backend uses SQLite and requires no explicit migrations.
    Migrate,

    /// Seed dev data into the local backend.
    ///
    /// The server seeds a default org automatically on startup; this command is a no-op.
    Seed,
}

#[tokio::main]
#[tracing::instrument(level = "info")]
async fn main() -> anyhow::Result<()> {
    // Global tracing + optional OTLP/console exporting.
    // If no exporter env vars are set, this falls back to JSON stdout logs.
    let o11y = horizons_core::o11y::init_global_from_env()?;
    let cli = Cli::parse();

    match cli.cmd {
        Command::Serve { addr, data_dir } => {
            let state = horizons_rs::dev_backends::build_dev_state(data_dir).await?;
            horizons_rs::server::serve(addr, state).await?;
        }
        Command::Migrate => {
            tracing::info!("no migrations required for the dev backend");
        }
        Command::Seed => {
            tracing::info!("no seed required; dev org is created on server startup");
        }
    }

    if let Some(rt) = o11y {
        rt.shutdown().await?;
    }

    Ok(())
}
