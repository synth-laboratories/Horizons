use crate::{Error, Result};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub url: String,
    pub max_connections: u32,
    pub acquire_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct ProjectDbConfig {
    /// Local directory used for per-project libsql databases.
    pub root_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    /// Optional custom endpoint (e.g. MinIO).
    pub endpoint: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Optional key prefix applied before org/project scoping.
    pub prefix: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RedisConfig {
    pub url: String,
    pub key_prefix: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HelixConfig {
    pub url: String,
    pub api_key: Option<String>,
    pub timeout: Duration,
}

/// Configuration for the bundled "On Board Business Logic" infrastructure.
#[derive(Debug, Clone)]
pub struct OnboardConfig {
    pub postgres: PostgresConfig,
    pub project_db: ProjectDbConfig,
    pub s3: S3Config,
    pub redis: RedisConfig,
    pub helix: HelixConfig,
}

impl OnboardConfig {
    #[tracing::instrument(level = "debug")]
    pub fn from_env() -> Result<Self> {
        let pg_url = std::env::var("HORIZONS_CENTRAL_DB_URL")
            .map_err(|_| Error::InvalidInput("HORIZONS_CENTRAL_DB_URL is required".to_string()))?;
        let pg_max_connections = std::env::var("HORIZONS_CENTRAL_DB_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(10);
        let pg_acquire_timeout_ms = std::env::var("HORIZONS_CENTRAL_DB_ACQUIRE_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5_000);

        let project_root_dir = std::env::var("HORIZONS_PROJECT_DB_ROOT_DIR")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./data/project_dbs"));

        let s3_bucket = std::env::var("HORIZONS_S3_BUCKET")
            .map_err(|_| Error::InvalidInput("HORIZONS_S3_BUCKET is required".to_string()))?;
        let s3_region =
            std::env::var("HORIZONS_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let s3_endpoint = std::env::var("HORIZONS_S3_ENDPOINT").ok();
        let s3_access_key_id = std::env::var("HORIZONS_S3_ACCESS_KEY_ID").map_err(|_| {
            Error::InvalidInput("HORIZONS_S3_ACCESS_KEY_ID is required".to_string())
        })?;
        let s3_secret_access_key =
            std::env::var("HORIZONS_S3_SECRET_ACCESS_KEY").map_err(|_| {
                Error::InvalidInput("HORIZONS_S3_SECRET_ACCESS_KEY is required".to_string())
            })?;
        let s3_prefix = std::env::var("HORIZONS_S3_PREFIX").ok();

        let redis_url = std::env::var("HORIZONS_REDIS_URL")
            .map_err(|_| Error::InvalidInput("HORIZONS_REDIS_URL is required".to_string()))?;
        let redis_key_prefix = std::env::var("HORIZONS_REDIS_KEY_PREFIX").ok();

        let helix_url =
            std::env::var("HORIZONS_HELIX_URL").unwrap_or_else(|_| "http://localhost:6969".into());
        let helix_api_key = std::env::var("HORIZONS_HELIX_API_KEY").ok();
        let helix_timeout_ms = std::env::var("HORIZONS_HELIX_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10_000);

        let cfg = Self {
            postgres: PostgresConfig {
                url: pg_url,
                max_connections: pg_max_connections,
                acquire_timeout: Duration::from_millis(pg_acquire_timeout_ms),
            },
            project_db: ProjectDbConfig {
                root_dir: project_root_dir,
            },
            s3: S3Config {
                bucket: s3_bucket,
                region: s3_region,
                endpoint: s3_endpoint,
                access_key_id: s3_access_key_id,
                secret_access_key: s3_secret_access_key,
                prefix: s3_prefix,
            },
            redis: RedisConfig {
                url: redis_url,
                key_prefix: redis_key_prefix,
            },
            helix: HelixConfig {
                url: helix_url,
                api_key: helix_api_key,
                timeout: Duration::from_millis(helix_timeout_ms),
            },
        };

        cfg.validate()?;
        Ok(cfg)
    }

    #[tracing::instrument(level = "debug")]
    pub fn validate(&self) -> Result<()> {
        if self.postgres.url.trim().is_empty() {
            return Err(Error::InvalidInput("postgres.url is empty".to_string()));
        }
        if self.postgres.max_connections == 0 {
            return Err(Error::InvalidInput(
                "postgres.max_connections must be > 0".to_string(),
            ));
        }
        if self.postgres.acquire_timeout.is_zero() {
            return Err(Error::InvalidInput(
                "postgres.acquire_timeout must be > 0".to_string(),
            ));
        }

        if self.s3.bucket.trim().is_empty() {
            return Err(Error::InvalidInput("s3.bucket is empty".to_string()));
        }
        if self.s3.region.trim().is_empty() {
            return Err(Error::InvalidInput("s3.region is empty".to_string()));
        }
        if self.s3.access_key_id.trim().is_empty() {
            return Err(Error::InvalidInput("s3.access_key_id is empty".to_string()));
        }
        if self.s3.secret_access_key.trim().is_empty() {
            return Err(Error::InvalidInput(
                "s3.secret_access_key is empty".to_string(),
            ));
        }

        if self.redis.url.trim().is_empty() {
            return Err(Error::InvalidInput("redis.url is empty".to_string()));
        }

        if self.helix.url.trim().is_empty() {
            return Err(Error::InvalidInput("helix.url is empty".to_string()));
        }
        if self.helix.timeout.is_zero() {
            return Err(Error::InvalidInput("helix.timeout is empty".to_string()));
        }

        Ok(())
    }
}
