use super::{Error, Result};

#[derive(Debug, Clone)]
pub struct EventSyncConfig {
    pub redis_url: String,
    pub postgres_url: String,

    pub max_retries: u32,
    pub retry_backoff_base_ms: u64,
    pub retry_backoff_max_ms: u64,

    pub dlq_retention_days: u32,
    pub max_payload_bytes: usize,

    /// How often the router should sweep for pending events (durability backstop).
    pub poll_interval_ms: u64,

    /// Optional shared secret for inbound webhook signature verification.
    /// If `None`, signatures are not enforced.
    pub webhook_signing_secret: Option<String>,

    /// Default outbound webhook timeout.
    pub outbound_webhook_timeout_ms: u64,
}

impl Default for EventSyncConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            postgres_url: "postgres://postgres:postgres@127.0.0.1:5432/postgres".to_string(),
            max_retries: 5,
            retry_backoff_base_ms: 1_000,
            retry_backoff_max_ms: 60_000,
            dlq_retention_days: 30,
            max_payload_bytes: 1_000_000,
            poll_interval_ms: 5_000,
            webhook_signing_secret: None,
            outbound_webhook_timeout_ms: 10_000,
        }
    }
}

impl EventSyncConfig {
    #[tracing::instrument(level = "debug")]
    pub fn validate(&self) -> Result<()> {
        if self.redis_url.trim().is_empty() {
            return Err(Error::message("redis_url is empty"));
        }
        if self.postgres_url.trim().is_empty() {
            return Err(Error::message("postgres_url is empty"));
        }
        if self.max_retries == 0 {
            return Err(Error::message("max_retries must be > 0"));
        }
        if self.retry_backoff_base_ms == 0 {
            return Err(Error::message("retry_backoff_base_ms must be > 0"));
        }
        if self.retry_backoff_max_ms < self.retry_backoff_base_ms {
            return Err(Error::message(
                "retry_backoff_max_ms must be >= retry_backoff_base_ms",
            ));
        }
        if self.max_payload_bytes == 0 {
            return Err(Error::message("max_payload_bytes must be > 0"));
        }
        if self.poll_interval_ms == 0 {
            return Err(Error::message("poll_interval_ms must be > 0"));
        }
        if self.outbound_webhook_timeout_ms == 0 {
            return Err(Error::message("outbound_webhook_timeout_ms must be > 0"));
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug")]
    pub fn retry_backoff_ms(&self, retry_count: u32) -> u64 {
        // Exponential backoff: base * 2^retry_count, capped.
        let shift = retry_count.min(63) as u32;
        let exp = 1u128 << shift;
        let ms = (self.retry_backoff_base_ms as u128).saturating_mul(exp);
        (ms.min(self.retry_backoff_max_ms as u128)) as u64
    }
}
