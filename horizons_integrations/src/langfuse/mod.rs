pub mod mapping;
pub mod types;

use crate::langfuse::mapping::spans_to_langfuse_batch;
use async_trait::async_trait;
use horizons_core::o11y::traits::{O11yExporter, OtelSpan};
use horizons_core::{Error, Result};
use std::time::Duration;

#[derive(Clone)]
pub struct LangfuseExporter {
    client: reqwest::Client,
    host: String,
    public_key: String,
    secret_key: String,
}

impl LangfuseExporter {
    #[tracing::instrument(level = "debug")]
    pub fn new(
        host: impl Into<String> + std::fmt::Debug,
        public_key: impl Into<String> + std::fmt::Debug,
        secret_key: impl Into<String> + std::fmt::Debug,
        timeout: Duration,
    ) -> Result<Self> {
        let host = host.into();
        if host.trim().is_empty() {
            return Err(Error::InvalidInput("langfuse host is empty".to_string()));
        }
        let public_key = public_key.into();
        if public_key.trim().is_empty() {
            return Err(Error::InvalidInput(
                "langfuse public_key is empty".to_string(),
            ));
        }
        let secret_key = secret_key.into();
        if secret_key.trim().is_empty() {
            return Err(Error::InvalidInput(
                "langfuse secret_key is empty".to_string(),
            ));
        }

        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| Error::backend("build reqwest client", e))?;

        Ok(Self {
            client,
            host: host.trim_end_matches('/').to_string(),
            public_key,
            secret_key,
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn ingestion_url(&self) -> String {
        format!("{}/api/public/ingestion", self.host)
    }
}

#[async_trait]
impl O11yExporter for LangfuseExporter {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn export(&self, spans: Vec<OtelSpan>) -> Result<()> {
        let batch_events = spans_to_langfuse_batch(&spans)?;
        if batch_events.batch.is_empty() {
            return Ok(());
        }

        let resp = self
            .client
            .post(self.ingestion_url())
            .basic_auth(&self.public_key, Some(&self.secret_key))
            .json(&batch_events)
            .send()
            .await
            .map_err(|e| Error::backend("send langfuse ingestion request", e))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "langfuse ingestion failed: {status} {text}"
            )));
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "langfuse"
    }
}
