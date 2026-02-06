use crate::o11y::traits::{O11yExporter, OtelSpan};
use crate::{Error, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

#[async_trait]
pub trait LineWriter: Send + Sync {
    async fn write_line(&self, line: String) -> Result<()>;
}

struct StdoutWriter;

#[async_trait]
impl LineWriter for StdoutWriter {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn write_line(&self, line: String) -> Result<()> {
        let mut out = tokio::io::stdout();
        out.write_all(line.as_bytes())
            .await
            .map_err(|e| Error::backend("stdout write", e))?;
        out.write_all(b"\n")
            .await
            .map_err(|e| Error::backend("stdout write newline", e))?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct ConsoleExporter {
    writer: Arc<dyn LineWriter>,
}

impl ConsoleExporter {
    #[tracing::instrument(level = "debug")]
    pub fn stdout() -> Self {
        Self {
            writer: Arc::new(StdoutWriter),
        }
    }

    #[tracing::instrument(level = "debug", skip(writer))]
    pub fn new(writer: Arc<dyn LineWriter>) -> Self {
        Self { writer }
    }
}

#[async_trait]
impl O11yExporter for ConsoleExporter {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn export(&self, spans: Vec<OtelSpan>) -> Result<()> {
        for s in spans {
            let line = serde_json::to_string(&serde_json::json!({
                "type": "otel.span",
                "span": s,
            }))
            .map_err(|e| Error::backend("serialize console span", e))?;
            self.writer.write_line(line).await?;
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
        "console"
    }
}
