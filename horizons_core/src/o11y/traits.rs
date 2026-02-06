use crate::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// A minimal, serializable span representation for exporter backends.
///
/// Horizons uses `tracing` instrumentation throughout, but exporters often need a stable,
/// backend-agnostic payload (OTLP, Langfuse, console, etc.).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OtelSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub attributes: BTreeMap<String, serde_json::Value>,
    pub events: Vec<OtelEvent>,
    pub status: OtelSpanStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OtelEvent {
    pub name: String,
    pub timestamp: DateTime<Utc>,
    pub attributes: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtelSpanStatus {
    pub code: OtelStatusCode,
    pub message: Option<String>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OtelStatusCode {
    Unset,
    Ok,
    Error,
}

/// Trait for observability exporters. Implemented by OTLP (core), console (core),
/// and integration sinks (e.g. Langfuse in `horizons_integrations`).
#[async_trait]
pub trait O11yExporter: Send + Sync {
    async fn export(&self, spans: Vec<OtelSpan>) -> Result<()>;
    async fn flush(&self) -> Result<()>;
    async fn shutdown(&self) -> Result<()>;

    fn name(&self) -> &'static str;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OtlpProtocol {
    Grpc,
    HttpProtobuf,
}

impl OtlpProtocol {
    #[tracing::instrument(level = "debug")]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "grpc" => Some(Self::Grpc),
            "http/protobuf" | "http_protobuf" | "http" => Some(Self::HttpProtobuf),
            _ => None,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedactionLevel {
    Full,
    Metadata,
    Off,
}

impl RedactionLevel {
    #[tracing::instrument(level = "debug")]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "full" => Some(Self::Full),
            "metadata" => Some(Self::Metadata),
            "off" => Some(Self::Off),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct O11yConfig {
    pub service_name: String,
    pub otlp_endpoint: Option<String>,
    pub otlp_protocol: OtlpProtocol,
    pub otlp_headers: BTreeMap<String, String>,
    pub otlp_timeout: Duration,

    pub console: bool,

    pub redaction_level: RedactionLevel,
    pub redaction_patterns: Vec<String>,

    pub span_buffer_capacity: usize,
    pub batch_size: usize,
    pub flush_interval: Duration,
}

impl Default for O11yConfig {
    fn default() -> Self {
        Self {
            service_name: "horizons".to_string(),
            otlp_endpoint: None,
            otlp_protocol: OtlpProtocol::HttpProtobuf,
            otlp_headers: BTreeMap::new(),
            otlp_timeout: Duration::from_secs(3),
            console: false,
            redaction_level: RedactionLevel::Metadata,
            redaction_patterns: vec![],
            span_buffer_capacity: 10_000,
            batch_size: 200,
            flush_interval: Duration::from_millis(1000),
        }
    }
}

impl O11yConfig {
    #[tracing::instrument(level = "debug")]
    pub fn from_env() -> Result<Self> {
        let mut cfg = Self::default();

        if let Ok(v) = std::env::var("OTEL_SERVICE_NAME") {
            if !v.trim().is_empty() {
                cfg.service_name = v;
            }
        }

        if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            if !v.trim().is_empty() {
                cfg.otlp_endpoint = Some(v);
            }
        }

        if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL") {
            if !v.trim().is_empty() {
                cfg.otlp_protocol = OtlpProtocol::parse(&v).ok_or_else(|| {
                    crate::Error::InvalidInput(format!("invalid OTEL_EXPORTER_OTLP_PROTOCOL: {v}"))
                })?;
            }
        }

        if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_HEADERS") {
            if !v.trim().is_empty() {
                cfg.otlp_headers = parse_headers(&v)?;
            }
        }

        if let Ok(v) = std::env::var("HORIZONS_O11Y_CONSOLE") {
            cfg.console = matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "y"
            );
        }

        if let Ok(v) = std::env::var("HORIZONS_O11Y_REDACTION_LEVEL") {
            if !v.trim().is_empty() {
                cfg.redaction_level = RedactionLevel::parse(&v).ok_or_else(|| {
                    crate::Error::InvalidInput(format!(
                        "invalid HORIZONS_O11Y_REDACTION_LEVEL: {v}"
                    ))
                })?;
            }
        }

        if let Ok(v) = std::env::var("HORIZONS_O11Y_REDACTION_PATTERNS") {
            // Comma-separated regexes.
            let parts: Vec<String> = v
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            if !parts.is_empty() {
                cfg.redaction_patterns = parts;
            }
        }

        Ok(cfg)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn validate(&self) -> Result<()> {
        if self.service_name.trim().is_empty() {
            return Err(crate::Error::InvalidInput(
                "service_name is empty".to_string(),
            ));
        }
        if let Some(ep) = &self.otlp_endpoint {
            if ep.trim().is_empty() {
                return Err(crate::Error::InvalidInput(
                    "otlp_endpoint is empty".to_string(),
                ));
            }
        }
        if self.span_buffer_capacity == 0 {
            return Err(crate::Error::InvalidInput(
                "span_buffer_capacity must be > 0".to_string(),
            ));
        }
        if self.batch_size == 0 {
            return Err(crate::Error::InvalidInput(
                "batch_size must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}

#[tracing::instrument(level = "debug")]
fn parse_headers(raw: &str) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let Some((k, v)) = part.split_once('=') else {
            return Err(crate::Error::InvalidInput(format!(
                "invalid OTEL_EXPORTER_OTLP_HEADERS entry (expected k=v): {part}"
            )));
        };
        let k = k.trim();
        if k.is_empty() {
            return Err(crate::Error::InvalidInput(
                "invalid OTEL_EXPORTER_OTLP_HEADERS: empty key".to_string(),
            ));
        }
        out.insert(k.to_string(), v.trim().to_string());
    }
    Ok(out)
}
