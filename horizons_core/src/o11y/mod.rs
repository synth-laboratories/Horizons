pub mod console;
pub mod conventions;
pub mod otel;
pub mod redaction;
pub mod traits;

use crate::o11y::redaction::RedactionPolicy;
use crate::o11y::traits::{O11yConfig, O11yExporter, OtelSpan};
use crate::{Error, Result};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::time::{self, Duration};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

#[derive(Clone)]
struct SpanIds {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
}

impl SpanIds {
    fn trace_id_hex(&self) -> String {
        hex_lower(&self.trace_id)
    }
    fn span_id_hex(&self) -> String {
        hex_lower(&self.span_id)
    }
    fn parent_span_id_hex(&self) -> Option<String> {
        self.parent_span_id.map(|b| hex_lower(&b))
    }
}

#[derive(Clone)]
struct SpanState {
    ids: SpanIds,
    name: String,
    start_time: chrono::DateTime<chrono::Utc>,
    attributes: std::collections::BTreeMap<String, serde_json::Value>,
    events: Vec<crate::o11y::traits::OtelEvent>,
    status: crate::o11y::traits::OtelSpanStatus,
}

struct CaptureLayer {
    tx: mpsc::Sender<OtelSpan>,
}

impl CaptureLayer {
    #[tracing::instrument(level = "debug", skip(tx))]
    fn new(tx: mpsc::Sender<OtelSpan>) -> Self {
        Self { tx }
    }
}

impl<S> Layer<S> for CaptureLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let now = chrono::Utc::now();

        let meta = attrs.metadata();
        let name = meta.name().to_string();

        let parent = attrs
            .parent()
            .and_then(|pid| ctx.span(pid))
            .and_then(|s| s.extensions().get::<SpanIds>().cloned());

        let (trace_id, parent_span_id) = match parent {
            Some(p) => (p.trace_id, Some(p.span_id)),
            None => (uuid::Uuid::new_v4().into_bytes(), None),
        };

        let span_id: [u8; 8] = uuid::Uuid::new_v4().into_bytes()[..8]
            .try_into()
            .expect("slice length");

        let ids = SpanIds {
            trace_id,
            span_id,
            parent_span_id,
        };

        let mut attributes = std::collections::BTreeMap::new();
        let mut visitor = JsonVisitor::new(&mut attributes);
        attrs.record(&mut visitor);

        if let Some(span) = ctx.span(id) {
            let mut exts = span.extensions_mut();
            exts.insert(ids.clone());
            exts.insert(SpanState {
                ids,
                name,
                start_time: now,
                attributes,
                events: vec![],
                status: crate::o11y::traits::OtelSpanStatus {
                    code: crate::o11y::traits::OtelStatusCode::Unset,
                    message: None,
                },
            });
        }
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else { return };
        let mut exts = span.extensions_mut();
        let Some(state) = exts.get_mut::<SpanState>() else {
            return;
        };
        let mut visitor = JsonVisitor::new(&mut state.attributes);
        values.record(&mut visitor);
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let Some(span) = ctx.lookup_current() else {
            return;
        };
        let mut exts = span.extensions_mut();
        let Some(state) = exts.get_mut::<SpanState>() else {
            return;
        };

        let mut attrs = std::collections::BTreeMap::new();
        let mut visitor = JsonVisitor::new(&mut attrs);
        event.record(&mut visitor);
        attrs.insert(
            "level".to_string(),
            serde_json::Value::String(event.metadata().level().as_str().to_string()),
        );

        state.events.push(crate::o11y::traits::OtelEvent {
            name: event.metadata().name().to_string(),
            timestamp: chrono::Utc::now(),
            attributes: attrs,
        });

        // Best-effort status mapping: any ERROR-level event marks span as error.
        if *event.metadata().level() == tracing::Level::ERROR {
            state.status.code = crate::o11y::traits::OtelStatusCode::Error;
        }
    }

    fn on_close(&self, id: tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let mut exts = span.extensions_mut();
        let Some(state) = exts.remove::<SpanState>() else {
            return;
        };

        let end_time = chrono::Utc::now();
        let span = OtelSpan {
            trace_id: state.ids.trace_id_hex(),
            span_id: state.ids.span_id_hex(),
            parent_span_id: state.ids.parent_span_id_hex(),
            name: state.name,
            start_time: state.start_time,
            end_time,
            attributes: state.attributes,
            events: state.events,
            status: state.status,
        };

        // Drop spans if the channel is saturated (o11y must not block the system).
        let _ = self.tx.try_send(span);
    }
}

struct JsonVisitor<'a> {
    out: &'a mut std::collections::BTreeMap<String, serde_json::Value>,
}

impl<'a> JsonVisitor<'a> {
    fn new(out: &'a mut std::collections::BTreeMap<String, serde_json::Value>) -> Self {
        Self { out }
    }
}

impl tracing::field::Visit for JsonVisitor<'_> {
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.out
            .insert(field.name().to_string(), serde_json::Value::Bool(value));
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.out.insert(
            field.name().to_string(),
            serde_json::Value::Number(value.into()),
        );
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if let Some(n) = serde_json::Number::from_f64(value as f64) {
            self.out
                .insert(field.name().to_string(), serde_json::Value::Number(n));
        }
    }
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if let Some(n) = serde_json::Number::from_f64(value) {
            self.out
                .insert(field.name().to_string(), serde_json::Value::Number(n));
        }
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.out.insert(
            field.name().to_string(),
            serde_json::Value::String(value.to_string()),
        );
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.out.insert(
            field.name().to_string(),
            serde_json::Value::String(format!("{value:?}")),
        );
    }
}

pub struct O11yRuntime {
    shutdown_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl O11yRuntime {
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        self.task
            .await
            .map_err(|e| Error::backend("o11y runtime join", e))?;
        Ok(())
    }
}

#[tracing::instrument(level = "info", skip_all)]
pub fn init_global_from_env() -> Result<Option<O11yRuntime>> {
    let cfg = O11yConfig::from_env()?;
    init_global(cfg)
}

#[tracing::instrument(level = "info", skip_all)]
pub fn init_global(cfg: O11yConfig) -> Result<Option<O11yRuntime>> {
    cfg.validate()?;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt = tracing_subscriber::fmt::layer().json().with_target(true);

    let exporters = exporters_from_config(&cfg)?;
    if exporters.is_empty() {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt)
            .try_init()
            .map_err(|e| Error::Conflict(format!("tracing already initialized: {e}")))?;
        return Ok(None);
    }

    let (tx, rx) = mpsc::channel::<OtelSpan>(cfg.span_buffer_capacity);
    let capture = CaptureLayer::new(tx);

    let redaction = RedactionPolicy::new(cfg.redaction_level, cfg.redaction_patterns.clone())?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(export_loop(
        rx,
        exporters,
        redaction,
        cfg.batch_size,
        cfg.flush_interval,
        shutdown_rx,
    ));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(capture)
        .try_init()
        .map_err(|e| Error::Conflict(format!("tracing already initialized: {e}")))?;

    Ok(Some(O11yRuntime { shutdown_tx, task }))
}

#[tracing::instrument(level = "debug", skip_all)]
fn exporters_from_config(cfg: &O11yConfig) -> Result<Vec<Arc<dyn O11yExporter>>> {
    let mut out: Vec<Arc<dyn O11yExporter>> = Vec::new();

    if cfg.console {
        out.push(Arc::new(console::ConsoleExporter::stdout()));
    }

    if let Some(endpoint) = &cfg.otlp_endpoint {
        out.push(Arc::new(otel::OtlpExporter::new(
            endpoint,
            cfg.otlp_protocol,
            cfg.service_name.clone(),
            cfg.otlp_headers.clone(),
            cfg.otlp_timeout,
        )?));
    }

    Ok(out)
}

#[tracing::instrument(level = "debug", skip_all)]
async fn export_loop(
    mut rx: mpsc::Receiver<OtelSpan>,
    exporters: Vec<Arc<dyn O11yExporter>>,
    redaction: RedactionPolicy,
    batch_size: usize,
    flush_interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut buf: Vec<OtelSpan> = Vec::with_capacity(batch_size.max(1));
    let mut tick = time::interval(flush_interval);
    tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                if !buf.is_empty() {
                    flush_batch(&exporters, &redaction, &mut buf).await;
                }
            }
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    break;
                }
            }
            maybe = rx.recv() => {
                let Some(span) = maybe else { break };
                buf.push(span);
                if buf.len() >= batch_size.max(1) {
                    flush_batch(&exporters, &redaction, &mut buf).await;
                }
            }
        }
    }

    if !buf.is_empty() {
        flush_batch(&exporters, &redaction, &mut buf).await;
    }
    for ex in &exporters {
        if let Err(e) = ex.flush().await {
            tracing::warn!(exporter = ex.name(), error = %e, "o11y exporter flush failed");
        }
    }
    for ex in &exporters {
        if let Err(e) = ex.shutdown().await {
            tracing::warn!(exporter = ex.name(), error = %e, "o11y exporter shutdown failed");
        }
    }
}

#[tracing::instrument(level = "debug", skip_all)]
async fn flush_batch(
    exporters: &[Arc<dyn O11yExporter>],
    redaction: &RedactionPolicy,
    buf: &mut Vec<OtelSpan>,
) {
    let mut spans = std::mem::take(buf);
    for s in &mut spans {
        redaction.redact_span(s);
    }

    for ex in exporters {
        if let Err(e) = ex.export(spans.clone()).await {
            tracing::warn!(exporter = ex.name(), error = %e, "o11y export failed");
        }
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(LUT[(b >> 4) as usize] as char);
        out.push(LUT[(b & 0x0f) as usize] as char);
    }
    out
}
