//! VictoriaLogs sink for sandbox-agent universal events.
//!
//! This is intentionally best-effort:
//! - never blocks agent execution
//! - drops events on backpressure
//! - logs failures, but doesn't fail the run
//!
//! It supports:
//! - centralized VictoriaLogs service (HTTP ingest)
//! - "onboard local" VictoriaLogs for a single Horizons box (spawns victoria-logs
//!   process if configured)

use crate::engine::models::{AgentKind, SandboxBackendKind};
use crate::{Error, Result};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VictoriaLogsMode {
    /// Send to a remote/centralized VictoriaLogs endpoint.
    Centralized,
    /// Start a local victoria-logs process for this Horizons "box" (single host)
    /// and send to localhost.
    OnboardLocal,
}

#[derive(Debug, Clone)]
pub struct VictoriaLogsLocalConfig {
    pub bin: String,
    pub storage_data_path: String,
    pub retention_period: String,
    pub http_listen_addr: String,
}

#[derive(Debug, Clone)]
pub struct VictoriaLogsConfig {
    pub mode: VictoriaLogsMode,
    /// Fully qualified insert URL (e.g. http://host:9428/insert/jsonline?...).
    pub insert_url: String,
    pub bearer_token: Option<String>,
    pub basic_user: Option<String>,
    pub basic_password: Option<String>,
    pub local: Option<VictoriaLogsLocalConfig>,
    pub batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct VictoriaSessionCtx {
    pub sandbox_id: String,
    pub sandbox_backend: SandboxBackendKind,
    pub sandbox_agent_url: String,
    pub host_port: u16,
    pub session_id: String,
    pub agent: AgentKind,
    pub tags: Arc<HashMap<String, String>>,
}

#[derive(Debug, Clone)]
struct Envelope {
    ctx: VictoriaSessionCtx,
    seq: u64,
    event: serde_json::Value,
}

#[derive(Clone)]
pub struct VictoriaLogsEmitter {
    tx: mpsc::Sender<Envelope>,
}

impl VictoriaLogsEmitter {
    pub fn from_env() -> Option<Self> {
        // Prefer service-specific vars, but allow a single global sink.
        let base = env_nonempty("HORIZONS_VICTORIA_LOGS_URL")
            .or_else(|| env_nonempty("SMR_VICTORIA_LOGS_URL"))
            .or_else(|| env_nonempty("SYNTH_VICTORIA_LOGS_URL"))
            .or_else(|| env_nonempty("VICTORIA_LOGS_URL"))
            .or_else(default_local_victoria_base)?;

        let mode = match env_nonempty("HORIZONS_VICTORIA_LOGS_MODE")
            .or_else(|| env_nonempty("SMR_VICTORIA_LOGS_MODE"))
            .or_else(|| env_nonempty("SYNTH_VICTORIA_LOGS_MODE"))
            .or_else(|| env_nonempty("VICTORIA_LOGS_MODE"))
            .as_deref()
        {
            Some("onboard_local") | Some("local") => VictoriaLogsMode::OnboardLocal,
            _ => VictoriaLogsMode::Centralized,
        };

        let mut insert_url = if base.contains("/insert/") {
            base
        } else {
            format!("{}/insert/jsonline", base.trim_end_matches('/'))
        };

        // Defaults for stream/time/message fields. Keep them stable across callers.
        if !insert_url.contains("_stream_fields=") {
            let sep = if insert_url.contains('?') { '&' } else { '?' };
            insert_url.push(sep);
            insert_url.push_str(
                "_stream_fields=service,component,deployment_env,host,sandbox_id,session_id",
            );
        }
        if !insert_url.contains("_time_field=") {
            insert_url.push(if insert_url.contains('?') { '&' } else { '?' });
            insert_url.push_str("_time_field=ts");
        }
        if !insert_url.contains("_msg_field=") {
            insert_url.push(if insert_url.contains('?') { '&' } else { '?' });
            insert_url.push_str("_msg_field=message");
        }

        let bearer_token = env_nonempty("HORIZONS_VICTORIA_LOGS_BEARER_TOKEN")
            .or_else(|| env_nonempty("SMR_VICTORIA_LOGS_BEARER_TOKEN"))
            .or_else(|| env_nonempty("SYNTH_VICTORIA_LOGS_BEARER_TOKEN"))
            .or_else(|| env_nonempty("VICTORIA_LOGS_BEARER_TOKEN"));
        let basic_user = env_nonempty("HORIZONS_VICTORIA_LOGS_BASIC_USER")
            .or_else(|| env_nonempty("SMR_VICTORIA_LOGS_BASIC_USER"))
            .or_else(|| env_nonempty("SYNTH_VICTORIA_LOGS_BASIC_USER"))
            .or_else(|| env_nonempty("VICTORIA_LOGS_BASIC_USER"));
        let basic_password = env_nonempty("HORIZONS_VICTORIA_LOGS_BASIC_PASSWORD")
            .or_else(|| env_nonempty("SMR_VICTORIA_LOGS_BASIC_PASSWORD"))
            .or_else(|| env_nonempty("SYNTH_VICTORIA_LOGS_BASIC_PASSWORD"))
            .or_else(|| env_nonempty("VICTORIA_LOGS_BASIC_PASSWORD"));

        let batch_size = env_nonempty("HORIZONS_VICTORIA_LOGS_BATCH_SIZE")
            .or_else(|| env_nonempty("SMR_VICTORIA_LOGS_BATCH_SIZE"))
            .or_else(|| env_nonempty("SYNTH_VICTORIA_LOGS_BATCH_SIZE"))
            .or_else(|| env_nonempty("VICTORIA_LOGS_BATCH_SIZE"))
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(50)
            .clamp(1, 500);

        let local = if mode == VictoriaLogsMode::OnboardLocal {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
            let default_data = format!("{home}/.cache/horizons/victoria-logs-data");
            let port = env_nonempty("HORIZONS_VICTORIA_LOGS_PORT")
                .or_else(|| env_nonempty("VICTORIA_LOGS_PORT"))
                .and_then(|v| v.parse::<u16>().ok())
                .unwrap_or(9428);
            let listen = env_nonempty("HORIZONS_VICTORIA_LOGS_LISTEN_ADDR")
                .or_else(|| env_nonempty("VICTORIA_LOGS_LISTEN_ADDR"))
                .unwrap_or_else(|| format!("127.0.0.1:{port}"));
            Some(VictoriaLogsLocalConfig {
                bin: env_nonempty("HORIZONS_VICTORIA_LOGS_BIN")
                    .or_else(|| env_nonempty("VICTORIA_LOGS_BIN"))
                    .unwrap_or_else(|| "victoria-logs".to_string()),
                storage_data_path: env_nonempty("HORIZONS_VICTORIA_LOGS_DATA_DIR")
                    .or_else(|| env_nonempty("VICTORIA_LOGS_DATA_DIR"))
                    .unwrap_or(default_data),
                retention_period: env_nonempty("HORIZONS_VICTORIA_LOGS_RETENTION")
                    .or_else(|| env_nonempty("VICTORIA_LOGS_RETENTION"))
                    .unwrap_or_else(|| "7d".to_string()),
                http_listen_addr: listen,
            })
        } else {
            None
        };

        let cfg = VictoriaLogsConfig {
            mode,
            insert_url,
            bearer_token,
            basic_user,
            basic_password,
            local,
            batch_size,
        };

        Some(Self::new(cfg))
    }

    pub fn new(cfg: VictoriaLogsConfig) -> Self {
        let (tx, mut rx) = mpsc::channel::<Envelope>(4096);
        let cfg = Arc::new(cfg);
        let local_proc: Arc<Mutex<Option<tokio::process::Child>>> = Arc::new(Mutex::new(None));
        let started_local: Arc<OnceLock<()>> = Arc::new(OnceLock::new());

        let http = reqwest::Client::new();
        let cfg_bg = cfg.clone();
        let local_bg = local_proc.clone();
        let started_bg = started_local.clone();

        tokio::spawn(async move {
            // Best-effort local process start (for a single Horizons box).
            if cfg_bg.mode == VictoriaLogsMode::OnboardLocal {
                if let Err(e) = ensure_local_victoria(&cfg_bg, &local_bg, &started_bg).await {
                    tracing::warn!(err = %e, "victoria-logs onboard_local start failed; continuing without guarantees");
                }
            }

            let mut buf: Vec<Envelope> = Vec::with_capacity(cfg_bg.batch_size);
            loop {
                // Refill buffer.
                buf.clear();
                match rx.recv().await {
                    Some(first) => buf.push(first),
                    None => break, // all senders dropped
                }
                while buf.len() < cfg_bg.batch_size {
                    match rx.try_recv() {
                        Ok(ev) => buf.push(ev),
                        Err(_) => break,
                    }
                }

                if let Err(e) = ingest_batch(&http, &cfg_bg, &buf).await {
                    tracing::warn!(err = %e, "victoria logs ingest failed");
                }
            }
        });

        Self { tx }
    }

    pub fn enabled(&self) -> bool {
        true
    }

    pub fn enqueue_event(&self, ctx: &VictoriaSessionCtx, seq: u64, event: &serde_json::Value) {
        let env = Envelope {
            ctx: ctx.clone(),
            seq,
            event: event.clone(),
        };
        // Never block the agent runtime; drop on backpressure.
        let _ = self.tx.try_send(env);
    }

    pub fn enqueue_session_completed(
        &self,
        ctx: &VictoriaSessionCtx,
        completed: bool,
        duration_seconds: f64,
        error: Option<&str>,
    ) {
        let record = serde_json::json!({
            "type": "horizons.session.completed",
            "data": {
                "completed": completed,
                "duration_seconds": duration_seconds,
                "error": error,
            }
        });
        self.enqueue_event(ctx, u64::MAX, &record);
    }
}

fn default_local_victoria_base() -> Option<String> {
    let deployment_env = env_nonempty("SMR_DEPLOYMENT_ENV")
        .or_else(|| env_nonempty("SYNTH_DEPLOYMENT_ENV"))
        .or_else(|| env_nonempty("DEPLOYMENT_ENV"))
        .unwrap_or_else(|| "local".to_string())
        .to_ascii_lowercase();
    let cp_url = env_nonempty("SMR_CONTROL_PLANE_URL")
        .or_else(|| env_nonempty("SYNTH_BACKEND_URL"))
        .or_else(|| env_nonempty("BACKEND_URL"))
        .unwrap_or_default()
        .to_ascii_lowercase();
    let force_local = env_nonempty("SMR_VICTORIA_LOGS_LOCAL_DEFAULT")
        .map(|v| {
            matches!(
                v.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    let is_local = deployment_env == "local"
        || cp_url.contains("localhost")
        || cp_url.contains("127.0.0.1")
        || force_local;
    if !is_local {
        return None;
    }
    let base = "http://127.0.0.1:9428".to_string();
    tracing::info!(
        url = %base,
        "horizons victoria logs URL not set; using local default"
    );
    Some(base)
}

async fn ingest_batch(
    http: &reqwest::Client,
    cfg: &VictoriaLogsConfig,
    buf: &[Envelope],
) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    }

    let deployment_env = env_nonempty("SMR_DEPLOYMENT_ENV")
        .or_else(|| env_nonempty("SYNTH_DEPLOYMENT_ENV"))
        .or_else(|| env_nonempty("DEPLOYMENT_ENV"))
        .unwrap_or_else(|| "local".to_string());
    let host = env_nonempty("HOSTNAME")
        .or_else(|| env_nonempty("SMR_MACHINE_ID"))
        .unwrap_or_else(|| "unknown".to_string());

    let mut body = String::new();
    for env in buf {
        let event_type = env
            .event
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let ts = env
            .event
            .get("time")
            .and_then(|v| v.as_str())
            .or_else(|| env.event.get("timestamp").and_then(|v| v.as_str()))
            .or_else(|| env.event.get("ts").and_then(|v| v.as_str()))
            .map(|s| s.to_string())
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        let mut record = serde_json::Map::new();
        record.insert("ts".to_string(), serde_json::Value::String(ts));
        record.insert(
            "service".to_string(),
            serde_json::Value::String("horizons".to_string()),
        );
        record.insert(
            "component".to_string(),
            serde_json::Value::String("sandbox".to_string()),
        );
        record.insert(
            "deployment_env".to_string(),
            serde_json::Value::String(deployment_env.clone()),
        );
        record.insert("host".to_string(), serde_json::Value::String(host.clone()));

        record.insert(
            "sandbox_id".to_string(),
            serde_json::Value::String(env.ctx.sandbox_id.clone()),
        );
        record.insert(
            "sandbox_backend".to_string(),
            serde_json::Value::String(format!("{:?}", env.ctx.sandbox_backend).to_lowercase()),
        );
        record.insert(
            "sandbox_agent_url".to_string(),
            serde_json::Value::String(env.ctx.sandbox_agent_url.clone()),
        );
        record.insert(
            "host_port".to_string(),
            serde_json::Value::Number(serde_json::Number::from(env.ctx.host_port as u64)),
        );

        record.insert(
            "session_id".to_string(),
            serde_json::Value::String(env.ctx.session_id.clone()),
        );
        record.insert(
            "agent".to_string(),
            serde_json::Value::String(env.ctx.agent.to_string()),
        );
        record.insert(
            "seq".to_string(),
            serde_json::Value::Number(serde_json::Number::from(env.seq)),
        );

        record.insert(
            "event_type".to_string(),
            serde_json::Value::String(event_type.to_string()),
        );
        record.insert(
            "message".to_string(),
            serde_json::Value::String(format!("universal event: {event_type}")),
        );
        record.insert("raw".to_string(), env.event.clone());

        for (k, v) in env.ctx.tags.iter() {
            // Preserve explicit record keys (do not overwrite).
            if !record.contains_key(k) {
                record.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
        }

        body.push_str(&serde_json::Value::Object(record).to_string());
        body.push('\n');
    }

    let mut req = http
        .post(&cfg.insert_url)
        .header("Content-Type", "application/stream+json")
        .body(body);

    if let Some(token) = cfg.bearer_token.as_deref() {
        req = req.bearer_auth(token);
    }
    if let Some(user) = cfg.basic_user.as_deref() {
        req = req.basic_auth(user, cfg.basic_password.clone());
    }

    let resp = req.send().await.map_err(Error::backend_reqwest)?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(Error::BackendMessage(format!(
            "victorialogs ingest failed: status={status} body={}",
            text.chars().take(2000).collect::<String>()
        )));
    }
    Ok(())
}

async fn ensure_local_victoria(
    cfg: &VictoriaLogsConfig,
    local_proc: &Arc<Mutex<Option<tokio::process::Child>>>,
    started: &Arc<OnceLock<()>>,
) -> Result<()> {
    if started.get().is_some() {
        return Ok(());
    }
    let Some(local) = cfg.local.as_ref() else {
        return Ok(());
    };

    // Ensure storage dir exists.
    std::fs::create_dir_all(&local.storage_data_path).map_err(|e| {
        Error::backend(
            format!("create victoria logs data dir {}", local.storage_data_path),
            e,
        )
    })?;

    // Try to connect health first; if it's already running, don't spawn.
    // We keep this non-fatal: local mode is best-effort.
    let base_url = format!("http://{}", local.http_listen_addr);
    let health_url = format!("{base_url}/health");
    if let Ok(resp) = reqwest::get(&health_url).await {
        if resp.status().is_success() {
            let _ = started.set(());
            return Ok(());
        }
    }

    // Spawn victoria-logs process once.
    {
        let mut guard = local_proc.lock().await;
        if guard.is_none() {
            tracing::info!(
                bin = %local.bin,
                data = %local.storage_data_path,
                listen = %local.http_listen_addr,
                retention = %local.retention_period,
                "starting victoria-logs (onboard_local)"
            );
            let mut cmd = tokio::process::Command::new(&local.bin);
            cmd.args([
                format!("-storageDataPath={}", local.storage_data_path),
                format!("-retentionPeriod={}", local.retention_period),
                format!("-httpListenAddr={}", local.http_listen_addr),
            ]);
            // Avoid inheriting stdin; keep it daemon-like.
            cmd.stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            let child = cmd
                .spawn()
                .map_err(|e| Error::backend("spawn victoria-logs", e))?;
            *guard = Some(child);
        }
    }

    // Wait for health.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if std::time::Instant::now() > deadline {
            break;
        }
        if let Ok(resp) = reqwest::get(&health_url).await {
            if resp.status().is_success() {
                let _ = started.set(());
                return Ok(());
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    Err(Error::BackendMessage(format!(
        "victoria-logs did not become healthy at {health_url} within 10s"
    )))
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}
