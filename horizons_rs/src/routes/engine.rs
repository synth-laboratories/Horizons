//! HTTP routes for sandbox engine lifecycle.
//!
//! Provides endpoints to run coding agents in sandboxes, stream events,
//! check health, and release sandbox handles.

use crate::error::ApiError;
use crate::extract::OrgIdHeader;
use crate::server::AppState;
use axum::extract::Path;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Extension, Json};
use futures_util::{StreamExt, stream::Stream};
use horizons_core::engine::models::{AgentKind, PermissionMode, SandboxConfig};
use horizons_core::engine::sandbox_agent_client::SandboxAgentClient;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct RunEngineRequest {
    /// Which agent to run: "codex", "claude", "opencode".
    pub agent: AgentKind,
    /// The instruction to send to the agent.
    pub instruction: String,
    /// LLM model override.
    #[serde(default)]
    pub model: Option<String>,
    /// Permission mode: "bypass" (default) or "plan".
    #[serde(default = "default_permission_mode")]
    pub permission_mode: PermissionMode,
    /// Docker image override.
    #[serde(default)]
    pub image: Option<String>,
    /// Environment variables (API keys, etc).
    #[serde(default)]
    pub env_vars: HashMap<String, String>,
    /// Timeout in seconds (default: 1800).
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}

fn default_permission_mode() -> PermissionMode {
    PermissionMode::Bypass
}

fn default_timeout() -> u64 {
    1800
}

#[derive(Debug, Serialize)]
pub struct RunEngineResponse {
    pub handle_id: String,
    pub completed: bool,
    pub duration_seconds: f64,
    pub event_count: usize,
    pub final_output: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StartEngineResponse {
    pub handle_id: String,
    pub session_id: String,
    pub sandbox_agent_url: String,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub healthy: bool,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/engine/run", post(run_engine))
        .route("/engine/start", post(start_engine))
        .route("/engine/{handle_id}/events", get(events_sse))
        .route("/engine/{handle_id}/release", post(release_engine))
        .route("/engine/{handle_id}/health", get(health_engine))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// POST /api/v1/engine/run
///
/// One-shot: provision sandbox, run agent, collect results, release.
#[tracing::instrument(level = "info", skip_all)]
async fn run_engine(
    OrgIdHeader(_org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<RunEngineRequest>,
) -> Result<Json<RunEngineResponse>, ApiError> {
    let runtime = state
        .sandbox_runtime
        .as_ref()
        .ok_or_else(|| ApiError::InvalidInput("sandbox runtime not configured".to_string()))?;

    let mut env_vars = req.env_vars;
    if state.mcp_gateway.is_some() && !env_vars.contains_key("MCP_GATEWAY_URL") {
        let url = std::env::var("MCP_GATEWAY_URL")
            .or_else(|_| std::env::var("HORIZONS_MCP_GATEWAY_URL"))
            .ok();
        if let Some(url) = url {
            if !url.trim().is_empty() {
                env_vars.insert("MCP_GATEWAY_URL".to_string(), url);
            }
        }
    }

    let config = SandboxConfig {
        agent: req.agent,
        model: req.model,
        permission_mode: req.permission_mode,
        image: req.image,
        env_vars,
        timeout_seconds: req.timeout_seconds,
        no_progress_timeout_seconds: 180,
        workdir: None,
        docker_socket: false,
        restart_policy: None,
        log_tags: std::collections::HashMap::new(),
    };

    let (result, handle) = runtime.run_agent(&config, &req.instruction).await?;

    Ok(Json(RunEngineResponse {
        handle_id: handle.id,
        completed: result.completed,
        duration_seconds: result.duration_seconds,
        event_count: result.events.len(),
        final_output: result.final_output,
        error: result.error,
    }))
}

/// POST /api/v1/engine/start
///
/// Start an agent without waiting — returns handle + session for SSE streaming.
#[tracing::instrument(level = "info", skip_all)]
async fn start_engine(
    OrgIdHeader(_org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<RunEngineRequest>,
) -> Result<Json<StartEngineResponse>, ApiError> {
    let runtime = state
        .sandbox_runtime
        .as_ref()
        .ok_or_else(|| ApiError::InvalidInput("sandbox runtime not configured".to_string()))?;

    let mut env_vars = req.env_vars;
    if state.mcp_gateway.is_some() && !env_vars.contains_key("MCP_GATEWAY_URL") {
        let url = std::env::var("MCP_GATEWAY_URL")
            .or_else(|_| std::env::var("HORIZONS_MCP_GATEWAY_URL"))
            .ok();
        if let Some(url) = url {
            if !url.trim().is_empty() {
                env_vars.insert("MCP_GATEWAY_URL".to_string(), url);
            }
        }
    }

    let config = SandboxConfig {
        agent: req.agent,
        model: req.model,
        permission_mode: req.permission_mode,
        image: req.image,
        env_vars,
        timeout_seconds: req.timeout_seconds,
        no_progress_timeout_seconds: 180,
        workdir: None,
        docker_socket: false,
        restart_policy: None,
        log_tags: std::collections::HashMap::new(),
    };

    let (handle, session_id, _last_sse, _channels) = runtime.start_agent(&config, &req.instruction).await?;

    // Store the handle for later access.
    {
        let mut handles = state.sandbox_handles.write().await;
        handles.insert(handle.id.clone(), handle.clone());
    }

    Ok(Json(StartEngineResponse {
        handle_id: handle.id,
        session_id,
        sandbox_agent_url: handle.sandbox_agent_url,
    }))
}

/// GET /api/v1/engine/:handle_id/events
///
/// SSE stream of events from a running sandbox agent session.
#[tracing::instrument(level = "info", skip_all)]
async fn events_sse(
    OrgIdHeader(_org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(handle_id): Path<String>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>> + Send>, ApiError> {
    let handle = {
        let handles = state.sandbox_handles.read().await;
        handles.get(&handle_id).cloned().ok_or_else(|| {
            ApiError::Core(horizons_core::Error::NotFound(format!(
                "sandbox handle not found: {handle_id}"
            )))
        })?
    };

    let client = SandboxAgentClient::new(&handle.sandbox_agent_url, None);
    let (tx, rx) = tokio::sync::mpsc::channel::<SseEvent>(64);

    // Proxy the REST SSE event stream to the HTTP client.
    let session_id = handle_id.clone();
    tokio::spawn(async move {
        let resp = match client.events_sse_connect(&session_id).await {
            Ok(r) => r,
            Err(e) => {
                let _ = tx
                    .send(
                        SseEvent::default()
                            .event("error")
                            .data(format!("SSE connect error: {e}")),
                    )
                    .await;
                return;
            }
        };

        let mut stream = resp.bytes_stream();
        let mut buf = String::new();

        while let Some(chunk) = stream.next().await {
            let bytes = match chunk {
                Ok(b) => b,
                Err(e) => {
                    let _ = tx
                        .send(
                            SseEvent::default()
                                .event("error")
                                .data(format!("SSE stream error: {e}")),
                        )
                        .await;
                    return;
                }
            };

            buf.push_str(&String::from_utf8_lossy(&bytes));

            while let Some(pos) = buf.find("\n\n") {
                let frame = buf[..pos].to_string();
                buf = buf[pos + 2..].to_string();

                for line in frame.lines() {
                    if let Some(data) = line.strip_prefix("data: ") {
                        let sse = SseEvent::default().event("event").data(data.to_string());
                        if tx.send(sse).await.is_err() {
                            return;
                        }
                    }
                }
            }
        }

        let _ = tx.send(SseEvent::default().event("done")).await;
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok);
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// POST /api/v1/engine/:handle_id/release
///
/// Release a sandbox by its handle.
#[tracing::instrument(level = "info", skip_all)]
async fn release_engine(
    OrgIdHeader(_org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(handle_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let runtime = state
        .sandbox_runtime
        .as_ref()
        .ok_or_else(|| ApiError::InvalidInput("sandbox runtime not configured".to_string()))?;

    let handle = {
        let mut handles = state.sandbox_handles.write().await;
        handles.remove(&handle_id).ok_or_else(|| {
            ApiError::Core(horizons_core::Error::NotFound(format!(
                "sandbox handle not found: {handle_id}"
            )))
        })?
    };

    runtime.release(&handle).await?;

    Ok(Json(serde_json::json!({
        "released": true,
        "handle_id": handle_id,
    })))
}

/// GET /api/v1/engine/:handle_id/health
///
/// Check if the sandbox-agent in the container is healthy.
#[tracing::instrument(level = "debug", skip_all)]
async fn health_engine(
    OrgIdHeader(_org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(handle_id): Path<String>,
) -> Result<Json<HealthResponse>, ApiError> {
    let handle = {
        let handles = state.sandbox_handles.read().await;
        handles.get(&handle_id).cloned().ok_or_else(|| {
            ApiError::Core(horizons_core::Error::NotFound(format!(
                "sandbox handle not found: {handle_id}"
            )))
        })?
    };

    let client = SandboxAgentClient::new(&handle.sandbox_agent_url, None);
    let healthy = client.health().await.unwrap_or(false);

    Ok(Json(HealthResponse { healthy }))
}
