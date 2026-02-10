//! HTTP routes for sandbox engine lifecycle.
//!
//! Provides endpoints to run coding agents in sandboxes, stream events,
//! check health, and release sandbox handles.

use crate::error::ApiError;
use crate::extract::OrgIdHeader;
use crate::server::AppState;
use axum::extract::Path;
use axum::extract::Query;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Extension, Json};
use futures_util::{StreamExt, stream::Stream};
use horizons_core::Error as CoreError;
use horizons_core::engine::models::{AgentKind, PermissionMode, SandboxConfig};
use horizons_core::engine::sandbox_agent_client::SandboxAgentClient;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

fn inherit_env_if_missing(env_vars: &mut HashMap<String, String>, key: &str) {
    if env_vars.contains_key(key) {
        return;
    }
    if let Ok(v) = std::env::var(key) {
        let t = v.trim();
        if !t.is_empty() {
            env_vars.insert(key.to_string(), t.to_string());
        }
    }
}

fn inject_common_llm_env(env_vars: &mut HashMap<String, String>) {
    // Keep this conservative. The intent is dev ergonomics: let the server's environment
    // supply keys so callers (like Dhakka UI) don't have to paste secrets into requests.
    for k in [
        "OPENAI_API_KEY",
        "OPENAI_BASE_URL",
        "OPENAI_ORG_ID",
        "OPENAI_PROJECT_ID",
        "ANTHROPIC_API_KEY",
        "GROQ_API_KEY",
        "GOOGLE_API_KEY",
        "GRAPH_LLM_API_KEY",
    ] {
        inherit_env_if_missing(env_vars, k);
    }
}

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

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    #[serde(default)]
    pub offset: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct SendMessageRequest {
    pub message: String,
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
        .route("/engine/{handle_id}/message", post(send_message))
        .route("/engine/{handle_id}/release", post(release_engine))
        .route("/engine/{handle_id}/health", get(health_engine))
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn not_found_run(handle_id: &str) -> ApiError {
    ApiError::Core(CoreError::NotFound(format!(
        "sandbox run not found: {handle_id}"
    )))
}

fn require_run_for_org(
    runtime: &horizons_core::engine::sandbox_runtime::SandboxRuntime,
    org_id: &horizons_core::models::OrgId,
    handle_id: &str,
) -> Result<horizons_core::engine::sandbox_runtime::ActiveSandboxRun, ApiError> {
    let run = runtime
        .get_active(handle_id)
        .ok_or_else(|| not_found_run(handle_id))?;

    // Org-level authorization: avoid handle-based cross-tenant access by ensuring the
    // caller's org matches the tracked run's org. Return 404 on mismatch to reduce
    // handle enumeration surface area.
    if run.org_id != org_id.to_string() {
        return Err(not_found_run(handle_id));
    }

    Ok(run)
}

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
    inject_common_llm_env(&mut env_vars);

    let config = SandboxConfig {
        agent: req.agent,
        model: req.model,
        permission_mode: req.permission_mode,
        image: req.image,
        env_vars,
        timeout_seconds: req.timeout_seconds,
        workdir: None,
        restart_policy: None,
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
    inject_common_llm_env(&mut env_vars);

    let config = SandboxConfig {
        agent: req.agent,
        model: req.model,
        permission_mode: req.permission_mode,
        image: req.image,
        env_vars,
        timeout_seconds: req.timeout_seconds,
        workdir: None,
        restart_policy: None,
    };

    let run = runtime
        .start_agent_tracked(_org_id.to_string(), None, &config, &req.instruction)
        .await?;

    Ok(Json(StartEngineResponse {
        handle_id: run.run_id,
        session_id: run.session_id,
        sandbox_agent_url: run.sandbox.sandbox_agent_url,
    }))
}

/// GET /api/v1/engine/:handle_id/events
///
/// SSE stream of events from a running sandbox agent session.
#[tracing::instrument(level = "info", skip_all)]
async fn events_sse(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(handle_id): Path<String>,
    Query(q): Query<EventsQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>> + Send>, ApiError> {
    let runtime = state
        .sandbox_runtime
        .as_ref()
        .ok_or_else(|| ApiError::InvalidInput("sandbox runtime not configured".to_string()))?;
    let run = require_run_for_org(runtime, &org_id, &handle_id)?;

    let client = SandboxAgentClient::new(&run.sandbox.sandbox_agent_url, None);
    let (tx, rx) = tokio::sync::mpsc::channel::<SseEvent>(64);

    tokio::spawn(async move {
        let mut offset: u64 = q.offset.unwrap_or(0);

        loop {
            match client.fetch_events(&run.session_id, offset).await {
                Ok(resp) => {
                    for event in &resp.events {
                        if let Some(seq) = event.get("sequence").and_then(|v| v.as_u64()) {
                            if seq > offset {
                                offset = seq;
                            }
                        }

                        let data = serde_json::to_string(event).unwrap_or_default();
                        let sse = SseEvent::default().event("event").data(data);
                        if tx.send(sse).await.is_err() {
                            return; // Client disconnected.
                        }

                        // Check for session ended.
                        if event.get("type").and_then(|t| t.as_str()) == Some("session.ended") {
                            let _ = tx.send(SseEvent::default().event("done")).await;
                            return;
                        }
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(
                            SseEvent::default()
                                .event("error")
                                .data(format!("event poll error: {e}")),
                        )
                        .await;
                    return;
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok);
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// POST /api/v1/engine/:handle_id/message
///
/// Send a follow-up message to an existing tracked sandbox session.
#[tracing::instrument(level = "info", skip_all)]
async fn send_message(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(handle_id): Path<String>,
    Json(req): Json<SendMessageRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let runtime = state
        .sandbox_runtime
        .as_ref()
        .ok_or_else(|| ApiError::InvalidInput("sandbox runtime not configured".to_string()))?;
    let run = require_run_for_org(runtime, &org_id, &handle_id)?;

    let msg = req.message.trim();
    if msg.is_empty() {
        return Err(ApiError::InvalidInput(
            "invalid input: missing message".to_string(),
        ));
    }

    let client = SandboxAgentClient::new(&run.sandbox.sandbox_agent_url, None);
    client.send_message(&run.session_id, msg).await?;

    Ok(Json(serde_json::json!({
        "ok": true,
        "handle_id": handle_id,
        "session_id": run.session_id,
    })))
}

/// POST /api/v1/engine/:handle_id/release
///
/// Release a sandbox by its handle.
#[tracing::instrument(level = "info", skip_all)]
async fn release_engine(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(handle_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let runtime = state
        .sandbox_runtime
        .as_ref()
        .ok_or_else(|| ApiError::InvalidInput("sandbox runtime not configured".to_string()))?;

    // Enforce org-level authorization on handle-scoped release.
    let _run = require_run_for_org(runtime, &org_id, &handle_id)?;
    runtime.release_run(&handle_id).await?;

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
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(handle_id): Path<String>,
) -> Result<Json<HealthResponse>, ApiError> {
    let runtime = state
        .sandbox_runtime
        .as_ref()
        .ok_or_else(|| ApiError::InvalidInput("sandbox runtime not configured".to_string()))?;
    let run = require_run_for_org(runtime, &org_id, &handle_id)?;

    let client = SandboxAgentClient::new(&run.sandbox.sandbox_agent_url, None);
    let healthy = client.health().await.unwrap_or(false);

    Ok(Json(HealthResponse { healthy }))
}
