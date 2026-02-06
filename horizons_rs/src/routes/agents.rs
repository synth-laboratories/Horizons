use crate::error::ApiError;
use crate::extract::{IdentityHeader, OrgIdHeader, ProjectIdHeader};
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::routing::{get, post};
use futures_util::{StreamExt, stream::Stream};
use horizons_core::core_agents::traits::CoreAgents as _;
use horizons_core::models::{ProjectDbHandle, ProjectId};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct RunAgentRequest {
    pub project_id: Option<ProjectId>,
    pub agent_id: String,
    pub inputs: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct RunAgentResponse {
    pub result: horizons_core::core_agents::models::AgentRunResult,
}

#[derive(Debug, Deserialize)]
pub struct ChatAgentRequest {
    pub project_id: Option<ProjectId>,
    pub agent_id: String,
    pub inputs: Option<serde_json::Value>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/agents/run", post(run_agent))
        .route("/agents/chat", post(chat_sse))
        .route("/agents", get(list_agents))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn run_agent(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    IdentityHeader(identity): IdentityHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<RunAgentRequest>,
) -> Result<Json<RunAgentResponse>, ApiError> {
    let project_id = req
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    let handle = resolve_project_handle(org_id, project_id, &state).await?;

    let result = state
        .core_agents
        .run(
            org_id,
            project_id,
            handle,
            &req.agent_id,
            &identity,
            req.inputs,
        )
        .await?;
    Ok(Json(RunAgentResponse { result }))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn chat_sse(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    IdentityHeader(identity): IdentityHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<ChatAgentRequest>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>> + Send>, ApiError> {
    let project_id = req
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    let handle = resolve_project_handle(org_id, project_id, &state).await?;

    // Minimal, general-purpose streaming: emit start/result/done.
    let (tx, rx) = tokio::sync::mpsc::channel::<SseEvent>(8);
    let agent_id = req.agent_id.clone();
    let inputs = req.inputs.clone();
    let identity = identity.clone();
    let core_agents = state.core_agents.clone();

    tokio::spawn(async move {
        let _ = tx
            .send(sse_json(
                "start",
                serde_json::json!({
                    "org_id": org_id.to_string(),
                    "project_id": project_id.to_string(),
                    "agent_id": agent_id.clone(),
                    "at": chrono::Utc::now().to_rfc3339(),
                }),
            ))
            .await;

        let res = core_agents
            .run(org_id, project_id, handle, &agent_id, &identity, inputs)
            .await;

        match res {
            Ok(result) => {
                let _ = tx
                    .send(sse_json("result", serde_json::json!({ "result": result })))
                    .await;
            }
            Err(e) => {
                let _ = tx
                    .send(sse_json(
                        "error",
                        serde_json::json!({ "error": e.to_string() }),
                    ))
                    .await;
            }
        }

        let _ = tx.send(SseEvent::default().event("done")).await;
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok);
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn list_agents(
    OrgIdHeader(_org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
) -> Result<Json<Vec<String>>, ApiError> {
    Ok(Json(state.core_agents.list_registered_agent_ids().await))
}

#[tracing::instrument(level = "debug", skip_all)]
async fn resolve_project_handle(
    org_id: horizons_core::OrgId,
    project_id: ProjectId,
    state: &AppState,
) -> Result<ProjectDbHandle, ApiError> {
    let handle = state
        .project_db
        .get_handle(org_id, project_id)
        .await?
        .ok_or_else(|| {
            ApiError::Core(horizons_core::Error::NotFound(
                "project not provisioned".to_string(),
            ))
        })?;
    Ok(handle)
}

fn sse_json(event: &str, value: serde_json::Value) -> SseEvent {
    // Avoid panics in the SSE path; fall back to a string message.
    match serde_json::to_string(&value) {
        Ok(s) => SseEvent::default().event(event).data(s),
        Err(e) => SseEvent::default()
            .event("error")
            .data(format!("sse serialization failed: {e}")),
    }
}
