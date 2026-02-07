use crate::error::ApiError;
use crate::extract::{IdentityHeader, OrgIdHeader, ProjectIdHeader};
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::extract::{Path, Query};
use axum::routing::{get, post};
use chrono::Utc;
use horizons_core::core_agents::models::{ActionProposal, RiskLevel};
use horizons_core::core_agents::traits::CoreAgents as _;
use horizons_core::models::{ProjectDbHandle, ProjectId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct ProposeActionRequest {
    pub project_id: Option<ProjectId>,
    pub agent_id: String,
    pub action_type: String,
    pub payload: serde_json::Value,
    pub risk_level: RiskLevel,
    #[serde(default)]
    pub dedupe_key: Option<String>,
    pub context: serde_json::Value,
    pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ProposeActionResponse {
    pub action_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct DecideActionRequest {
    pub project_id: Option<ProjectId>,
    pub reason: String,
}

#[derive(Debug, Deserialize)]
pub struct ListPendingQuery {
    pub project_id: Option<ProjectId>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/actions/propose", post(propose_action))
        .route("/actions/{id}/approve", post(approve_action))
        .route("/actions/{id}/deny", post(deny_action))
        .route("/actions/pending", get(list_pending))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn propose_action(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    IdentityHeader(identity): IdentityHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<ProposeActionRequest>,
) -> Result<Json<ProposeActionResponse>, ApiError> {
    let project_id = req
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    let handle = resolve_project_handle(org_id, project_id, &state).await?;

    let ttl = req.ttl_seconds.unwrap_or(60 * 60);
    let now = Utc::now();

    let proposal = ActionProposal::new(
        org_id,
        project_id,
        req.agent_id,
        req.action_type,
        req.payload,
        req.risk_level,
        req.dedupe_key,
        req.context,
        now,
        ttl,
    )?;

    let action_id = state
        .core_agents
        .propose_action(org_id, project_id, &handle, proposal, &identity)
        .await?;
    Ok(Json(ProposeActionResponse { action_id }))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn approve_action(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    IdentityHeader(identity): IdentityHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<DecideActionRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let project_id = req
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    let handle = resolve_project_handle(org_id, project_id, &state).await?;
    let action_id = Uuid::parse_str(&id).map_err(|e| ApiError::InvalidInput(e.to_string()))?;

    state
        .core_agents
        .approve(org_id, project_id, &handle, action_id, &identity, &req.reason)
        .await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn deny_action(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    IdentityHeader(identity): IdentityHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<DecideActionRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let project_id = req
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    let handle = resolve_project_handle(org_id, project_id, &state).await?;
    let action_id = Uuid::parse_str(&id).map_err(|e| ApiError::InvalidInput(e.to_string()))?;

    state
        .core_agents
        .deny(org_id, project_id, &handle, action_id, &identity, &req.reason)
        .await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn list_pending(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<ListPendingQuery>,
) -> Result<Json<Vec<ActionProposal>>, ApiError> {
    let project_id = q
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    let handle = resolve_project_handle(org_id, project_id, &state).await?;
    let limit = q.limit.unwrap_or(100);
    let offset = q.offset.unwrap_or(0);
    let out = state
        .core_agents
        .list_pending(org_id, project_id, &handle, limit, offset)
        .await?;
    Ok(Json(out))
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
