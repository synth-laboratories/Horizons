use crate::error::ApiError;
use crate::extract::{IdentityHeader, OrgIdHeader, ProjectIdHeader};
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::extract::{Path, Query};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use chrono::Utc;
use horizons_core::core_agents::models::{ActionProposal, RiskLevel};
use horizons_core::core_agents::traits::CoreAgents as _;
use horizons_core::models::{OrgId, ProjectDbHandle, ProjectId};
use horizons_core::onboard::traits::ListQuery;
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
    /// Consumer-friendly alias for `project_id` (slug or UUID).
    #[serde(default, alias = "project")]
    pub project: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct CompatAction {
    pub id: String,
    pub agent_name: String,
    pub action_type: String,
    pub description: String,
    pub proposed_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub risk_level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

async fn resolve_project_id_from_slug(
    state: &AppState,
    org_id: OrgId,
    slug_or_uuid: &str,
) -> Result<ProjectId, ApiError> {
    let s = slug_or_uuid.trim();
    if s.is_empty() {
        return Err(ApiError::InvalidInput("project is empty".to_string()));
    }
    if let Ok(uuid) = Uuid::parse_str(s) {
        return Ok(ProjectId(uuid));
    }
    let rec = state
        .central_db
        .get_project_by_slug(org_id, s)
        .await?
        .ok_or_else(|| {
            ApiError::Core(horizons_core::Error::NotFound(format!(
                "project not found for slug '{s}'"
            )))
        })?;
    Ok(rec.project_id)
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
    req: Option<Json<DecideActionRequest>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let action_id = Uuid::parse_str(&id).map_err(|e| ApiError::InvalidInput(e.to_string()))?;

    let req = req.map(|j| j.0);
    let project_id = req.as_ref().and_then(|r| r.project_id).or(project_id_h);
    if let Some(project_id) = project_id {
        let handle = resolve_project_handle(org_id, project_id, &state).await?;
        let reason = req
            .as_ref()
            .map(|r| r.reason.as_str())
            .unwrap_or("approved");
        state
            .core_agents
            .approve(org_id, project_id, &handle, action_id, &identity, reason)
            .await?;
        return Ok(Json(serde_json::json!({ "status": "ok" })));
    }

    // Fallback for older consumers: scan all org projects and approve the first match.
    let reason = req
        .as_ref()
        .map(|r| r.reason.as_str())
        .unwrap_or("approved");
    let mut offset = 0usize;
    let scan_limit = std::env::var("HORIZONS_ACTION_APPROVE_SCAN_LIMIT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(500);
    while offset < scan_limit {
        let batch = state
            .project_db
            .list_projects(org_id, ListQuery { limit: 100, offset })
            .await?;
        if batch.is_empty() {
            break;
        }
        for h in batch {
            let pid = h.project_id;
            match state
                .core_agents
                .approve(org_id, pid, &h, action_id, &identity, reason)
                .await
            {
                Ok(()) => return Ok(Json(serde_json::json!({ "status": "ok" }))),
                Err(horizons_core::Error::NotFound(_)) => {}
                Err(e) => return Err(ApiError::Core(e)),
            }
        }
        offset += 100;
    }

    Err(ApiError::Core(horizons_core::Error::NotFound(
        "action not found".to_string(),
    )))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn deny_action(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    IdentityHeader(identity): IdentityHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(id): Path<String>,
    req: Option<Json<DecideActionRequest>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let action_id = Uuid::parse_str(&id).map_err(|e| ApiError::InvalidInput(e.to_string()))?;

    let req = req.map(|j| j.0);
    let project_id = req.as_ref().and_then(|r| r.project_id).or(project_id_h);
    if let Some(project_id) = project_id {
        let handle = resolve_project_handle(org_id, project_id, &state).await?;
        let reason = req.as_ref().map(|r| r.reason.as_str()).unwrap_or("denied");
        state
            .core_agents
            .deny(org_id, project_id, &handle, action_id, &identity, reason)
            .await?;
        return Ok(Json(serde_json::json!({ "status": "ok" })));
    }

    // Fallback for older consumers: scan all org projects and deny the first match.
    let reason = req.as_ref().map(|r| r.reason.as_str()).unwrap_or("denied");
    let mut offset = 0usize;
    let scan_limit = std::env::var("HORIZONS_ACTION_DENY_SCAN_LIMIT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(500);
    while offset < scan_limit {
        let batch = state
            .project_db
            .list_projects(org_id, ListQuery { limit: 100, offset })
            .await?;
        if batch.is_empty() {
            break;
        }
        for h in batch {
            let pid = h.project_id;
            match state
                .core_agents
                .deny(org_id, pid, &h, action_id, &identity, reason)
                .await
            {
                Ok(()) => return Ok(Json(serde_json::json!({ "status": "ok" }))),
                Err(horizons_core::Error::NotFound(_)) => {}
                Err(e) => return Err(ApiError::Core(e)),
            }
        }
        offset += 100;
    }

    Err(ApiError::Core(horizons_core::Error::NotFound(
        "action not found".to_string(),
    )))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn list_pending(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<ListPendingQuery>,
) -> Result<axum::response::Response, ApiError> {
    let project_id = match (q.project_id.or(project_id_h), q.project.as_deref()) {
        (Some(pid), _) => pid,
        (None, Some(s)) => resolve_project_id_from_slug(&state, org_id, s).await?,
        (None, None) => {
            return Err(ApiError::InvalidInput(
                "project_id (or project) is required".to_string(),
            ));
        }
    };
    let handle = resolve_project_handle(org_id, project_id, &state).await?;
    let limit = q.limit.unwrap_or(100);
    let offset = q.offset.unwrap_or(0);
    let out = state
        .core_agents
        .list_pending(org_id, project_id, &handle, limit, offset)
        .await?;

    if q.project.is_some() && q.project_id.is_none() {
        let out: Vec<CompatAction> = out
            .into_iter()
            .map(|a| {
                let ttl = (a.expires_at - a.created_at).num_seconds();
                CompatAction {
                    id: a.id.to_string(),
                    agent_name: a.agent_id,
                    action_type: a.action_type.clone(),
                    description: a.action_type,
                    proposed_at: a.created_at.timestamp(),
                    risk_level: Some(format!("{:?}", a.risk_level).to_ascii_lowercase()),
                    ttl_seconds: Some(ttl.max(0)),
                    details: Some(a.payload),
                }
            })
            .collect();
        return Ok(Json(out).into_response());
    }

    Ok(Json(out).into_response())
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
