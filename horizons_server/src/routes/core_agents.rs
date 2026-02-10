use crate::error::ApiError;
use crate::extract::{OrgIdHeader, ProjectIdHeader};
use crate::server::AppState;
use axum::extract::{Path, Query};
use axum::routing::{get, post};
use axum::{Extension, Json};
use horizons_core::core_agents::models::AgentSchedule;
use horizons_core::models::ProjectId;
use horizons_core::onboard::models::{CoreAgentRecord, ListQuery};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct ListCoreAgentsQuery {
    pub project_id: Option<ProjectId>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct UpsertCoreAgentRequest {
    pub project_id: Option<ProjectId>,
    pub agent_id: String,
    pub name: String,
    pub sandbox_image: Option<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    pub schedule: Option<AgentSchedule>,
    pub enabled: Option<bool>,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct SetTimeEnabledRequest {
    pub project_id: Option<ProjectId>,
    pub enabled: bool,
}

#[derive(Debug, Serialize)]
pub struct SetTimeEnabledResponse {
    pub updated: u64,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/core_agents", get(list_core_agents).put(upsert_core_agent))
        .route(
            "/core_agents/{agent_id}",
            get(get_core_agent).delete(delete_core_agent),
        )
        .route("/core_agents/time_enabled", post(set_time_enabled))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn list_core_agents(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<ListCoreAgentsQuery>,
) -> Result<Json<Vec<CoreAgentRecord>>, ApiError> {
    let project_id = q
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    let limit = q.limit.unwrap_or(100).min(1000);
    let offset = q.offset.unwrap_or(0);

    let rows = state
        .central_db
        .list_core_agents(org_id, project_id, ListQuery { limit, offset })
        .await?;
    Ok(Json(rows))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn get_core_agent(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(agent_id): Path<String>,
    Query(q): Query<ListCoreAgentsQuery>,
) -> Result<Json<Option<CoreAgentRecord>>, ApiError> {
    let project_id = q
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    let row = state
        .central_db
        .get_core_agent(org_id, project_id, &agent_id)
        .await?;
    Ok(Json(row))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn upsert_core_agent(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<UpsertCoreAgentRequest>,
) -> Result<Json<CoreAgentRecord>, ApiError> {
    let project_id = req
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;

    validate_schedule(&req.schedule)?;

    let now = chrono::Utc::now();
    let existing = state
        .central_db
        .get_core_agent(org_id, project_id, &req.agent_id)
        .await?;

    let enabled = req.enabled.unwrap_or_else(|| match &req.schedule {
        Some(AgentSchedule::Cron(_)) | Some(AgentSchedule::Interval { .. }) => true,
        _ => true,
    });

    let next_run_at = match &req.schedule {
        Some(AgentSchedule::Cron(_)) | Some(AgentSchedule::Interval { .. }) => {
            if !enabled {
                None
            } else if let Some(ex) = &existing {
                // Preserve next_run_at if schedule is unchanged and already set.
                if ex.schedule == req.schedule && ex.next_run_at.is_some() {
                    ex.next_run_at
                } else {
                    Some(now)
                }
            } else {
                Some(now)
            }
        }
        _ => None,
    };

    let record = CoreAgentRecord {
        org_id,
        project_id,
        agent_id: req.agent_id,
        name: req.name,
        sandbox_image: req.sandbox_image,
        tools: req.tools,
        schedule: req.schedule,
        enabled,
        next_run_at,
        config: req.config,
        created_at: existing.as_ref().map(|r| r.created_at).unwrap_or(now),
        updated_at: now,
    };

    state.central_db.upsert_core_agent(&record).await?;
    Ok(Json(record))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn delete_core_agent(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(agent_id): Path<String>,
    Query(q): Query<ListCoreAgentsQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let project_id = q
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    state
        .central_db
        .delete_core_agent(org_id, project_id, &agent_id)
        .await?;
    Ok(Json(serde_json::json!({ "deleted": true })))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn set_time_enabled(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<SetTimeEnabledRequest>,
) -> Result<Json<SetTimeEnabledResponse>, ApiError> {
    let project_id = req
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;

    let updated = state
        .central_db
        .set_core_agents_time_enabled(org_id, project_id, req.enabled)
        .await?;
    Ok(Json(SetTimeEnabledResponse { updated }))
}

fn validate_schedule(schedule: &Option<AgentSchedule>) -> std::result::Result<(), ApiError> {
    if let Some(AgentSchedule::Cron(expr)) = schedule {
        if expr.trim().is_empty() {
            return Err(ApiError::InvalidInput(
                "cron expression is empty".to_string(),
            ));
        }
    }
    if let Some(AgentSchedule::OnEvent { topic }) = schedule {
        if topic.trim().is_empty() {
            return Err(ApiError::InvalidInput(
                "on_event topic is empty".to_string(),
            ));
        }
    }
    if let Some(AgentSchedule::Interval { seconds }) = schedule {
        if *seconds == 0 {
            return Err(ApiError::InvalidInput(
                "interval seconds must be > 0".to_string(),
            ));
        }
    }
    Ok(())
}
