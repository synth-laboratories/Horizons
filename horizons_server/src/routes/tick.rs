use crate::error::ApiError;
use crate::extract::{OrgIdHeader, ProjectIdHeader};
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::routing::post;
use horizons_core::models::{OrgId, ProjectId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct TickRequest {
    pub project_id: Option<ProjectId>,
    /// Consumer-friendly alias (slug or UUID).
    #[serde(default, alias = "project")]
    pub project: Option<String>,
    /// Compatibility fields (currently ignored by real scheduler).
    #[serde(default)]
    pub advance_seconds: Option<i64>,
    #[serde(default)]
    pub mode: Option<String>,
    pub max_runs_per_tick: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct TickResponse {
    pub result: horizons_core::core_agents::scheduler::TickResult,
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
    axum::Router::new().route("/tick", post(tick))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn tick(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<TickRequest>,
) -> Result<Json<TickResponse>, ApiError> {
    let project_id = match (req.project_id.or(project_id_h), req.project.as_deref()) {
        (Some(pid), _) => pid,
        (None, Some(s)) => resolve_project_id_from_slug(&state, org_id, s).await?,
        (None, None) => {
            return Err(ApiError::InvalidInput(
                "project_id (or project) is required".to_string(),
            ));
        }
    };
    let max_runs_per_tick = req.max_runs_per_tick.unwrap_or(1);

    // `advance_seconds`/`mode` are compatibility fields used by some external orchestrators.
    // Horizons currently uses real wall-clock time for scheduling; we accept these fields
    // but do not apply a mock clock shift here.
    let res = state
        .core_scheduler
        .tick_project(org_id, project_id, chrono::Utc::now(), max_runs_per_tick)
        .await?;
    Ok(Json(TickResponse { result: res }))
}
