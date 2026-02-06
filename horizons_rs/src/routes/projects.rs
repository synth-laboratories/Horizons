use crate::error::ApiError;
use crate::extract::OrgIdHeader;
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::extract::Query;
use axum::routing::post;
use horizons_core::models::{ProjectDbHandle, ProjectId};
use horizons_core::onboard::traits::ListQuery;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct CreateProjectRequest {
    pub project_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct CreateProjectResponse {
    pub handle: ProjectDbHandle,
}

#[derive(Debug, Deserialize)]
pub struct ListProjectsQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new().route("/projects", post(create_project).get(list_projects))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn create_project(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<CreateProjectRequest>,
) -> Result<Json<CreateProjectResponse>, ApiError> {
    let project_id = ProjectId(req.project_id.unwrap_or_else(Uuid::new_v4));
    let handle = state.project_db.provision(org_id, project_id).await?;
    Ok(Json(CreateProjectResponse { handle }))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn list_projects(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<ListProjectsQuery>,
) -> Result<Json<Vec<ProjectDbHandle>>, ApiError> {
    let query = ListQuery {
        limit: q.limit.unwrap_or(100),
        offset: q.offset.unwrap_or(0),
    };
    let projects = state.project_db.list_projects(org_id, query).await?;
    Ok(Json(projects))
}
