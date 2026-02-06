use crate::error::ApiError;
use crate::extract::OrgIdHeader;
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::extract::Path;
use axum::routing::post;
use horizons_core::onboard::traits::{ProjectDbParam, ProjectDbRow};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct SqlRequest {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<ProjectDbParam>,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub rows: Vec<ProjectDbRow>,
}

#[derive(Debug, Serialize)]
pub struct ExecuteResponse {
    pub rows_affected: u64,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/projects/{id}/query", post(query))
        .route("/projects/{id}/execute", post(execute))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn query(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(project_id): Path<Uuid>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let project_id = horizons_core::ProjectId(project_id);
    let handle = state
        .project_db
        .get_handle(org_id, project_id)
        .await?
        .ok_or_else(|| horizons_core::Error::NotFound("project not found".to_string()))?;
    let rows = state
        .project_db
        .query(org_id, &handle, &req.sql, &req.params)
        .await?;
    Ok(Json(QueryResponse { rows }))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn execute(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(project_id): Path<Uuid>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<ExecuteResponse>, ApiError> {
    let project_id = horizons_core::ProjectId(project_id);
    let handle = state
        .project_db
        .get_handle(org_id, project_id)
        .await?
        .ok_or_else(|| horizons_core::Error::NotFound("project not found".to_string()))?;
    let rows_affected = state
        .project_db
        .execute(org_id, &handle, &req.sql, &req.params)
        .await?;
    Ok(Json(ExecuteResponse { rows_affected }))
}
