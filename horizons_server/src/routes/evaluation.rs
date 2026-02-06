use crate::error::ApiError;
use crate::extract::{IdentityHeader, OrgIdHeader, ProjectIdHeader};
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::extract::Query;
use axum::routing::{get, post};
use horizons_core::evaluation::engine::{EvalArtifact, EvalReportRow};
use horizons_core::evaluation::traits::VerificationCase;
use horizons_core::models::{ProjectDbHandle, ProjectId};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct RunRequest {
    pub project_id: Option<ProjectId>,
    pub case: VerificationCase,
}

#[derive(Debug, Deserialize)]
pub struct ReportsQuery {
    pub project_id: Option<ProjectId>,
    pub report_id: Option<Uuid>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/eval/run", post(run))
        .route("/eval/reports", get(reports))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn run(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    IdentityHeader(identity): IdentityHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<RunRequest>,
) -> Result<Json<EvalReportRow>, ApiError> {
    let project_id = req
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    let handle = resolve_project_handle(org_id, project_id, &state).await?;

    let out = state
        .evaluation
        .run(org_id, project_id, &handle, &identity, req.case)
        .await?;
    Ok(Json(out))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn reports(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<ReportsQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let project_id = q
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    let handle = resolve_project_handle(org_id, project_id, &state).await?;

    if let Some(report_id) = q.report_id {
        let report: EvalArtifact = state
            .evaluation
            .get_report(org_id, project_id, &handle, report_id)
            .await?;
        return Ok(Json(serde_json::to_value(report).map_err(|e| {
            ApiError::Core(horizons_core::Error::backend("serialize eval report", e))
        })?));
    }

    let rows = state
        .evaluation
        .list_reports(
            org_id,
            project_id,
            &handle,
            q.limit.unwrap_or(50),
            q.offset.unwrap_or(0),
        )
        .await?;
    Ok(Json(serde_json::json!({ "reports": rows })))
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
