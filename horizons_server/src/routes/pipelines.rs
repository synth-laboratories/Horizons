use crate::error::ApiError;
use crate::extract::{IdentityHeader, OrgIdHeader, ProjectIdHeader};
use crate::server::AppState;
use axum::extract::Path;
use axum::routing::{get, post};
use axum::{Extension, Json};
use horizons_core::models::ProjectId;
use horizons_core::pipelines::models::{PipelineRun, PipelineSpec, StepKind};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct RunPipelineRequest {
    pub spec: PipelineSpec,
    #[serde(default)]
    pub inputs: serde_json::Value,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/pipelines/run", post(run))
        .route("/pipelines/runs/{id}", get(get_run))
        .route("/pipelines/runs/{id}/approve/{step_id}", post(approve))
        .route("/pipelines/runs/{id}/cancel", post(cancel))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn run(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    IdentityHeader(identity): IdentityHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(mut req): Json<RunPipelineRequest>,
) -> Result<Json<PipelineRun>, ApiError> {
    // Enforce tenant scoping from the request header.
    req.spec.org_id = org_id.to_string();

    // Convenience: if a project is selected, inject `_project_db_handle` into agent step contexts
    // when missing so CoreAgents-backed subagents can persist their outputs.
    let project_id: Option<ProjectId> = project_id_h;
    if let Some(project_id) = project_id {
        let handle = state
            .project_db
            .get_handle(org_id, project_id)
            .await?
            .ok_or_else(|| {
                ApiError::Core(horizons_core::Error::NotFound(
                    "project not provisioned".to_string(),
                ))
            })?;
        for step in req.spec.steps.iter_mut() {
            if let StepKind::Agent { spec } = &mut step.kind {
                if let serde_json::Value::Object(m) = &mut spec.context {
                    m.entry("_project_db_handle".to_string())
                        .or_insert_with(|| serde_json::to_value(&handle).unwrap_or(serde_json::Value::Null));
                } else if spec.context.is_null() {
                    spec.context = serde_json::json!({ "_project_db_handle": handle });
                } else {
                    spec.context = serde_json::json!({ "_project_db_handle": handle, "context": spec.context });
                }
            }
        }
    }

    let run = state.pipelines.run(&req.spec, req.inputs, &identity).await?;
    Ok(Json(run))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn get_run(
    Extension(state): Extension<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<PipelineRun>, ApiError> {
    let Some(run) = state.pipelines.get_run(&id).await? else {
        return Err(ApiError::Core(horizons_core::Error::NotFound(format!(
            "pipeline run not found: {id}"
        ))));
    };
    Ok(Json(run))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn approve(
    IdentityHeader(identity): IdentityHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path((id, step_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state
        .pipelines
        .approve_step(&id, &step_id, &identity)
        .await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn cancel(
    Extension(state): Extension<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state.pipelines.cancel(&id).await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}
