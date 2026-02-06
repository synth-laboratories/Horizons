use crate::error::ApiError;
use crate::extract::{IdentityHeader, OrgIdHeader, ProjectIdHeader};
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::extract::Query;
use axum::routing::{get, post};
use horizons_core::models::{ProjectDbHandle, ProjectId};
use horizons_core::optimization::engine::{OptimizationArtifact, OptimizationRunRow};
use horizons_core::optimization::traits::{Dataset, MiproConfig, Policy};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct RunRequest {
    pub project_id: Option<ProjectId>,
    pub cfg: MiproConfig,
    pub initial_policy: Policy,
    pub dataset: Dataset,
}

#[derive(Debug, Deserialize)]
pub struct StatusQuery {
    pub project_id: Option<ProjectId>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ReportsQuery {
    pub project_id: Option<ProjectId>,
    pub run_id: Option<Uuid>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct CycleRequest {
    pub agent_id: String,
    #[serde(default)]
    pub memory_horizon_seconds: Option<u64>,
    #[serde(default)]
    pub memory_limit: Option<usize>,
    #[serde(default)]
    pub min_improvement: Option<f64>,
    #[serde(default)]
    pub eval_samples: Option<usize>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/optimization/run", post(run))
        .route("/optimization/status", get(status))
        .route("/optimization/reports", get(reports))
        .route("/optimization/cycles", post(cycle))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn run(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    IdentityHeader(identity): IdentityHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<RunRequest>,
) -> Result<Json<OptimizationRunRow>, ApiError> {
    let project_id = req
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    let handle = resolve_project_handle(org_id, project_id, &state).await?;

    let out = state
        .optimization
        .run(
            org_id,
            project_id,
            &handle,
            &identity,
            req.cfg,
            req.initial_policy,
            req.dataset,
        )
        .await?;
    Ok(Json(out))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn status(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id_h): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<StatusQuery>,
) -> Result<Json<Vec<OptimizationRunRow>>, ApiError> {
    let project_id = q
        .project_id
        .or(project_id_h)
        .ok_or_else(|| ApiError::InvalidInput("project_id is required".to_string()))?;
    let handle = resolve_project_handle(org_id, project_id, &state).await?;
    let out = state
        .optimization
        .list_status(
            org_id,
            project_id,
            &handle,
            q.limit.unwrap_or(50),
            q.offset.unwrap_or(0),
        )
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

    if let Some(run_id) = q.run_id {
        let report: OptimizationArtifact = state
            .optimization
            .get_report(org_id, project_id, &handle, run_id)
            .await?;
        return Ok(Json(serde_json::to_value(report).map_err(|e| {
            ApiError::Core(horizons_core::Error::backend(
                "serialize optimization report",
                e,
            ))
        })?));
    }

    let rows = state
        .optimization
        .list_status(
            org_id,
            project_id,
            &handle,
            q.limit.unwrap_or(50),
            q.offset.unwrap_or(0),
        )
        .await?;
    Ok(Json(serde_json::json!({ "runs": rows })))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn cycle(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<CycleRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    #[cfg(all(feature = "memory", feature = "optimization", feature = "evaluation"))]
    {
        let cfg = horizons_core::optimization::continual::CycleConfig {
            memory_horizon: std::time::Duration::from_secs(
                req.memory_horizon_seconds.unwrap_or(60 * 60),
            ),
            memory_limit: req.memory_limit.unwrap_or(100),
            min_improvement: req.min_improvement.unwrap_or(0.0),
            eval_samples: req.eval_samples.unwrap_or(10),
        };
        let result = state
            .continual_learning
            .run_cycle(org_id, &req.agent_id, &cfg)
            .await?;
        return Ok(Json(serde_json::to_value(result).map_err(|e| {
            ApiError::Core(horizons_core::Error::backend("serialize cycle result", e))
        })?));
    }

    #[cfg(not(all(feature = "memory", feature = "optimization", feature = "evaluation")))]
    {
        let _ = (org_id, state, req);
        Err(ApiError::InvalidInput(
            "continual learning requires features: memory + optimization + evaluation".to_string(),
        ))
    }
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
