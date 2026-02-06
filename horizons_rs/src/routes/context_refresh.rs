use crate::error::ApiError;
use crate::extract::{IdentityHeader, OrgIdHeader};
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::extract::Query;
use axum::routing::{get, post};
use chrono::Utc;
use horizons_core::context_refresh::models::{
    CronSchedule, EventTriggerConfig, RefreshRunQuery, RefreshTrigger, SourceConfig,
};
use horizons_core::context_refresh::schedule::CronExpr;
use horizons_core::context_refresh::traits::{RefreshResult, RefreshStatus};
use horizons_core::onboard::traits::ListQuery;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct TriggerRefreshRequest {
    pub source_id: String,
    pub trigger: Option<RefreshTrigger>,
}

#[derive(Debug, Deserialize)]
pub struct StatusQuery {
    pub source_id: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub statuses: Vec<RefreshStatus>,
    pub recent_runs: Vec<horizons_core::context_refresh::models::RefreshRun>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterSourceRequest {
    pub project_id: horizons_core::ProjectId,
    /// Unique identifier within an org, e.g. "gmail:inbox:primary".
    pub source_id: String,
    /// Connector implementation identifier, e.g. "gmail" or "hubspot".
    pub connector_id: String,
    /// Sync-state scope (connector-defined partition key).
    pub scope: String,
    pub enabled: Option<bool>,
    /// Optional 5-field cron expression (UTC), e.g. "0 */6 * * *".
    pub schedule_expr: Option<String>,
    pub event_triggers: Option<Vec<EventTriggerConfig>>,
    /// Connector-specific configuration blob (auth references, filters, etc.).
    pub settings: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ListSourcesResponse {
    pub sources: Vec<SourceConfig>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/context-refresh/run", post(trigger_refresh))
        .route("/context-refresh/status", get(get_status))
        .route("/connectors", post(register_source).get(list_sources))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn trigger_refresh(
    OrgIdHeader(org_id): OrgIdHeader,
    IdentityHeader(identity): IdentityHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<TriggerRefreshRequest>,
) -> Result<Json<RefreshResult>, ApiError> {
    let trigger = req.trigger.unwrap_or(RefreshTrigger::OnDemand);
    let out = state
        .context_refresh
        .trigger_refresh(&identity, org_id, &req.source_id, trigger)
        .await?;
    Ok(Json(out))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn get_status(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<StatusQuery>,
) -> Result<Json<StatusResponse>, ApiError> {
    let limit = q.limit.unwrap_or(25);
    let offset = q.offset.unwrap_or(0);

    let sources = state
        .central_db
        .list_source_configs(
            org_id,
            ListQuery {
                limit: 1000,
                offset: 0,
            },
        )
        .await?;

    let mut statuses = Vec::new();
    if let Some(source_id) = q.source_id.as_deref() {
        let st = state.context_refresh.get_status(org_id, source_id).await?;
        statuses.push(st);
    } else {
        for s in sources.iter() {
            let st = state
                .context_refresh
                .get_status(org_id, &s.source_id)
                .await?;
            statuses.push(st);
        }
    }

    let recent_runs = state
        .central_db
        .list_refresh_runs(
            org_id,
            RefreshRunQuery {
                source_id: q.source_id.clone(),
                limit,
                offset,
                ..Default::default()
            },
        )
        .await?;

    Ok(Json(StatusResponse {
        statuses,
        recent_runs,
    }))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn register_source(
    OrgIdHeader(org_id): OrgIdHeader,
    IdentityHeader(identity): IdentityHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<RegisterSourceRequest>,
) -> Result<Json<SourceConfig>, ApiError> {
    let Some(project_db) = state.project_db.get_handle(org_id, req.project_id).await? else {
        return Err(ApiError::Core(horizons_core::Error::NotFound(
            "project not provisioned".to_string(),
        )));
    };

    let schedule = match req.schedule_expr.as_deref() {
        None => None,
        Some(expr) => {
            let cron = CronExpr::parse(expr)?;
            let next_run_at = cron.next_after(Utc::now())?;
            Some(CronSchedule::new(expr.to_string(), next_run_at)?)
        }
    };

    let src = SourceConfig::new(
        org_id,
        req.project_id,
        project_db,
        req.source_id,
        req.connector_id,
        req.scope,
        req.enabled.unwrap_or(true),
        schedule,
        req.event_triggers.unwrap_or_default(),
        req.settings.unwrap_or_else(|| serde_json::json!({})),
        Some(Utc::now()),
    )?;
    state
        .context_refresh
        .register_source(&identity, src.clone())
        .await?;
    Ok(Json(src))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn list_sources(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
) -> Result<Json<ListSourcesResponse>, ApiError> {
    let sources = state
        .central_db
        .list_source_configs(
            org_id,
            ListQuery {
                limit: 1000,
                offset: 0,
            },
        )
        .await?;
    Ok(Json(ListSourcesResponse { sources }))
}
