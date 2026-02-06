use crate::error::ApiError;
use crate::extract::OrgIdHeader;
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::extract::Query;
use axum::routing::get;
use chrono::{DateTime, Utc};
use horizons_core::models::{AuditEntry, ProjectId};
use horizons_core::onboard::traits::AuditQuery;
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct AuditQueryParams {
    pub project_id: Option<Uuid>,
    pub actor_contains: Option<String>,
    pub action_prefix: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new().route("/audit", get(list_audit))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn list_audit(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<AuditQueryParams>,
) -> Result<Json<Vec<AuditEntry>>, ApiError> {
    let query = AuditQuery {
        project_id: q.project_id.map(ProjectId),
        actor_contains: q.actor_contains,
        action_prefix: q.action_prefix,
        since: q.since,
        until: q.until,
        limit: q.limit.unwrap_or(100),
    };
    let entries = state.central_db.list_audit_entries(org_id, query).await?;
    Ok(Json(entries))
}
