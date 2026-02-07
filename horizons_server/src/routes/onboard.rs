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
use std::time::Duration;
use tokio::time::timeout;
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
    #[serde(default)]
    pub truncated: bool,
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

fn env_usize(key: &str) -> Option<usize> {
    std::env::var(key).ok()?.trim().parse::<usize>().ok()
}

fn env_bool(key: &str) -> Option<bool> {
    let v = std::env::var(key).ok()?;
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum SqlKind {
    ReadOnly,
    Write,
}

fn classify_sql(sql: &str) -> Result<SqlKind, ApiError> {
    let s = sql.trim();
    if s.is_empty() {
        return Err(ApiError::InvalidInput("sql is empty".to_string()));
    }

    // Disallow obvious multi-statement payloads (keep the rules simple for now).
    // Allow a trailing semicolon only.
    if let Some(idx) = s.find(';') {
        if s[idx + 1..].trim().is_empty() {
            // trailing semicolon ok
        } else {
            return Err(ApiError::InvalidInput(
                "multiple SQL statements are not allowed".to_string(),
            ));
        }
    }

    // Very small classifier: first keyword.
    let first = s
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let kind = match first.as_str() {
        "select" | "with" | "show" | "pragma" | "explain" => SqlKind::ReadOnly,
        _ => SqlKind::Write,
    };
    Ok(kind)
}

fn apply_select_limit(sql: &str, max_rows: usize) -> String {
    // Best-effort limit injection for SELECT/WITH queries without an explicit LIMIT.
    // This is intentionally conservative: we do not attempt full SQL parsing here.
    let s = sql.trim().trim_end_matches(';').trim().to_string();
    let lower = s.to_ascii_lowercase();
    if !(lower.starts_with("select") || lower.starts_with("with")) {
        return s;
    }
    if lower.contains(" limit ") || lower.ends_with(" limit") {
        return s;
    }
    format!("{s} LIMIT {max_rows}")
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn query(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(project_id): Path<Uuid>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let max_rows = env_usize("HORIZONS_PROJECTDB_MAX_ROWS").unwrap_or(1000);
    let timeout_ms = env_usize("HORIZONS_PROJECTDB_TIMEOUT_MS").unwrap_or(10_000);

    let project_id = horizons_core::ProjectId(project_id);
    let handle = state
        .project_db
        .get_handle(org_id, project_id)
        .await?
        .ok_or_else(|| horizons_core::Error::NotFound("project not found".to_string()))?;

    if classify_sql(&req.sql)? != SqlKind::ReadOnly {
        return Err(ApiError::Core(horizons_core::Error::Unauthorized(
            "write SQL is not allowed in /query".to_string(),
        )));
    }

    let sql = apply_select_limit(&req.sql, max_rows);

    let rows = timeout(
        Duration::from_millis(timeout_ms as u64),
        state.project_db.query(org_id, &handle, &sql, &req.params),
    )
    .await
    .map_err(|_| ApiError::InvalidInput("project db query timed out".to_string()))??;

    let truncated = rows.len() > max_rows;
    let rows = if truncated {
        rows.into_iter().take(max_rows).collect()
    } else {
        rows
    };

    Ok(Json(QueryResponse { rows, truncated }))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn execute(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(project_id): Path<Uuid>,
    Json(req): Json<SqlRequest>,
) -> Result<Json<ExecuteResponse>, ApiError> {
    let timeout_ms = env_usize("HORIZONS_PROJECTDB_TIMEOUT_MS").unwrap_or(10_000);
    let allow_writes = env_bool("HORIZONS_PROJECTDB_ALLOW_WRITES").unwrap_or(false);

    let project_id = horizons_core::ProjectId(project_id);
    let handle = state
        .project_db
        .get_handle(org_id, project_id)
        .await?
        .ok_or_else(|| horizons_core::Error::NotFound("project not found".to_string()))?;

    if !allow_writes {
        return Err(ApiError::Core(horizons_core::Error::Unauthorized(
            "project db writes are disabled (set HORIZONS_PROJECTDB_ALLOW_WRITES=true to enable)"
                .to_string(),
        )));
    }
    if classify_sql(&req.sql)? != SqlKind::Write {
        return Err(ApiError::InvalidInput(
            "/execute expects a write statement".to_string(),
        ));
    }

    let rows_affected = state
        .project_db
        .execute(org_id, &handle, &req.sql, &req.params);
    let rows_affected = timeout(Duration::from_millis(timeout_ms as u64), rows_affected)
        .await
        .map_err(|_| ApiError::InvalidInput("project db execute timed out".to_string()))??;
    Ok(Json(ExecuteResponse { rows_affected }))
}
