use crate::error::ApiError;
use crate::extract::OrgIdHeader;
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::extract::Query;
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use horizons_core::memory::traits::{MemoryItem, MemoryType, RetrievalQuery, Scope, Summary};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct MemoryQuery {
    pub agent_id: String,
    pub q: String,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct PutMemoryRequest {
    pub agent_id: String,
    pub item_type: MemoryType,
    pub content: serde_json::Value,
    pub index_text: Option<String>,
    pub importance_0_to_1: Option<f32>,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct PutMemoryResponse {
    pub id: ulid::Ulid,
}

#[derive(Debug, Deserialize)]
pub struct SummarizeRequest {
    pub agent_id: String,
    /// Horizon string like "7d", "24h", "60m".
    pub horizon: String,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/memory", get(get_memory).post(put_memory))
        .route("/memory/summarize", post(summarize))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn get_memory(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<MemoryQuery>,
) -> Result<Json<Vec<MemoryItem>>, ApiError> {
    if q.agent_id.trim().is_empty() {
        return Err(ApiError::InvalidInput("agent_id is required".to_string()));
    }
    if q.q.trim().is_empty() {
        return Err(ApiError::InvalidInput("q is required".to_string()));
    }
    let query = RetrievalQuery::new(q.q, q.limit.unwrap_or(20));
    let items = state.memory.retrieve(org_id, &q.agent_id, query).await?;
    Ok(Json(items))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn put_memory(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<PutMemoryRequest>,
) -> Result<Json<PutMemoryResponse>, ApiError> {
    if req.agent_id.trim().is_empty() {
        return Err(ApiError::InvalidInput("agent_id is required".to_string()));
    }
    let created_at = req.created_at.unwrap_or_else(Utc::now);

    let scope = Scope::new(org_id.to_string(), req.agent_id.clone());
    let mut item = MemoryItem::new(&scope, req.item_type, req.content, created_at);
    if let Some(t) = req.index_text {
        item = item.with_index_text(t);
    }
    if let Some(s) = req.importance_0_to_1 {
        item = item.with_importance(s);
    }

    let id = state.memory.append_item(org_id, item).await?;
    Ok(Json(PutMemoryResponse { id }))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn summarize(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<SummarizeRequest>,
) -> Result<Json<Summary>, ApiError> {
    if req.agent_id.trim().is_empty() {
        return Err(ApiError::InvalidInput("agent_id is required".to_string()));
    }
    let out = state
        .memory
        .summarize(org_id, &req.agent_id, &req.horizon)
        .await?;
    Ok(Json(out))
}
