use std::sync::Arc;

use axum::extract::{Path, Query};
use axum::routing::{get, post};
use axum::{Extension, Json};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::extract::OrgIdHeader;
use crate::server::AppState;
use horizons_core::onboard::models::{OperationRecord, ResourceRecord};
use horizons_core::onboard::traits::ListQuery;

#[derive(Debug, Deserialize)]
pub struct UpsertResourceRequest {
    pub resource_id: String,
    pub resource_type: String,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct UpsertOperationRequest {
    pub operation_id: String,
    pub resource_id: String,
    pub operation_type: String,
    #[serde(default)]
    pub config: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct ListQueryParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct ListRunsQuery {
    pub operation_id: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct UpsertResponse {
    pub ok: bool,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route(
            "/assets/resources",
            post(upsert_resource).get(list_resources),
        )
        .route("/assets/resources/{id}", get(get_resource))
        .route(
            "/assets/operations",
            post(upsert_operation).get(list_operations),
        )
        .route("/assets/operations/{id}", get(get_operation))
        .route("/assets/operations/runs", get(list_operation_runs))
}

#[tracing::instrument(level = "info", skip_all)]
async fn upsert_resource(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<UpsertResourceRequest>,
) -> Result<Json<UpsertResponse>, ApiError> {
    let now = Utc::now();
    let record = ResourceRecord {
        org_id,
        resource_id: req.resource_id,
        resource_type: req.resource_type,
        config: req.config,
        created_at: now,
        updated_at: now,
    };
    state.central_db.upsert_resource(&record).await?;
    Ok(Json(UpsertResponse { ok: true }))
}

#[tracing::instrument(level = "debug", skip_all)]
async fn get_resource(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ResourceRecord>, ApiError> {
    let r = state
        .central_db
        .get_resource(org_id, &id)
        .await?
        .ok_or_else(|| {
            ApiError::Core(horizons_core::Error::NotFound(format!(
                "resource not found: {id}"
            )))
        })?;
    Ok(Json(r))
}

#[tracing::instrument(level = "debug", skip_all)]
async fn list_resources(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<ListQueryParams>,
) -> Result<Json<Vec<ResourceRecord>>, ApiError> {
    let out = state
        .central_db
        .list_resources(
            org_id,
            ListQuery {
                limit: q.limit.unwrap_or(100),
                offset: q.offset.unwrap_or(0),
            },
        )
        .await?;
    Ok(Json(out))
}

#[tracing::instrument(level = "info", skip_all)]
async fn upsert_operation(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<UpsertOperationRequest>,
) -> Result<Json<UpsertResponse>, ApiError> {
    let now = Utc::now();
    let record = OperationRecord {
        org_id,
        operation_id: req.operation_id,
        resource_id: req.resource_id,
        operation_type: req.operation_type,
        config: req.config,
        created_at: now,
        updated_at: now,
    };
    state.central_db.upsert_operation(&record).await?;
    Ok(Json(UpsertResponse { ok: true }))
}

#[tracing::instrument(level = "debug", skip_all)]
async fn get_operation(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<OperationRecord>, ApiError> {
    let r = state
        .central_db
        .get_operation(org_id, &id)
        .await?
        .ok_or_else(|| {
            ApiError::Core(horizons_core::Error::NotFound(format!(
                "operation not found: {id}"
            )))
        })?;
    Ok(Json(r))
}

#[tracing::instrument(level = "debug", skip_all)]
async fn list_operations(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<ListQueryParams>,
) -> Result<Json<Vec<OperationRecord>>, ApiError> {
    let out = state
        .central_db
        .list_operations(
            org_id,
            ListQuery {
                limit: q.limit.unwrap_or(100),
                offset: q.offset.unwrap_or(0),
            },
        )
        .await?;
    Ok(Json(out))
}

#[tracing::instrument(level = "debug", skip_all)]
async fn list_operation_runs(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<ListRunsQuery>,
) -> Result<Json<Vec<horizons_core::onboard::models::OperationRunRecord>>, ApiError> {
    let out = state
        .central_db
        .list_operation_runs(
            org_id,
            q.operation_id.as_deref(),
            ListQuery {
                limit: q.limit.unwrap_or(100),
                offset: q.offset.unwrap_or(0),
            },
        )
        .await?;
    Ok(Json(out))
}
