use crate::error::ApiError;
use crate::extract::OrgIdHeader;
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::routing::get;
use chrono::Utc;
use horizons_core::models::PlatformConfig;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct PutConfigRequest {
    pub data: serde_json::Value,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new().route("/config", get(get_config).put(put_config))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn get_config(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
) -> Result<Json<PlatformConfig>, ApiError> {
    let cfg = state
        .central_db
        .get_platform_config(org_id)
        .await?
        .unwrap_or(PlatformConfig {
            org_id,
            data: serde_json::json!({}),
            updated_at: Utc::now(),
        });
    Ok(Json(cfg))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn put_config(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<PutConfigRequest>,
) -> Result<Json<PlatformConfig>, ApiError> {
    let cfg = PlatformConfig {
        org_id,
        data: req.data,
        updated_at: Utc::now(),
    };
    state.central_db.set_platform_config(&cfg).await?;
    Ok(Json(cfg))
}
