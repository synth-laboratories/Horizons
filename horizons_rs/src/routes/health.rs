use crate::server::AppState;
use axum::Extension;
use axum::Json;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub uptime_ms: u128,
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn get_health(Extension(state): Extension<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        uptime_ms: state.started_at.elapsed().as_millis(),
    })
}
