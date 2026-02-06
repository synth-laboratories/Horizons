use crate::error::ApiError;
use crate::extract::{OrgIdHeader, ProjectIdHeader};
use crate::server::AppState;
use axum::Extension;
use axum::body::Body;
use axum::extract::Path;
use axum::http::{Response, StatusCode, header};
use axum::routing::put;
use bytes::Bytes;
use std::sync::Arc;

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new().route(
        "/files/{*key}",
        put(put_file).get(get_file).delete(delete_file),
    )
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn put_file(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(key): Path<String>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    state.filestore.put(org_id, project_id, &key, body).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn get_file(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Response<Body>, ApiError> {
    let Some(bytes) = state.filestore.get(org_id, project_id, &key).await? else {
        return Err(horizons_core::Error::NotFound("file not found".to_string()).into());
    };
    let mut resp = Response::new(Body::from(bytes));
    *resp.status_mut() = StatusCode::OK;
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/octet-stream"),
    );
    Ok(resp)
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn delete_file(
    OrgIdHeader(org_id): OrgIdHeader,
    ProjectIdHeader(project_id): ProjectIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<StatusCode, ApiError> {
    state.filestore.delete(org_id, project_id, &key).await?;
    Ok(StatusCode::NO_CONTENT)
}
