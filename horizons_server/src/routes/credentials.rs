//! Credential management routes (non-UI).
//!
//! Backed by `CredentialManager` (AES-256-GCM encrypted at rest in CentralDb).

use std::sync::Arc;

use axum::extract::Path;
use axum::routing::get;
use axum::{Extension, Json};

use crate::error::ApiError;
use crate::extract::OrgIdHeader;
use crate::server::AppState;

fn mask_value(v: &serde_json::Value) -> serde_json::Value {
    if let Some(s) = v.as_str() {
        if s.len() > 8 {
            serde_json::Value::String(format!("{}...{}", &s[..4], &s[s.len() - 4..]))
        } else if !s.is_empty() {
            serde_json::Value::String("****".to_string())
        } else {
            serde_json::Value::String(String::new())
        }
    } else {
        v.clone()
    }
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/credentials", get(list_credentials))
        .route(
            "/credentials/{connector_id}",
            get(get_credential_info)
                .put(put_credentials)
                .delete(delete_credentials),
        )
}

/// GET /api/v1/credentials — list all stored connector credentials (values masked).
#[tracing::instrument(level = "debug", skip_all)]
async fn list_credentials(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Some(cm) = state.credential_manager.as_ref() else {
        return Err(ApiError::InvalidInput(
            "credential manager not configured".to_string(),
        ));
    };

    let ids = cm.list_connector_ids(org_id).await?;
    let mut masked = serde_json::Map::new();
    for id in ids {
        match cm.retrieve(org_id, &id).await {
            Ok(Some(settings)) => {
                if let Some(obj) = settings.as_object() {
                    let mut m = serde_json::Map::new();
                    for (k, v) in obj {
                        m.insert(k.clone(), mask_value(v));
                    }
                    masked.insert(id, serde_json::Value::Object(m));
                } else {
                    masked.insert(id, mask_value(&settings));
                }
            }
            _ => {
                masked.insert(id, serde_json::json!({}));
            }
        }
    }

    Ok(Json(serde_json::Value::Object(masked)))
}

/// PUT /api/v1/credentials/{connector_id} — store credentials for a connector.
#[tracing::instrument(level = "info", skip_all)]
async fn put_credentials(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(connector_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Some(cm) = state.credential_manager.as_ref() else {
        return Err(ApiError::InvalidInput(
            "credential manager not configured".to_string(),
        ));
    };
    if !body.is_object() {
        return Err(ApiError::InvalidInput(
            "body must be a JSON object".to_string(),
        ));
    }

    cm.store(org_id, &connector_id, body).await?;
    Ok(Json(
        serde_json::json!({ "connector_id": connector_id, "status": "saved" }),
    ))
}

/// GET /api/v1/credentials/{connector_id} — return configured key names (no secrets).
#[tracing::instrument(level = "debug", skip_all)]
async fn get_credential_info(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(connector_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Some(cm) = state.credential_manager.as_ref() else {
        return Err(ApiError::InvalidInput(
            "credential manager not configured".to_string(),
        ));
    };

    match cm.retrieve(org_id, &connector_id).await? {
        None => Ok(Json(serde_json::json!({
            "connector_id": connector_id,
            "configured_keys": [],
        }))),
        Some(settings) => {
            if let Some(obj) = settings.as_object() {
                let configured: Vec<String> = obj
                    .iter()
                    .filter(|(_, v)| v.as_str().map(|s| !s.is_empty()).unwrap_or(true))
                    .map(|(k, _)| k.clone())
                    .collect();
                Ok(Json(serde_json::json!({
                    "connector_id": connector_id,
                    "configured_keys": configured,
                })))
            } else {
                Ok(Json(serde_json::json!({ "connector_id": connector_id })))
            }
        }
    }
}

/// DELETE /api/v1/credentials/{connector_id}
#[tracing::instrument(level = "info", skip_all)]
async fn delete_credentials(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(connector_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Some(cm) = state.credential_manager.as_ref() else {
        return Err(ApiError::InvalidInput(
            "credential manager not configured".to_string(),
        ));
    };
    cm.delete(org_id, &connector_id).await?;
    Ok(Json(serde_json::json!({ "status": "deleted" })))
}
