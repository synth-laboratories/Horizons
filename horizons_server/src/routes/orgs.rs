use crate::error::ApiError;
use crate::extract::MaybeOrgIdHeader;
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::body::Bytes;
use axum::http::HeaderMap;
use axum::routing::post;
use chrono::Utc;
use horizons_core::models::OrgId;
use horizons_core::onboard::traits::OrgRecord;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct CreateOrgRequest {
    /// Optional explicit org_id (mostly for tests/dev).
    #[serde(default)]
    pub org_id: Option<String>,
    /// Optional display name. Defaults to "org".
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateOrgResponse {
    pub org_id: String,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new().route("/orgs", post(create_org))
}

fn admin_token_ok(headers: &HeaderMap) -> bool {
    let Some(expected) = std::env::var("HORIZONS_ADMIN_TOKEN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    else {
        // Dev default: if no admin token is configured, allow provisioning.
        return true;
    };

    // Accept either:
    // - x-horizons-admin-token: <token>
    // - Authorization: Bearer <token>
    if let Some(got) = headers
        .get("x-horizons-admin-token")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim())
    {
        return got == expected;
    }

    if let Some(got) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            s.strip_prefix("Bearer ")
                .or_else(|| s.strip_prefix("bearer "))
        })
        .map(|s| s.trim())
    {
        return got == expected;
    }

    false
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn create_org(
    MaybeOrgIdHeader(org_id_h): MaybeOrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<CreateOrgResponse>, ApiError> {
    let req: CreateOrgRequest = if body.iter().all(|b| b.is_ascii_whitespace()) {
        CreateOrgRequest {
            org_id: None,
            name: None,
        }
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| ApiError::InvalidInput(format!("invalid json body: {e}")))?
    };

    // If the caller is already authenticated into an org (bearer api key), treat this as
    // "ensure org exists" and return it. This keeps consumer provisioning flows ergonomic.
    if let Some(org_id) = org_id_h {
        let name = req.name.as_deref().unwrap_or("org").trim().to_string();
        state
            .central_db
            .upsert_org(&OrgRecord {
                org_id,
                name,
                created_at: Utc::now(),
            })
            .await?;
        return Ok(Json(CreateOrgResponse {
            org_id: org_id.to_string(),
        }));
    }

    if !admin_token_ok(&headers) {
        return Err(ApiError::Core(horizons_core::Error::Unauthorized(
            "admin token required".to_string(),
        )));
    }

    let org_id = match req
        .org_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(s) => OrgId::from_str(s).map_err(|e| ApiError::InvalidOrgId(e.to_string()))?,
        None => OrgId(Uuid::new_v4()),
    };
    let name = req.name.as_deref().unwrap_or("org").trim().to_string();

    state
        .central_db
        .upsert_org(&OrgRecord {
            org_id,
            name,
            created_at: Utc::now(),
        })
        .await?;

    Ok(Json(CreateOrgResponse {
        org_id: org_id.to_string(),
    }))
}
