use crate::error::ApiError;
use crate::extract::{MaybeOrgIdHeader, OrgIdHeader};
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::routing::post;
use horizons_core::models::{ProjectDbHandle, ProjectId};
use horizons_core::models::OrgId;
use horizons_core::onboard::traits::{ListQuery, ProjectRecord};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct CreateProjectRequest {
    /// Optional override for provisioning a project in a specific org.
    ///
    /// This is primarily intended for consumer-facing provisioning flows (e.g. Vistas)
    /// where the caller first creates an org and then creates a project under it.
    #[serde(default)]
    pub org_id: Option<String>,
    pub project_id: Option<Uuid>,
    /// Optional human-friendly project identifier.
    ///
    /// If omitted, defaults to the UUID string.
    #[serde(default)]
    pub slug: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CreateProjectResponse {
    pub project_id: String,
    pub slug: String,
    pub handle: ProjectDbHandle,
}

#[derive(Debug, Deserialize)]
pub struct ListProjectsQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn router() -> axum::Router {
    axum::Router::new().route("/projects", post(create_project).get(list_projects))
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
        .and_then(|s| s.strip_prefix("Bearer ").or_else(|| s.strip_prefix("bearer ")))
        .map(|s| s.trim())
    {
        return got == expected;
    }

    false
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn create_project(
    MaybeOrgIdHeader(org_id_h): MaybeOrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateProjectRequest>,
) -> Result<Json<CreateProjectResponse>, ApiError> {
    let org_id: OrgId = match (org_id_h, req.org_id.as_deref()) {
        (Some(org_id), _) => org_id,
        (None, Some(s)) => {
            if !admin_token_ok(&headers) {
                return Err(ApiError::Core(horizons_core::Error::Unauthorized(
                    "admin token required".to_string(),
                )));
            }
            OrgId::from_str(s.trim()).map_err(|e| ApiError::InvalidOrgId(e.to_string()))?
        }
        (None, None) => return Err(ApiError::MissingOrgId),
    };

    let project_id = ProjectId(req.project_id.unwrap_or_else(Uuid::new_v4));
    let handle = state.project_db.provision(org_id, project_id).await?;

    let slug = req
        .slug
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| project_id.to_string());

    // Persist slug mapping in the CentralDb so consumer-facing APIs can resolve `project=<slug>`.
    state
        .central_db
        .upsert_project(&ProjectRecord {
            org_id,
            project_id,
            slug: slug.clone(),
            created_at: chrono::Utc::now(),
        })
        .await?;

    Ok(Json(CreateProjectResponse {
        project_id: project_id.to_string(),
        slug,
        handle,
    }))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn list_projects(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<ListProjectsQuery>,
) -> Result<Json<Vec<ProjectDbHandle>>, ApiError> {
    let query = ListQuery {
        limit: q.limit.unwrap_or(100),
        offset: q.offset.unwrap_or(0),
    };
    let projects = state.project_db.list_projects(org_id, query).await?;
    Ok(Json(projects))
}
