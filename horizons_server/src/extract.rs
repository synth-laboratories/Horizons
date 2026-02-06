use crate::error::ApiError;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use horizons_core::{AgentIdentity, OrgId, ProjectId};
use std::future;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Copy, Clone)]
pub struct OrgIdHeader(pub OrgId);

impl<S> FromRequestParts<S> for OrgIdHeader
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    #[tracing::instrument(level = "debug", name = "extract.org_id", skip_all)]
    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        let res = (|| {
            let raw = parts
                .headers
                .get("x-org-id")
                .ok_or(ApiError::MissingOrgId)?
                .to_str()
                .map_err(|e| ApiError::InvalidOrgId(e.to_string()))?;
            let org_id = OrgId::from_str(raw).map_err(|e| ApiError::InvalidOrgId(e.to_string()))?;
            Ok(Self(org_id))
        })();
        future::ready(res)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ProjectIdHeader(pub Option<ProjectId>);

impl<S> FromRequestParts<S> for ProjectIdHeader
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    #[tracing::instrument(level = "debug", name = "extract.project_id", skip_all)]
    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        let res = (|| {
            let Some(raw) = parts.headers.get("x-project-id") else {
                return Ok(Self(None));
            };
            let raw = raw
                .to_str()
                .map_err(|e| ApiError::InvalidProjectId(e.to_string()))?;
            let project_id =
                ProjectId::from_str(raw).map_err(|e| ApiError::InvalidProjectId(e.to_string()))?;
            Ok(Self(Some(project_id)))
        })();
        future::ready(res)
    }
}

#[derive(Debug, Clone)]
pub struct IdentityHeader(pub AgentIdentity);

impl<S> FromRequestParts<S> for IdentityHeader
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    #[tracing::instrument(level = "debug", name = "extract.identity", skip_all)]
    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        let res = (|| {
            if let Some(user_id) = parts.headers.get("x-user-id") {
                let user_id = user_id
                    .to_str()
                    .map_err(|e| ApiError::InvalidUserId(e.to_string()))?;
                let user_id =
                    Uuid::parse_str(user_id).map_err(|e| ApiError::InvalidUserId(e.to_string()))?;
                let email = parts
                    .headers
                    .get("x-user-email")
                    .and_then(|h| h.to_str().ok())
                    .map(|s| s.to_string());
                return Ok(Self(AgentIdentity::User { user_id, email }));
            }

            if let Some(agent_id) = parts.headers.get("x-agent-id") {
                let agent_id = agent_id
                    .to_str()
                    .map_err(|e| ApiError::InvalidInput(format!("invalid x-agent-id: {e}")))?
                    .to_string();
                if agent_id.trim().is_empty() {
                    return Err(ApiError::InvalidInput("x-agent-id is empty".to_string()));
                }
                return Ok(Self(AgentIdentity::Agent { agent_id }));
            }

            Ok(Self(AgentIdentity::System {
                name: "http".to_string(),
            }))
        })();
        future::ready(res)
    }
}
