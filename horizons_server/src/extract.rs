use crate::error::ApiError;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use horizons_core::{AgentIdentity, OrgId, ProjectId};
use std::future;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Copy, Clone)]
pub struct AuthConfig {
    pub require_auth: bool,
    pub allow_insecure_headers: bool,
    /// If true, mutating requests to `/api/v1/*` require verified auth.
    pub require_auth_for_mutating: bool,
    /// Dev-only escape hatch: allow header-based identity for mutating routes.
    pub allow_insecure_mutating_requests: bool,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AuthMode {
    DevInsecure,
    DevStrict,
    Production,
}

impl AuthMode {
    fn from_env() -> Option<Self> {
        let v = std::env::var("HORIZONS_AUTH_MODE").ok()?;
        match v.trim().to_ascii_lowercase().as_str() {
            "dev_insecure" | "devinsecure" | "insecure" => Some(Self::DevInsecure),
            "dev_strict" | "devstrict" | "strict" => Some(Self::DevStrict),
            "production" | "prod" => Some(Self::Production),
            _ => None,
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        // Mode-driven defaults.
        let mut cfg = match AuthMode::from_env() {
            Some(AuthMode::DevInsecure) => Self {
                require_auth: false,
                allow_insecure_headers: true,
                require_auth_for_mutating: false,
                allow_insecure_mutating_requests: true,
            },
            Some(AuthMode::DevStrict) => Self {
                require_auth: false,
                allow_insecure_headers: true,
                require_auth_for_mutating: true,
                allow_insecure_mutating_requests: false,
            },
            Some(AuthMode::Production) => Self {
                require_auth: true,
                allow_insecure_headers: false,
                require_auth_for_mutating: true,
                allow_insecure_mutating_requests: false,
            },
            None => Self {
                // Canonical defaults (kept for backward compat).
                require_auth: false,
                allow_insecure_headers: true,
                require_auth_for_mutating: true,
                allow_insecure_mutating_requests: false,
            },
        };

        // Per-flag overrides (explicit env vars win over mode).
        if let Some(v) = env_bool("HORIZONS_REQUIRE_AUTH") {
            cfg.require_auth = v;
        }
        if let Some(v) = env_bool("HORIZONS_ALLOW_INSECURE_HEADERS") {
            cfg.allow_insecure_headers = v;
        }
        if let Some(v) = env_bool("HORIZONS_REQUIRE_AUTH_FOR_MUTATING") {
            cfg.require_auth_for_mutating = v;
        }
        if let Some(v) = env_bool("HORIZONS_ALLOW_INSECURE_MUTATING_REQUESTS") {
            cfg.allow_insecure_mutating_requests = v;
        }

        cfg
    }
}

/// Wrapper for injecting auth config into axum Extensions.
#[derive(Debug, Copy, Clone)]
pub struct AuthConfigExt(pub AuthConfig);

// ── Auth Provider ───────────────────────────────────────────────
//
// Pluggable authentication. Apps inject an `AuthProvider` into axum
// Extensions; the `AuthenticatedIdentity` extractor calls it to turn
// HTTP headers into a verified `(OrgId, AgentIdentity)` pair.
//
// If no `AuthProvider` is installed, the extractors fall back to the
// raw header-based behavior (useful for dev/tests).

/// Trait that apps implement to provide authentication.
///
/// The provider receives raw request headers and must return either:
/// - `Ok(Some((org, identity)))` — authenticated
/// - `Ok(None)` — no credentials present, fall through to header-based default
/// - `Err(...)` — credentials present but invalid → 401
#[async_trait::async_trait]
pub trait AuthProvider: Send + Sync {
    async fn authenticate(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<Option<(OrgId, AgentIdentity)>, ApiError>;
}

/// Wrapper for injecting an AuthProvider into axum state via Extensions.
#[derive(Clone)]
pub struct AuthProviderExt(pub Arc<dyn AuthProvider>);

/// Extractor that returns a verified `(OrgId, AgentIdentity)` pair.
///
/// Resolution order:
/// 1. If an `AuthProviderExt` is in Extensions, call it.
///    - `Ok(Some(pair))` → use it.
///    - `Err(e)` → reject with that error.
///    - `Ok(None)` → fall through.
/// 2. Fall back to raw `x-org-id` / `x-user-id` / `x-agent-id` headers.
#[derive(Debug, Clone)]
pub struct AuthenticatedIdentity {
    pub org_id: OrgId,
    pub identity: AgentIdentity,
}

#[derive(Debug, Clone)]
struct ResolvedAuthn(pub Option<(OrgId, AgentIdentity)>);

async fn resolve_authn(parts: &mut Parts) -> Result<Option<(OrgId, AgentIdentity)>, ApiError> {
    if let Some(cached) = parts.extensions.get::<ResolvedAuthn>().cloned() {
        return Ok(cached.0);
    }

    let cfg = parts
        .extensions
        .get::<AuthConfigExt>()
        .copied()
        .map(|c| c.0)
        .unwrap_or_default();

    let out = if let Some(provider) = parts.extensions.get::<AuthProviderExt>().cloned() {
        match provider.0.authenticate(&parts.headers).await {
            Ok(Some((org_id, identity))) => Some((org_id, identity)),
            Ok(None) => {
                if cfg.require_auth && !cfg.allow_insecure_headers {
                    return Err(ApiError::Core(horizons_core::Error::Unauthorized(
                        "missing credentials".to_string(),
                    )));
                }
                None
            }
            Err(e) => return Err(e),
        }
    } else {
        if cfg.require_auth && !cfg.allow_insecure_headers {
            return Err(ApiError::Core(horizons_core::Error::Unauthorized(
                "auth provider not configured".to_string(),
            )));
        }
        None
    };

    parts.extensions.insert(ResolvedAuthn(out.clone()));
    Ok(out)
}

impl<S> FromRequestParts<S> for AuthenticatedIdentity
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        async move {
            if let Some((org_id, identity)) = resolve_authn(parts).await? {
                return Ok(Self { org_id, identity });
            }

            // Strict: extract from raw headers.
            let raw_org = parts
                .headers
                .get("x-org-id")
                .ok_or(ApiError::MissingOrgId)?
                .to_str()
                .map_err(|e| ApiError::InvalidOrgId(e.to_string()))?;
            let org_id =
                OrgId::from_str(raw_org).map_err(|e| ApiError::InvalidOrgId(e.to_string()))?;

            let identity = extract_identity_from_headers(&parts.headers)?;

            Ok(Self { org_id, identity })
        }
    }
}

fn env_bool(key: &str) -> Option<bool> {
    let v = std::env::var(key).ok()?;
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

fn extract_identity_from_headers(
    headers: &axum::http::HeaderMap,
) -> Result<AgentIdentity, ApiError> {
    if let Some(user_id) = headers.get("x-user-id") {
        let user_id = user_id
            .to_str()
            .map_err(|e| ApiError::InvalidUserId(e.to_string()))?;
        let user_id =
            Uuid::parse_str(user_id).map_err(|e| ApiError::InvalidUserId(e.to_string()))?;
        let email = headers
            .get("x-user-email")
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());
        return Ok(AgentIdentity::User { user_id, email });
    }

    if let Some(agent_id) = headers.get("x-agent-id") {
        let agent_id = agent_id
            .to_str()
            .map_err(|e| ApiError::InvalidInput(format!("invalid x-agent-id: {e}")))?
            .to_string();
        if agent_id.trim().is_empty() {
            return Err(ApiError::InvalidInput("x-agent-id is empty".to_string()));
        }
        return Ok(AgentIdentity::Agent { agent_id });
    }

    Ok(AgentIdentity::System {
        name: "http".to_string(),
    })
}

// ── Canonical extractors (kept for backward compat) ────────────────

#[derive(Debug, Copy, Clone)]
pub struct OrgIdHeader(pub OrgId);

/// Optional org_id extractor.
///
/// Axum's `Option<T>` extractor support varies across versions; keep this as an explicit
/// extractor so routes can accept either verified auth org binding or no org header.
#[derive(Debug, Copy, Clone)]
pub struct MaybeOrgIdHeader(pub Option<OrgId>);

impl<S> FromRequestParts<S> for MaybeOrgIdHeader
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    #[tracing::instrument(level = "debug", name = "extract.maybe_org_id", skip_all)]
    fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        async move {
            match OrgIdHeader::from_request_parts(parts, state).await {
                Ok(OrgIdHeader(org_id)) => Ok(Self(Some(org_id))),
                Err(ApiError::MissingOrgId) => Ok(Self(None)),
                Err(e) => Err(e),
            }
        }
    }
}

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
        async move {
            if let Some((org_id, _)) = resolve_authn(parts).await? {
                return Ok(Self(org_id));
            }

            let raw = parts
                .headers
                .get("x-org-id")
                .ok_or(ApiError::MissingOrgId)?
                .to_str()
                .map_err(|e| ApiError::InvalidOrgId(e.to_string()))?;
            let org_id = OrgId::from_str(raw).map_err(|e| ApiError::InvalidOrgId(e.to_string()))?;
            Ok(Self(org_id))
        }
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
        async move {
            if let Some((_, identity)) = resolve_authn(parts).await? {
                return Ok(Self(identity));
            }
            extract_identity_from_headers(&parts.headers).map(Self)
        }
    }
}
