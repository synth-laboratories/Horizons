use crate::error::ApiError;
use crate::extract::{AuthConfigExt, AuthProviderExt};
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::{body::Body, http::Request};
use horizons_core::Error as CoreError;

fn is_mutating(method: &axum::http::Method) -> bool {
    matches!(
        *method,
        axum::http::Method::POST
            | axum::http::Method::PUT
            | axum::http::Method::PATCH
            | axum::http::Method::DELETE
    )
}

/// Guard mutating requests by requiring verified auth when configured.
///
/// This is intended to be applied to the `/api/v1` router so that:
/// - GET/HEAD/OPTIONS are unaffected
/// - inbound webhooks (mounted outside `/api/v1`) remain public
pub async fn require_auth_for_mutating(req: Request<Body>, next: Next) -> Response {
    if !is_mutating(req.method()) {
        return next.run(req).await;
    }

    let cfg = req
        .extensions()
        .get::<AuthConfigExt>()
        .copied()
        .map(|c| c.0)
        .unwrap_or_default();

    if !cfg.require_auth_for_mutating {
        return next.run(req).await;
    }

    // Authenticated path: require a configured provider and valid credentials.
    if let Some(provider) = req.extensions().get::<AuthProviderExt>() {
        match provider.0.authenticate(req.headers()).await {
            Ok(Some(_pair)) => return next.run(req).await,
            Ok(None) => {
                // Explicit escape hatch (dev-only usage): allow header-based identities for mutating
                // routes only if the caller opts into it.
                if cfg.allow_insecure_headers && cfg.allow_insecure_mutating_requests {
                    return next.run(req).await;
                }
                return ApiError::Core(CoreError::Unauthorized("missing credentials".to_string()))
                    .into_response();
            }
            Err(e) => return e.into_response(),
        }
    }

    if cfg.allow_insecure_headers && cfg.allow_insecure_mutating_requests {
        return next.run(req).await;
    }

    ApiError::Core(CoreError::Unauthorized("auth provider not configured".to_string()))
        .into_response()
}
