use crate::error::ApiError;
use crate::extract::AuthProvider;
use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use chrono::Utc;
use horizons_core::onboard::traits::CentralDb;
use horizons_core::{AgentIdentity, Error as CoreError, OrgId};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

/// Auth provider that validates `Authorization: Bearer ...` tokens against the CentralDb.
///
/// Supported token formats:
/// - `hzn_<uuid>.<secret>` (recommended)
/// - `hzn_<uuid>:<secret>` (accepted)
///
/// Dev-only escape hatch:
/// - if `HORIZONS_DEV_API_KEY` matches the bearer token and `HORIZONS_DEV_ORG_ID` is set,
///   this authenticates as that org with a `System` actor.
#[derive(Clone)]
pub struct CentralDbApiKeyAuth {
    central_db: Arc<dyn CentralDb>,
}

impl CentralDbApiKeyAuth {
    pub fn new(central_db: Arc<dyn CentralDb>) -> Self {
        Self { central_db }
    }
}

#[async_trait::async_trait]
impl AuthProvider for CentralDbApiKeyAuth {
    async fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<(OrgId, AgentIdentity)>, ApiError> {
        let Some(authz) = headers.get(AUTHORIZATION) else {
            return Ok(None);
        };
        let authz = authz.to_str().map_err(|_| {
            ApiError::Core(CoreError::Unauthorized(
                "invalid authorization header".into(),
            ))
        })?;

        let token = authz
            .strip_prefix("Bearer ")
            .or_else(|| authz.strip_prefix("bearer "))
            .ok_or_else(|| {
                ApiError::Core(CoreError::Unauthorized(
                    "unsupported authorization scheme".into(),
                ))
            })?
            .trim();

        // Dev escape hatch (explicit org binding).
        if let Ok(dev_key) = std::env::var("HORIZONS_DEV_API_KEY") {
            if token == dev_key {
                let org = std::env::var("HORIZONS_DEV_ORG_ID").map_err(|_| {
                    ApiError::Core(CoreError::Unauthorized(
                        "HORIZONS_DEV_ORG_ID must be set when using HORIZONS_DEV_API_KEY".into(),
                    ))
                })?;
                let org_id = OrgId(Uuid::parse_str(org.trim()).map_err(|e| {
                    ApiError::Core(CoreError::Unauthorized(format!(
                        "invalid HORIZONS_DEV_ORG_ID: {e}"
                    )))
                })?);
                return Ok(Some((
                    org_id,
                    AgentIdentity::System {
                        name: "dev_api_key".to_string(),
                    },
                )));
            }
        }

        let (key_id, secret) = parse_api_key_token(token).ok_or_else(|| {
            ApiError::Core(CoreError::Unauthorized("invalid api key format".into()))
        })?;

        let Some(rec) = self
            .central_db
            .get_api_key_by_id(key_id)
            .await
            .map_err(ApiError::Core)?
        else {
            return Err(ApiError::Core(CoreError::Unauthorized(
                "invalid api key".into(),
            )));
        };

        if let Some(exp) = rec.expires_at {
            if exp < Utc::now() {
                return Err(ApiError::Core(CoreError::Unauthorized(
                    "api key expired".into(),
                )));
            }
        }

        let secret_hash = sha256_hex(secret.as_bytes());
        if secret_hash != rec.secret_hash {
            return Err(ApiError::Core(CoreError::Unauthorized(
                "invalid api key".into(),
            )));
        }

        // Best-effort usage tracking.
        let _ = self
            .central_db
            .touch_api_key_last_used(key_id, Utc::now())
            .await;

        Ok(Some((rec.org_id, rec.actor)))
    }
}

fn parse_api_key_token(token: &str) -> Option<(Uuid, String)> {
    let t = token.trim();
    let t = t.strip_prefix("hzn_").unwrap_or(t);
    let (id_str, secret) = t.split_once('.').or_else(|| t.split_once(':'))?;
    let key_id = Uuid::parse_str(id_str.trim()).ok()?;
    let secret = secret.trim();
    if secret.is_empty() {
        return None;
    }
    Some((key_id, secret.to_string()))
}

pub fn format_api_key_token(key_id: Uuid, secret: &str) -> String {
    format!("hzn_{}.{}", key_id, secret)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut s = String::with_capacity(out.len() * 2);
    for b in out {
        use std::fmt::Write as _;
        let _ = write!(&mut s, "{:02x}", b);
    }
    s
}
