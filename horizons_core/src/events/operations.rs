use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use super::models::Event;
use super::webhook::WebhookSender;
use super::{Error, Result};
use crate::models::OrgId;
use crate::onboard::models::{OperationRunRecord, ResourceRecord};
use crate::onboard::secrets::CredentialManager;
use crate::onboard::traits::CentralDb;

/// Execute a managed operation against an incoming/outgoing event.
///
/// This is the glue that makes Event Sync "resource/operation aware" rather than
/// embedding raw URLs/headers in each subscription.
#[async_trait]
pub trait OperationExecutor: Send + Sync {
    async fn execute(
        &self,
        org_id: &str,
        operation_id: &str,
        environment: Option<&str>,
        event: &Event,
    ) -> Result<()>;
}

/// CentralDb-backed executor for a minimal HTTP "webhook event" operation.
///
/// Supported shapes (v0):
/// - Resource: `resource_type == "http"`
///   - config supports either:
///     - multi-env: `{ "default_env": "prod", "environments": { "prod": {"base_url":"...","headers":{...}} } }`
///     - single-env: `{ "base_url": "...", "headers": {...} }`
/// - Operation: `operation_type == "webhook_event_v1"`
///   - config: `{ "path": "/foo", "headers": {...}, "timeout_ms": 10000 }` OR `{ "url": "https://...", ... }`
///
/// Secrets should be referenced via `$cred:connector_id.key` tokens and resolved at runtime
/// when a `CredentialManager` is configured.
pub struct CentralDbOperationExecutor {
    central_db: Arc<dyn CentralDb>,
    credential_manager: Option<Arc<CredentialManager>>,
    webhook_sender: WebhookSender,
}

impl CentralDbOperationExecutor {
    pub fn new(
        central_db: Arc<dyn CentralDb>,
        credential_manager: Option<Arc<CredentialManager>>,
        webhook_sender: WebhookSender,
    ) -> Self {
        Self {
            central_db,
            credential_manager,
            webhook_sender,
        }
    }

    async fn resolve_json(
        &self,
        org_id: OrgId,
        v: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        match &self.credential_manager {
            None => Ok(v.clone()),
            Some(cm) => cm
                .resolve(org_id, v)
                .await
                .map_err(|e| Error::backend("resolve $cred tokens", e)),
        }
    }

    async fn get_resource(&self, org_id: OrgId, resource_id: &str) -> Result<ResourceRecord> {
        self.central_db
            .get_resource(org_id, resource_id)
            .await
            .map_err(|e| Error::backend("central_db.get_resource", e))?
            .ok_or_else(|| Error::message(format!("resource not found: {resource_id}")))
    }

    async fn record_run(
        &self,
        org_id: OrgId,
        operation_id: &str,
        source_event_id: Option<String>,
        status: &str,
        error: Option<String>,
        output: serde_json::Value,
    ) -> Result<()> {
        let run = OperationRunRecord {
            id: Uuid::new_v4(),
            org_id,
            operation_id: operation_id.to_string(),
            source_event_id,
            status: status.to_string(),
            error,
            output,
            created_at: Utc::now(),
        };
        self.central_db
            .append_operation_run(&run)
            .await
            .map_err(|e| Error::backend("central_db.append_operation_run", e))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
struct HttpEnvConfig {
    base_url: String,
    #[serde(default)]
    headers: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
struct HttpResourceConfigMultiEnv {
    #[serde(default)]
    default_env: Option<String>,
    environments: HashMap<String, HttpEnvConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct HttpResourceConfigSingleEnv {
    base_url: String,
    #[serde(default)]
    headers: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
struct WebhookEventOperationConfig {
    /// Optional absolute URL override. If set, resource base_url is ignored.
    #[serde(default)]
    url: Option<String>,
    /// Relative path appended to resource base_url when `url` is not set.
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

fn join_url(base_url: &str, path: &str) -> String {
    let b = base_url.trim_end_matches('/');
    let p = path.trim_start_matches('/');
    format!("{}/{}", b, p)
}

#[async_trait]
impl OperationExecutor for CentralDbOperationExecutor {
    async fn execute(
        &self,
        org_id: &str,
        operation_id: &str,
        environment: Option<&str>,
        event: &Event,
    ) -> Result<()> {
        // Parse org_id string -> OrgId UUID.
        let org_uuid = uuid::Uuid::parse_str(org_id)
            .map_err(|e| Error::message(format!("invalid org_id (expected uuid): {e}")))?;
        let org_id_t = OrgId(org_uuid);

        let op = self
            .central_db
            .get_operation(org_id_t, operation_id)
            .await
            .map_err(|e| Error::backend("central_db.get_operation", e))?
            .ok_or_else(|| Error::message(format!("operation not found: {operation_id}")))?;

        if op.operation_type != "webhook_event_v1" {
            return Err(Error::message(format!(
                "unsupported operation_type '{}' for operation '{}'",
                op.operation_type, op.operation_id
            )));
        }

        let res = self.get_resource(org_id_t, &op.resource_id).await?;
        if res.resource_type != "http" {
            return Err(Error::message(format!(
                "unsupported resource_type '{}' for resource '{}'",
                res.resource_type, res.resource_id
            )));
        }

        // Parse resource config.
        let (env_cfg, base_headers) =
            match serde_json::from_value::<HttpResourceConfigMultiEnv>(res.config.clone()) {
                Ok(multi) => {
                    let env = environment
                        .map(|s| s.to_string())
                        .or(multi.default_env.clone())
                        .unwrap_or_else(|| "prod".to_string());
                    let cfg = multi.environments.get(&env).cloned().ok_or_else(|| {
                        Error::message(format!(
                            "resource '{}' missing env '{}'",
                            res.resource_id, env
                        ))
                    })?;
                    let headers = cfg.headers.clone();
                    (cfg, headers)
                }
                Err(_) => {
                    let single: HttpResourceConfigSingleEnv =
                        serde_json::from_value(res.config.clone()).map_err(|e| {
                            Error::message(format!("invalid http resource config: {e}"))
                        })?;
                    let cfg = HttpEnvConfig {
                        base_url: single.base_url,
                        headers: single.headers.clone(),
                    };
                    (cfg, single.headers)
                }
            };

        let op_cfg: WebhookEventOperationConfig = serde_json::from_value(op.config.clone())
            .map_err(|e| Error::message(format!("invalid operation config: {e}")))?;

        // Merge headers: resource headers then op headers override.
        let mut merged_headers: HashMap<String, String> = base_headers;
        for (k, v) in op_cfg.headers {
            merged_headers.insert(k, v);
        }

        // Resolve any $cred tokens in headers.
        let resolved_headers_json = self
            .resolve_json(
                org_id_t,
                &serde_json::to_value(&merged_headers).unwrap_or_default(),
            )
            .await?;
        let resolved_headers: HashMap<String, String> =
            serde_json::from_value(resolved_headers_json)
                .map_err(|e| Error::message(format!("invalid resolved headers: {e}")))?;
        let headers_vec: Vec<(String, String)> = resolved_headers.into_iter().collect();

        // Resolve URL.
        let url = if let Some(u) = op_cfg.url.as_deref() {
            u.to_string()
        } else {
            let path = op_cfg.path.as_deref().ok_or_else(|| {
                Error::message("operation config requires url or path".to_string())
            })?;
            join_url(&env_cfg.base_url, path)
        };
        let url_json = self
            .resolve_json(org_id_t, &serde_json::Value::String(url))
            .await?;
        let url = url_json
            .as_str()
            .ok_or_else(|| Error::message("resolved url is not a string".to_string()))?
            .to_string();

        // Execute.
        let res = self
            .webhook_sender
            .send(&url, &headers_vec, op_cfg.timeout_ms, event)
            .await;

        match res {
            Ok(()) => {
                self.record_run(
                    org_id_t,
                    operation_id,
                    Some(event.id.clone()),
                    "succeeded",
                    None,
                    serde_json::json!({ "url": url, "delivered": true }),
                )
                .await
                .map_err(|e| Error::backend("central_db.append_operation_run", e))?;
                Ok(())
            }
            Err(e) => {
                let _ = self
                    .record_run(
                        org_id_t,
                        operation_id,
                        Some(event.id.clone()),
                        "failed",
                        Some(e.to_string()),
                        serde_json::json!({ "url": url, "delivered": false }),
                    )
                    .await;
                Err(e)
            }
        }
    }
}
