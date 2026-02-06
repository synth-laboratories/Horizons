use crate::models::{OrgId, ProjectId};
use crate::onboard::config::HelixConfig;
use crate::onboard::traits::GraphStore;
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::Utc;
use reqwest::header::{HeaderMap, HeaderValue};

#[derive(Clone)]
pub struct HelixGraphStore {
    base_url: String,
    client: reqwest::Client,
    api_key: Option<String>,
}

impl HelixGraphStore {
    #[tracing::instrument(level = "debug", skip(cfg))]
    pub fn new(cfg: &HelixConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .build()
            .map_err(|e| Error::backend("build helix http client", e))?;

        Ok(Self {
            base_url: cfg.url.trim_end_matches('/').to_string(),
            client,
            api_key: cfg.api_key.clone(),
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        if let Some(key) = &self.api_key {
            headers.insert(
                "x-api-key",
                HeaderValue::from_str(key)
                    .map_err(|e| Error::InvalidInput(format!("invalid helix api key: {e}")))?,
            );
        }
        Ok(headers)
    }

    #[tracing::instrument(level = "debug", skip(self, body))]
    async fn post(&self, endpoint: &str, body: serde_json::Value) -> Result<serde_json::Value> {
        let url = format!("{}/{}", self.base_url, endpoint.trim_start_matches('/'));
        let resp = self
            .client
            .post(url)
            .headers(self.headers()?)
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::backend("helix request", e))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(Error::BackendMessage(format!(
                "helix returned {status}: {text}"
            )));
        }

        resp.json()
            .await
            .map_err(|e| Error::backend("parse helix json", e))
    }

    #[tracing::instrument(level = "debug", skip(params))]
    fn inject_org_id(org_id: OrgId, params: serde_json::Value) -> serde_json::Value {
        match params {
            serde_json::Value::Object(mut obj) => {
                obj.entry("org_id".to_string())
                    .or_insert_with(|| serde_json::Value::String(org_id.to_string()));
                serde_json::Value::Object(obj)
            }
            other => serde_json::json!({
                "org_id": org_id.to_string(),
                "params": other,
            }),
        }
    }
}

#[async_trait]
impl GraphStore for HelixGraphStore {
    #[tracing::instrument(level = "debug", skip(self, params))]
    async fn query(
        &self,
        org_id: OrgId,
        _project_id: Option<ProjectId>,
        cypher: &str,
        params: serde_json::Value,
    ) -> Result<Vec<serde_json::Value>> {
        // Helix's API is a POST-only RPC surface keyed by endpoint name.
        // The `cypher` parameter is treated as the endpoint name.
        let body = Self::inject_org_id(org_id, params);
        let value = self.post(cypher, body).await?;

        match value {
            serde_json::Value::Array(items) => Ok(items),
            other => Ok(vec![other]),
        }
    }

    #[tracing::instrument(level = "debug", skip(self, identity, props))]
    async fn upsert_node(
        &self,
        org_id: OrgId,
        _project_id: Option<ProjectId>,
        label: &str,
        identity: serde_json::Value,
        props: serde_json::Value,
    ) -> Result<String> {
        let name = identity
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::InvalidInput(
                    "helix upsert_node identity must include string field 'name'".into(),
                )
            })?
            .to_string();

        let description = props
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let relevance = props
            .get("relevance")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let visibility = props
            .get("visibility")
            .and_then(|v| v.as_str())
            .unwrap_or("public")
            .to_string();
        let created_at = props
            .get("created_at")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| Utc::now().timestamp());

        let body = serde_json::json!({
            "org_id": org_id.to_string(),
            "name": name,
            "node_type": label,
            "description": description,
            "relevance": relevance,
            "created_at": created_at,
            "visibility": visibility,
        });

        let resp = self.post("upsertNode", body).await?;
        Ok(resp
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| resp.get("name").and_then(|v| v.as_str()).unwrap_or(""))
            .to_string())
    }

    #[tracing::instrument(level = "debug", skip(self, props))]
    async fn upsert_edge(
        &self,
        org_id: OrgId,
        _project_id: Option<ProjectId>,
        from: &str,
        to: &str,
        rel: &str,
        props: serde_json::Value,
    ) -> Result<()> {
        let value = props
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let confidence = props
            .get("confidence")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);
        let status = props
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("active")
            .to_string();
        let visibility = props
            .get("visibility")
            .and_then(|v| v.as_str())
            .unwrap_or("public")
            .to_string();
        let created_at = props
            .get("created_at")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| Utc::now().timestamp());

        let body = serde_json::json!({
            "org_id": org_id.to_string(),
            "from_id": from,
            "to_id": to,
            "relation_type": rel,
            "value": value,
            "confidence": confidence,
            "status": status,
            "visibility": visibility,
            "created_at": created_at,
        });

        let _ = self.post("addRelationship", body).await?;
        Ok(())
    }
}
