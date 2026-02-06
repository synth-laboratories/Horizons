//! HubSpot connector.
//!
//! Pulls contacts + companies from HubSpot CRM API v3.
//! Incremental sync uses the `lastmodifieddate` property cursor (milliseconds since epoch).

use async_trait::async_trait;
use horizons_core::context_refresh::models::{ContextEntity, SourceConfig, SyncCursor};
use horizons_core::context_refresh::traits::{Connector, PullResult, RawRecord};
use horizons_core::models::OrgId;
use horizons_core::{Error, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::instrument;

#[derive(Debug, Deserialize)]
struct HubspotSearchResponse {
    #[serde(default)]
    results: Vec<HubspotObject>,
}

#[derive(Debug, Deserialize, Clone)]
struct HubspotObject {
    id: String,
    #[serde(default)]
    properties: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct HubspotSearchRequest {
    #[serde(rename = "filterGroups")]
    filter_groups: Vec<HubspotFilterGroup>,
    #[serde(default)]
    properties: Vec<String>,
    limit: u32,
}

#[derive(Debug, Serialize)]
struct HubspotFilterGroup {
    filters: Vec<HubspotFilter>,
}

#[derive(Debug, Serialize)]
struct HubspotFilter {
    #[serde(rename = "propertyName")]
    property_name: String,
    operator: String,
    value: String,
}

#[derive(Clone)]
pub struct HubSpotConnector {
    client: Client,
    api_base: String,
    api_key: Option<String>,
    token: Option<String>,
}

impl HubSpotConnector {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .expect("reqwest client");
        Self {
            client,
            api_base: "https://api.hubapi.com".to_string(),
            api_key: None,
            token: None,
        }
    }

    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    fn cursor_ms(cursor: Option<SyncCursor>) -> Result<i64> {
        let Some(cursor) = cursor else {
            return Ok(0);
        };
        if let Some(n) = cursor.value.as_i64() {
            return Ok(n.max(0));
        }
        if let Some(s) = cursor.value.as_str() {
            let n = s
                .parse::<i64>()
                .map_err(|e| Error::backend("parse hubspot cursor ms", e))?;
            return Ok(n.max(0));
        }
        Err(Error::InvalidInput(
            "hubspot cursor must be ms epoch (string or int)".to_string(),
        ))
    }

    fn lastmodified_ms(props: &serde_json::Value) -> Option<i64> {
        props
            .get("lastmodifieddate")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<i64>().ok())
    }

    async fn search_objects(
        &self,
        object: &str,
        modified_gt_ms: i64,
        properties: &[&str],
    ) -> Result<Vec<HubspotObject>> {
        let url = format!("{}/crm/v3/objects/{}/search", self.api_base, object);
        let req_body = HubspotSearchRequest {
            filter_groups: vec![HubspotFilterGroup {
                filters: vec![HubspotFilter {
                    property_name: "lastmodifieddate".to_string(),
                    operator: "GT".to_string(),
                    value: modified_gt_ms.to_string(),
                }],
            }],
            properties: properties.iter().map(|s| s.to_string()).collect(),
            limit: 100,
        };

        let mut req = self.client.post(url).json(&req_body);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        } else if let Some(key) = &self.api_key {
            req = req.query(&[("hapikey", key.as_str())]);
        } else {
            return Err(Error::InvalidInput(
                "hubspot auth missing (set api_key or token)".to_string(),
            ));
        }

        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        let body: HubspotSearchResponse = resp.json().await.map_err(Error::backend_reqwest)?;
        Ok(body.results)
    }
}

#[async_trait]
impl Connector for HubSpotConnector {
    #[instrument(level = "debug", skip(self))]
    async fn id(&self) -> &'static str {
        "hubspot"
    }

    #[instrument(level = "info", skip(self, _source, cursor))]
    async fn pull(
        &self,
        _org_id: OrgId,
        _source: &SourceConfig,
        cursor: Option<SyncCursor>,
    ) -> Result<PullResult> {
        let since_ms = Self::cursor_ms(cursor)?;

        let contacts = self
            .search_objects(
                "contacts",
                since_ms,
                &["firstname", "lastname", "email", "lastmodifieddate"],
            )
            .await?;
        let companies = self
            .search_objects(
                "companies",
                since_ms,
                &["name", "domain", "lastmodifieddate"],
            )
            .await?;

        let mut records = Vec::new();
        let mut max_ms = since_ms;

        for c in contacts {
            let obj_ms = Self::lastmodified_ms(&c.properties).unwrap_or(since_ms);
            if obj_ms > max_ms {
                max_ms = obj_ms;
            }
            let payload = serde_json::json!({
                "object_type": "contact",
                "properties": c.properties,
            });
            let dedupe_key = format!("contact:{}:{}", c.id, obj_ms);
            records.push(RawRecord::new(c.id.clone(), dedupe_key, payload, vec![])?);
        }

        for c in companies {
            let obj_ms = Self::lastmodified_ms(&c.properties).unwrap_or(since_ms);
            if obj_ms > max_ms {
                max_ms = obj_ms;
            }
            let payload = serde_json::json!({
                "object_type": "company",
                "properties": c.properties,
            });
            let dedupe_key = format!("company:{}:{}", c.id, obj_ms);
            records.push(RawRecord::new(c.id.clone(), dedupe_key, payload, vec![])?);
        }

        let next_cursor = Some(SyncCursor::new(serde_json::Value::Number(
            serde_json::Number::from(max_ms),
        )));

        Ok(PullResult {
            records,
            next_cursor,
        })
    }

    #[instrument(level = "debug", skip(self, records))]
    async fn process(
        &self,
        org_id: OrgId,
        source: &SourceConfig,
        records: Vec<RawRecord>,
    ) -> Result<Vec<ContextEntity>> {
        let mut out = Vec::with_capacity(records.len());
        for rec in records {
            let entity_type = rec
                .payload
                .get("object_type")
                .and_then(|v| v.as_str())
                .unwrap_or("contact");

            let entity = ContextEntity::new(
                org_id,
                Some(source.project_id),
                entity_type,
                rec.payload.clone(),
                "hubspot",
                rec.source_id.clone(),
                rec.dedupe_key.clone(),
                rec.acl.clone(),
                None,
            )?;
            out.push(entity);
        }
        Ok(out)
    }
}
