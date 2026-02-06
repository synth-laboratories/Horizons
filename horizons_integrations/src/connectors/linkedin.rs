//! LinkedIn connector.
//!
//! Implements the Context Refresh `Connector` trait by pulling organization updates from LinkedIn's REST API.
//! The connector is intentionally minimal and general-purpose: it ingests activity events and normalizes them
//! into `ContextEntity` records for later retrieval by agents.
//!
//! See: specifications/horizons/specs/onboard_business_logic.md

use async_trait::async_trait;
use horizons_core::Result;
use horizons_core::context_refresh::models::ContextEntity;
use horizons_core::context_refresh::traits::{Connector, PullResult, RawRecord};
use horizons_core::models::{OrgId, ProjectId};
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use tracing::instrument;

#[derive(Debug, Clone, Deserialize)]
struct LinkedinEvent {
    id: String,
    actor: String,
    text: Option<String>,
    #[serde(default)]
    visibility: Vec<String>,
}

/// Pulls activity events from LinkedIn.
#[derive(Clone)]
pub struct LinkedInConnector {
    client: Client,
    api_base: String,
    token: String,
}

impl LinkedInConnector {
    pub fn new(api_base: impl Into<String>, token: impl Into<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .expect("reqwest client");
        Self {
            client,
            api_base: api_base.into().trim_end_matches('/').to_string(),
            token: token.into(),
        }
    }
}

#[async_trait]
impl Connector for LinkedInConnector {
    #[instrument(level = "debug", skip(self))]
    async fn id(&self) -> &'static str {
        "linkedin"
    }

    #[instrument(level = "info", skip(self, source, cursor))]
    async fn pull(
        &self,
        _org_id: OrgId,
        source: &horizons_core::context_refresh::models::SourceConfig,
        cursor: Option<horizons_core::context_refresh::models::SyncCursor>,
    ) -> Result<PullResult> {
        let mut req = self
            .client
            .get(format!("{}/v2/events", self.api_base))
            .bearer_auth(&self.token)
            .query(&[("source", source.source_id.as_str())]);
        if let Some(cur) = cursor {
            req = req.query(&[("cursor", cur.value)]);
        }

        let resp = req
            .send()
            .await
            .map_err(horizons_core::Error::backend_reqwest)?;
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(horizons_core::Error::backend_reqwest)?;
        let events: Vec<LinkedinEvent> = serde_json::from_value(body["elements"].clone())
            .map_err(|e| horizons_core::Error::backend("linkedin parse", e))?;

        let records = events
            .into_iter()
            .map(|evt| {
                let payload = serde_json::json!({
                    "actor": evt.actor,
                    "text": evt.text,
                });
                RawRecord::new(evt.id.clone(), evt.id, payload, evt.visibility.clone())
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(PullResult {
            records,
            next_cursor: None,
        })
    }

    #[instrument(level = "debug", skip(self, records))]
    async fn process(
        &self,
        org_id: OrgId,
        source: &horizons_core::context_refresh::models::SourceConfig,
        records: Vec<RawRecord>,
    ) -> Result<Vec<ContextEntity>> {
        let mut out = Vec::with_capacity(records.len());
        for rec in records {
            let entity = ContextEntity::new(
                org_id,
                Some(source.project_id),
                "linkedin_event",
                rec.payload.clone(),
                "linkedin",
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
