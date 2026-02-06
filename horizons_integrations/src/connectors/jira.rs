//! Jira connector.
//!
//! Pulls issues from Jira's REST API and normalizes them as ContextEntity records.
//! Authentication uses basic auth with an API token. The connector only depends on
//! the generic Context Refresh traits and keeps payloads opaque for applications to interpret.
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

#[derive(Debug, Deserialize)]
struct JiraSearchResponse {
    issues: Vec<JiraIssue>,
}

#[derive(Debug, Deserialize, Clone)]
struct JiraIssue {
    id: String,
    key: String,
    #[serde(default)]
    fields: serde_json::Value,
}

#[derive(Clone)]
pub struct JiraConnector {
    client: Client,
    api_base: String,
    username: String,
    token: String,
    jql: String,
}

impl JiraConnector {
    pub fn new(
        api_base: impl Into<String>,
        username: impl Into<String>,
        token: impl Into<String>,
        jql: impl Into<String>,
    ) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .expect("reqwest client");
        Self {
            client,
            api_base: api_base.into().trim_end_matches('/').to_string(),
            username: username.into(),
            token: token.into(),
            jql: jql.into(),
        }
    }
}

#[async_trait]
impl Connector for JiraConnector {
    #[instrument(level = "debug", skip(self))]
    async fn id(&self) -> &'static str {
        "jira"
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
            .get(format!("{}/rest/api/3/search", self.api_base))
            .basic_auth(&self.username, Some(&self.token))
            .query(&[("jql", self.jql.as_str())])
            .query(&[
                ("fields", "summary,description,status,updated"),
                ("maxResults", "50"),
            ]);
        if let Some(c) = cursor {
            if let Some(start_at) = c.value.as_i64() {
                req = req.query(&[("startAt", start_at.to_string())]);
            }
        }
        let resp = req
            .send()
            .await
            .map_err(horizons_core::Error::backend_reqwest)?;
        let body: JiraSearchResponse = resp
            .json()
            .await
            .map_err(horizons_core::Error::backend_reqwest)?;

        let mut records = Vec::new();
        for issue in body.issues {
            let payload = serde_json::json!({
                "key": issue.key,
                "fields": issue.fields,
            });
            let record = RawRecord::new(issue.id.clone(), issue.key.clone(), payload, vec![])?;
            records.push(record);
        }

        Ok(PullResult {
            records,
            // For simplicity we do not paginate further; Jira cursor value is startAt offset.
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
                "jira_issue",
                rec.payload.clone(),
                "jira",
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
