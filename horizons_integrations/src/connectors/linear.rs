//! Linear connector.
//!
//! Pulls issues from Linear GraphQL API.
//! Incremental sync uses an `updatedAt` cursor (ISO 8601 timestamp).

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use horizons_core::context_refresh::models::{ContextEntity, SourceConfig, SyncCursor};
use horizons_core::context_refresh::traits::{Connector, PullResult, RawRecord};
use horizons_core::models::OrgId;
use horizons_core::{Error, Result};
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use tracing::instrument;

#[derive(Debug, Deserialize)]
struct LinearResponse<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
struct IssuesData {
    issues: IssuesConnection,
}

#[derive(Debug, Deserialize)]
struct IssuesConnection {
    #[serde(default)]
    nodes: Vec<LinearIssue>,
}

#[derive(Debug, Deserialize, Clone)]
struct LinearIssue {
    id: String,
    identifier: String,
    title: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(rename = "updatedAt")]
    updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct LinearConnector {
    client: Client,
    api_base: String,
    api_key: String,
    team_id: Option<String>,
}

impl LinearConnector {
    pub fn new(api_key: impl Into<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .expect("reqwest client");
        Self {
            client,
            api_base: "https://api.linear.app/graphql".to_string(),
            api_key: api_key.into(),
            team_id: None,
        }
    }

    pub fn with_team_id(mut self, team_id: impl Into<String>) -> Self {
        self.team_id = Some(team_id.into());
        self
    }

    fn default_since() -> DateTime<Utc> {
        Utc::now() - ChronoDuration::days(30)
    }

    fn cursor_since(cursor: Option<SyncCursor>, source: &SourceConfig) -> Result<DateTime<Utc>> {
        if let Some(cursor) = cursor {
            let s = cursor.value.as_str().ok_or_else(|| {
                Error::InvalidInput("linear cursor must be ISO 8601 string".to_string())
            })?;
            let dt = DateTime::parse_from_rfc3339(s)
                .map_err(|e| Error::backend("parse linear cursor datetime", e))?
                .with_timezone(&Utc);
            return Ok(dt);
        }

        // Optional override in settings: {"since":"..."}
        if let Some(since) = source
            .settings
            .get("since")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
        {
            let dt = DateTime::parse_from_rfc3339(since)
                .map_err(|e| Error::backend("parse linear settings.since", e))?
                .with_timezone(&Utc);
            return Ok(dt);
        }

        Ok(Self::default_since())
    }
}

#[async_trait]
impl Connector for LinearConnector {
    #[instrument(level = "debug", skip(self))]
    async fn id(&self) -> &'static str {
        "linear"
    }

    #[instrument(level = "info", skip(self, source, cursor))]
    async fn pull(
        &self,
        _org_id: OrgId,
        source: &SourceConfig,
        cursor: Option<SyncCursor>,
    ) -> Result<PullResult> {
        let since = Self::cursor_since(cursor, source)?;
        let team_id = self.team_id.clone().or_else(|| {
            source
                .settings
                .get("team_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        });

        // Filter by updatedAt, optionally by team.
        const QUERY: &str = r#"
query Issues($since: DateTime!, $team_id: String) {
  issues(
    first: 100,
    filter: {
      updatedAt: { gt: $since },
      team: { id: { eq: $team_id } }
    }
  ) {
    nodes {
      id
      identifier
      title
      description
      updatedAt
    }
  }
}
"#;

        const QUERY_NO_TEAM: &str = r#"
query Issues($since: DateTime!) {
  issues(first: 100, filter: { updatedAt: { gt: $since } }) {
    nodes {
      id
      identifier
      title
      description
      updatedAt
    }
  }
}
"#;

        let since_str = since.to_rfc3339();
        let (query, variables) = if let Some(team_id) = team_id {
            (
                QUERY,
                serde_json::json!({
                    "since": since_str,
                    "team_id": team_id,
                }),
            )
        } else {
            (QUERY_NO_TEAM, serde_json::json!({ "since": since_str }))
        };

        let resp = self
            .client
            .post(&self.api_base)
            .bearer_auth(&self.api_key)
            .json(&serde_json::json!({
                "query": query,
                "variables": variables,
            }))
            .send()
            .await
            .map_err(Error::backend_reqwest)?;

        let v: serde_json::Value = resp.json().await.map_err(Error::backend_reqwest)?;
        let parsed: LinearResponse<IssuesData> =
            serde_json::from_value(v).map_err(|e| Error::backend("linear parse", e))?;

        let mut records = Vec::new();
        let mut max_updated = since;
        for iss in parsed.data.issues.nodes {
            if iss.updated_at > max_updated {
                max_updated = iss.updated_at;
            }
            let payload = serde_json::json!({
                "identifier": iss.identifier,
                "title": iss.title,
                "description": iss.description,
                "updated_at": iss.updated_at,
            });
            let dedupe_key = format!("issue:{}:{}", iss.id, iss.updated_at.timestamp_millis());
            records.push(RawRecord::new(iss.id.clone(), dedupe_key, payload, vec![])?);
        }

        let next_cursor = Some(SyncCursor::new(serde_json::Value::String(
            max_updated.to_rfc3339(),
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
            let entity = ContextEntity::new(
                org_id,
                Some(source.project_id),
                "issue",
                rec.payload.clone(),
                "linear",
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
