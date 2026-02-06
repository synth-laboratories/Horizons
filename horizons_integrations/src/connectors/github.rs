//! GitHub connector.
//!
//! Pulls issues and pull requests from GitHub REST API v3.
//! Incremental sync uses an ISO 8601 timestamp cursor (`since` / `updated_at`).

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use horizons_core::context_refresh::models::{ContextEntity, SourceConfig, SyncCursor};
use horizons_core::context_refresh::traits::{Connector, PullResult, RawRecord};
use horizons_core::models::OrgId;
use horizons_core::{Error, Result};
use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;
use std::time::Duration;
use tracing::instrument;

#[derive(Debug, Deserialize, Clone)]
struct GithubIssueLike {
    id: u64,
    number: u64,
    title: String,
    #[serde(default)]
    body: Option<String>,
    #[serde(rename = "updated_at")]
    updated_at: DateTime<Utc>,
    #[serde(default)]
    pull_request: Option<serde_json::Value>,
    #[serde(default)]
    html_url: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct GithubPull {
    id: u64,
    number: u64,
    title: String,
    #[serde(default)]
    body: Option<String>,
    #[serde(rename = "updated_at")]
    updated_at: DateTime<Utc>,
    #[serde(default)]
    html_url: Option<String>,
}

#[derive(Clone)]
pub struct GithubConnector {
    client: Client,
    api_base: String,
    token: String,
    owner: String,
    repo: String,
}

impl GithubConnector {
    pub fn new(
        token: impl Into<String>,
        owner: impl Into<String>,
        repo: impl Into<String>,
    ) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .expect("reqwest client");

        Self {
            client,
            api_base: "https://api.github.com".to_string(),
            token: token.into(),
            owner: owner.into(),
            repo: repo.into(),
        }
    }

    fn default_since() -> DateTime<Utc> {
        Utc::now() - ChronoDuration::days(30)
    }

    fn cursor_since(cursor: Option<SyncCursor>, source: &SourceConfig) -> Result<DateTime<Utc>> {
        if let Some(cursor) = cursor {
            let s = cursor.value.as_str().ok_or_else(|| {
                Error::InvalidInput("github cursor must be ISO 8601 string".to_string())
            })?;
            let dt = DateTime::parse_from_rfc3339(s)
                .map_err(|e| Error::backend("parse github cursor datetime", e))?
                .with_timezone(&Utc);
            return Ok(dt);
        }

        // Optional `since` override in settings: {"since":"2026-01-01T00:00:00Z"}
        if let Some(since) = source
            .settings
            .get("since")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
        {
            let dt = DateTime::parse_from_rfc3339(since)
                .map_err(|e| Error::backend("parse github settings.since", e))?
                .with_timezone(&Utc);
            return Ok(dt);
        }

        Ok(Self::default_since())
    }

    fn headers(&self) -> Result<HeaderMap> {
        let mut h = HeaderMap::new();
        h.insert(USER_AGENT, HeaderValue::from_static("horizons"));
        h.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.github+json"),
        );
        let auth = format!("Bearer {}", self.token);
        h.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&auth)
                .map_err(|e| Error::backend("invalid github auth header", e))?,
        );
        Ok(h)
    }
}

#[async_trait]
impl Connector for GithubConnector {
    #[instrument(level = "debug", skip(self))]
    async fn id(&self) -> &'static str {
        "github"
    }

    #[instrument(level = "info", skip(self, source, cursor))]
    async fn pull(
        &self,
        _org_id: OrgId,
        source: &SourceConfig,
        cursor: Option<SyncCursor>,
    ) -> Result<PullResult> {
        let since = Self::cursor_since(cursor, source)?;
        let since_str = since.to_rfc3339();
        let headers = self.headers()?;

        let issues_url = format!(
            "{}/repos/{}/{}/issues",
            self.api_base, self.owner, self.repo
        );
        let pulls_url = format!("{}/repos/{}/{}/pulls", self.api_base, self.owner, self.repo);

        // Issues endpoint includes PRs too; we keep them separate and filter by presence of pull_request.
        let issues_resp = self
            .client
            .get(&issues_url)
            .headers(headers.clone())
            .query(&[
                ("state", "all"),
                ("since", since_str.as_str()),
                ("per_page", "100"),
            ])
            .send()
            .await
            .map_err(Error::backend_reqwest)?;
        let issues: Vec<GithubIssueLike> =
            issues_resp.json().await.map_err(Error::backend_reqwest)?;

        // Pull requests endpoint doesn't support `since`; use updated sorting and filter client-side.
        let pulls_resp = self
            .client
            .get(&pulls_url)
            .headers(headers)
            .query(&[
                ("state", "all"),
                ("sort", "updated"),
                ("direction", "desc"),
                ("per_page", "100"),
            ])
            .send()
            .await
            .map_err(Error::backend_reqwest)?;
        let pulls: Vec<GithubPull> = pulls_resp.json().await.map_err(Error::backend_reqwest)?;

        let mut records = Vec::new();
        let mut max_updated = since;

        for i in issues {
            // Skip PRs in issues list; we'll emit PRs from /pulls.
            if i.pull_request.is_some() {
                continue;
            }
            if i.updated_at > max_updated {
                max_updated = i.updated_at;
            }
            let payload = serde_json::json!({
                "repo": format!("{}/{}", self.owner, self.repo),
                "number": i.number,
                "title": i.title,
                "body": i.body,
                "updated_at": i.updated_at,
                "html_url": i.html_url,
            });
            let dedupe_key = format!("issue:{}:{}", i.id, i.updated_at.timestamp_millis());
            records.push(RawRecord::new(
                i.id.to_string(),
                dedupe_key,
                payload,
                vec![],
            )?);
        }

        for p in pulls {
            // Filter by cursor.
            if p.updated_at <= since {
                continue;
            }
            if p.updated_at > max_updated {
                max_updated = p.updated_at;
            }
            let payload = serde_json::json!({
                "repo": format!("{}/{}", self.owner, self.repo),
                "number": p.number,
                "title": p.title,
                "body": p.body,
                "updated_at": p.updated_at,
                "html_url": p.html_url,
            });
            let dedupe_key = format!("pull_request:{}:{}", p.id, p.updated_at.timestamp_millis());
            records.push(RawRecord::new(
                p.id.to_string(),
                dedupe_key,
                payload,
                vec![],
            )?);
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
            // Determine entity type from dedupe key prefix (constructed in pull()).
            let entity_type = if rec.dedupe_key.starts_with("pull_request:") {
                "pull_request"
            } else {
                "issue"
            };

            let entity = ContextEntity::new(
                org_id,
                Some(source.project_id),
                entity_type,
                rec.payload.clone(),
                "github",
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
