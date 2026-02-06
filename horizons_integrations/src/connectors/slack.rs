//! Slack connector.
//!
//! Pulls channel messages using Slack Web API with a bot token.
//! Incremental sync uses an `oldest` timestamp cursor.

use async_trait::async_trait;
use horizons_core::context_refresh::models::ContextEntity;
use horizons_core::context_refresh::models::{SourceConfig, SyncCursor};
use horizons_core::context_refresh::traits::{Connector, PullResult, RawRecord};
use horizons_core::models::OrgId;
use horizons_core::{Error, Result};
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use tracing::instrument;

#[derive(Debug, Deserialize)]
struct SlackHistoryResponse {
    ok: bool,
    #[serde(default)]
    messages: Vec<SlackMessage>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct SlackMessage {
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    ts: String,
}

#[derive(Clone)]
pub struct SlackConnector {
    client: Client,
    token: String,
    channel_id: String,
}

impl SlackConnector {
    pub fn new(token: impl Into<String>, channel_id: impl Into<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .expect("reqwest client");
        Self {
            client,
            token: token.into(),
            channel_id: channel_id.into(),
        }
    }

    fn cursor_oldest(cursor: Option<SyncCursor>) -> Result<Option<String>> {
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        if let Some(s) = cursor.value.as_str() {
            return Ok(Some(s.to_string()));
        }
        if let Some(n) = cursor.value.as_f64() {
            return Ok(Some(format!("{n}")));
        }
        Err(Error::InvalidInput(
            "slack cursor must be a string or number timestamp".to_string(),
        ))
    }
}

#[async_trait]
impl Connector for SlackConnector {
    #[instrument(level = "debug", skip(self))]
    async fn id(&self) -> &'static str {
        "slack"
    }

    #[instrument(level = "info", skip(self, _source, cursor))]
    async fn pull(
        &self,
        _org_id: OrgId,
        _source: &SourceConfig,
        cursor: Option<SyncCursor>,
    ) -> Result<PullResult> {
        let oldest = Self::cursor_oldest(cursor)?;

        let mut req = self
            .client
            .get("https://slack.com/api/conversations.history")
            .bearer_auth(&self.token)
            .query(&[("channel", self.channel_id.as_str()), ("limit", "200")]);
        if let Some(o) = &oldest {
            req = req.query(&[("oldest", o.as_str())]);
        }

        let resp = req.send().await.map_err(Error::backend_reqwest)?;
        let body: SlackHistoryResponse = resp.json().await.map_err(Error::backend_reqwest)?;
        if !body.ok {
            return Err(Error::BackendMessage(format!(
                "slack conversations.history failed: {}",
                body.error.unwrap_or_else(|| "unknown".to_string())
            )));
        }

        let mut max_ts: Option<(f64, String)> = oldest
            .as_deref()
            .and_then(|s| s.parse::<f64>().ok())
            .map(|n| (n, oldest.unwrap()));
        let mut records = Vec::new();
        for msg in body.messages {
            let ts = msg.ts;
            // Slack timestamps are strings like "1700000000.12345"
            if let Ok(ts_num) = ts.parse::<f64>() {
                if max_ts.as_ref().map_or(true, |(cur, _)| ts_num > *cur) {
                    max_ts = Some((ts_num, ts.clone()));
                }
            }
            let payload = serde_json::json!({
                "channel_id": self.channel_id,
                "user": msg.user,
                "text": msg.text,
                "ts": ts.clone(),
            });
            records.push(RawRecord::new(ts.clone(), ts, payload, vec![])?);
        }

        // If `has_more` is true, Slack expects pagination with `cursor`, but this
        // v0.1.1 connector keeps pulls bounded and relies on the next run to continue.
        // The timestamp cursor still prevents full re-pulls.
        let next_cursor = max_ts.map(|(_, ts)| SyncCursor::new(serde_json::Value::String(ts)));

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
                "message",
                rec.payload.clone(),
                "slack",
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
