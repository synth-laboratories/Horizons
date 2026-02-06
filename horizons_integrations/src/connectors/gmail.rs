//! Gmail connector.
//!
//! Implements incremental sync using Gmail's `historyId` cursor.
//! Auth uses an OAuth2 access token (bearer).

use async_trait::async_trait;
use horizons_core::context_refresh::models::{SourceConfig, SyncCursor};
use horizons_core::context_refresh::traits::{Connector, PullResult, RawRecord};
use horizons_core::{Error, Result};
use horizons_core::{context_refresh::models::ContextEntity, models::OrgId};
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use tracing::instrument;

#[derive(Debug, Deserialize)]
struct GmailProfile {
    #[serde(rename = "historyId")]
    history_id: String,
}

#[derive(Debug, Deserialize)]
struct GmailMessagesListResponse {
    #[serde(default)]
    messages: Vec<GmailMessageRef>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GmailMessageRef {
    id: String,
}

#[derive(Debug, Deserialize)]
struct GmailHistoryResponse {
    #[serde(rename = "historyId")]
    history_id: Option<String>,
    #[serde(default)]
    history: Vec<GmailHistoryItem>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GmailHistoryItem {
    #[serde(rename = "messagesAdded", default)]
    messages_added: Vec<GmailMessageAdded>,
}

#[derive(Debug, Deserialize)]
struct GmailMessageAdded {
    message: GmailMessageRef,
}

/// Pulls messages from Gmail.
#[derive(Clone)]
pub struct GmailConnector {
    client: Client,
    token: String,
    user_id: String,
}

impl GmailConnector {
    pub fn new(token: impl Into<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .expect("reqwest client");
        Self {
            client,
            token: token.into(),
            user_id: "me".to_string(),
        }
    }

    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = user_id.into();
        self
    }

    async fn get_profile_history_id(&self) -> Result<String> {
        let url = format!(
            "https://gmail.googleapis.com/gmail/v1/users/{}/profile",
            self.user_id
        );
        let resp = self
            .client
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(Error::backend_reqwest)?;
        let p: GmailProfile = resp.json().await.map_err(Error::backend_reqwest)?;
        if p.history_id.trim().is_empty() {
            return Err(Error::BackendMessage(
                "gmail profile missing historyId".to_string(),
            ));
        }
        Ok(p.history_id)
    }

    async fn get_message(&self, message_id: &str) -> Result<serde_json::Value> {
        let url = format!(
            "https://gmail.googleapis.com/gmail/v1/users/{}/messages/{}",
            self.user_id, message_id
        );
        let resp = self
            .client
            .get(url)
            .bearer_auth(&self.token)
            .query(&[
                ("format", "metadata"),
                ("metadataHeaders", "From"),
                ("metadataHeaders", "To"),
                ("metadataHeaders", "Subject"),
                ("metadataHeaders", "Date"),
            ])
            .send()
            .await
            .map_err(Error::backend_reqwest)?;
        let v: serde_json::Value = resp.json().await.map_err(Error::backend_reqwest)?;
        Ok(v)
    }

    fn cursor_history_id(cursor: Option<SyncCursor>) -> Result<Option<(String, Option<String>)>> {
        let Some(cursor) = cursor else {
            return Ok(None);
        };

        // Allow either a bare string (historyId) or an object:
        // {"history_id":"...", "page_token":"..."}
        if let Some(s) = cursor.value.as_str() {
            return Ok(Some((s.to_string(), None)));
        }
        let obj = cursor.value.as_object().ok_or_else(|| {
            Error::InvalidInput("gmail cursor must be string or object".to_string())
        })?;
        let history_id = obj
            .get("history_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::InvalidInput("gmail cursor missing history_id".to_string()))?
            .to_string();
        let page_token = obj
            .get("page_token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        Ok(Some((history_id, page_token)))
    }
}

#[async_trait]
impl Connector for GmailConnector {
    #[instrument(level = "debug", skip(self))]
    async fn id(&self) -> &'static str {
        "gmail"
    }

    #[instrument(level = "info", skip(self, _source, cursor))]
    async fn pull(
        &self,
        _org_id: OrgId,
        _source: &SourceConfig,
        cursor: Option<SyncCursor>,
    ) -> Result<PullResult> {
        // If we have a historyId cursor, use incremental history sync. Otherwise do a small initial pull.
        if let Some((history_id, page_token)) = Self::cursor_history_id(cursor)? {
            let mut records = Vec::new();
            let mut next_page_token = page_token;
            let mut next_history_id: Option<String> = None;

            // Bounded pagination to avoid unbounded pull times.
            for _ in 0..10 {
                let url = format!(
                    "https://gmail.googleapis.com/gmail/v1/users/{}/history",
                    self.user_id
                );
                let mut req = self.client.get(url).bearer_auth(&self.token).query(&[
                    ("startHistoryId", history_id.as_str()),
                    ("historyTypes", "messageAdded"),
                    ("maxResults", "100"),
                ]);
                if let Some(tok) = &next_page_token {
                    req = req.query(&[("pageToken", tok.as_str())]);
                }

                let resp = req.send().await.map_err(Error::backend_reqwest)?;
                let body: GmailHistoryResponse =
                    resp.json().await.map_err(Error::backend_reqwest)?;
                next_history_id = body.history_id.or(next_history_id);
                next_page_token = body.next_page_token;

                let mut message_ids: Vec<String> = Vec::new();
                for h in body.history {
                    for added in h.messages_added {
                        message_ids.push(added.message.id);
                    }
                }

                for mid in message_ids {
                    let payload = self.get_message(&mid).await?;
                    records.push(RawRecord::new(mid.clone(), mid, payload, vec![])?);
                }

                if next_page_token.is_none() {
                    break;
                }
            }

            let next_cursor = if let Some(tok) = next_page_token {
                Some(SyncCursor::new(serde_json::json!({
                    "history_id": history_id,
                    "page_token": tok,
                })))
            } else {
                let hid = next_history_id.unwrap_or(history_id);
                Some(SyncCursor::new(serde_json::Value::String(hid)))
            };

            return Ok(PullResult {
                records,
                next_cursor,
            });
        }

        // Initial pull: list recent messages from INBOX and snapshot the current historyId.
        let url = format!(
            "https://gmail.googleapis.com/gmail/v1/users/{}/messages",
            self.user_id
        );
        let resp = self
            .client
            .get(url)
            .bearer_auth(&self.token)
            .query(&[("maxResults", "50"), ("labelIds", "INBOX")])
            .send()
            .await
            .map_err(Error::backend_reqwest)?;
        let list: GmailMessagesListResponse = resp.json().await.map_err(Error::backend_reqwest)?;

        let mut records = Vec::new();
        for m in list.messages {
            let payload = self.get_message(&m.id).await?;
            records.push(RawRecord::new(m.id.clone(), m.id, payload, vec![])?);
        }

        // Snapshot current historyId so subsequent pulls can use history sync.
        let history_id = self.get_profile_history_id().await?;
        let next_cursor = if let Some(tok) = list.next_page_token {
            // If the initial list was paginated, allow a continuation using the same cursor shape.
            Some(SyncCursor::new(serde_json::json!({
                "history_id": history_id,
                "page_token": tok,
            })))
        } else {
            Some(SyncCursor::new(serde_json::Value::String(history_id)))
        };

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
                "email",
                rec.payload.clone(),
                "gmail",
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
