//! SQLite-backed EventBus implementation.
//!
//! Designed for single-node/self-hosted deployments where Redis is unavailable
//! but durable event history is required.

use std::path::Path;
use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use super::models::{Event, EventDirection, EventQuery, EventStatus, Subscription};
use super::traits::EventBus;
use super::{Error, Result};

#[derive(Clone)]
pub struct SqliteEventBus {
    pool: SqlitePool,
}

impl SqliteEventBus {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::backend("sqlite_event_bus mkdir", e))?;
        }

        let opts = SqliteConnectOptions::from_str(&format!("sqlite://{}?mode=rwc", path.display()))
            .map_err(|e| Error::backend("sqlite_event_bus connect options", e))?
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await
            .map_err(|e| Error::backend("sqlite_event_bus connect", e))?;

        sqlx::query(SCHEMA)
            .execute(&pool)
            .await
            .map_err(|e| Error::backend("sqlite_event_bus schema", e))?;

        Ok(Self { pool })
    }
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS horizons_events (
  id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  project_id TEXT NULL,
  timestamp TEXT NOT NULL,
  received_at TEXT NOT NULL,
  direction TEXT NOT NULL,
  topic TEXT NOT NULL,
  source TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  dedupe_key TEXT NOT NULL,
  status TEXT NOT NULL,
  retry_count INTEGER NOT NULL,
  metadata_json TEXT NOT NULL,
  last_attempt_at TEXT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS he_org_dedupe_idx ON horizons_events(org_id, dedupe_key);
CREATE INDEX IF NOT EXISTS he_org_project_received_idx
  ON horizons_events(org_id, project_id, received_at DESC);
CREATE INDEX IF NOT EXISTS he_org_topic_received_idx
  ON horizons_events(org_id, topic, received_at DESC);

CREATE TABLE IF NOT EXISTS horizons_subscriptions (
  id TEXT PRIMARY KEY,
  org_id TEXT NOT NULL,
  topic_pattern TEXT NOT NULL,
  direction TEXT NOT NULL,
  handler_json TEXT NOT NULL,
  config_json TEXT NOT NULL,
  filter_json TEXT NULL
);
CREATE INDEX IF NOT EXISTS hs_org_idx ON horizons_subscriptions(org_id);
"#;

#[async_trait]
impl EventBus for SqliteEventBus {
    async fn publish(&self, event: Event) -> Result<String> {
        if event.org_id.trim().is_empty() {
            return Err(Error::InvalidOrgId);
        }
        if event.topic.trim().is_empty() {
            return Err(Error::InvalidTopic);
        }

        let payload_json = serde_json::to_string(&event.payload)
            .map_err(|e| Error::backend("serialize payload", e))?;
        let metadata_json = serde_json::to_string(&event.metadata)
            .map_err(|e| Error::backend("serialize metadata", e))?;

        let rows = sqlx::query(
            r#"
INSERT INTO horizons_events
  (id, org_id, project_id, timestamp, received_at, direction, topic, source,
   payload_json, dedupe_key, status, retry_count, metadata_json, last_attempt_at)
VALUES
  (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
   ?9, ?10, ?11, ?12, ?13, ?14)
ON CONFLICT(org_id, dedupe_key) DO NOTHING
"#,
        )
        .bind(&event.id)
        .bind(&event.org_id)
        .bind(&event.project_id)
        .bind(event.timestamp.to_rfc3339())
        .bind(event.received_at.to_rfc3339())
        .bind(direction_to_str(event.direction))
        .bind(&event.topic)
        .bind(&event.source)
        .bind(payload_json)
        .bind(&event.dedupe_key)
        .bind(status_to_str(event.status))
        .bind(event.retry_count as i64)
        .bind(metadata_json)
        .bind(event.last_attempt_at.map(|d| d.to_rfc3339()))
        .execute(&self.pool)
        .await?;

        if rows.rows_affected() > 0 {
            return Ok(event.id);
        }

        let existing = sqlx::query(
            r#"
SELECT id FROM horizons_events
WHERE org_id = ?1 AND dedupe_key = ?2
LIMIT 1
"#,
        )
        .bind(&event.org_id)
        .bind(&event.dedupe_key)
        .fetch_optional(&self.pool)
        .await?;
        let id: String = existing
            .ok_or_else(|| Error::backend("publish dedupe", "existing row not found"))?
            .get("id");
        Ok(id)
    }

    async fn subscribe(&self, sub: Subscription) -> Result<String> {
        if sub.org_id.trim().is_empty() {
            return Err(Error::InvalidOrgId);
        }
        let handler_json = serde_json::to_string(&sub.handler)
            .map_err(|e| Error::backend("serialize handler", e))?;
        let config_json = serde_json::to_string(&sub.config)
            .map_err(|e| Error::backend("serialize config", e))?;
        let filter_json = sub
            .filter
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| Error::backend("serialize filter", e))?;

        sqlx::query(
            r#"
INSERT OR REPLACE INTO horizons_subscriptions
  (id, org_id, topic_pattern, direction, handler_json, config_json, filter_json)
VALUES
  (?1, ?2, ?3, ?4, ?5, ?6, ?7)
"#,
        )
        .bind(&sub.id)
        .bind(&sub.org_id)
        .bind(&sub.topic_pattern)
        .bind(direction_to_str(sub.direction))
        .bind(handler_json)
        .bind(config_json)
        .bind(filter_json)
        .execute(&self.pool)
        .await?;
        Ok(sub.id)
    }

    async fn unsubscribe(&self, org_id: &str, sub_id: &str) -> Result<()> {
        sqlx::query("DELETE FROM horizons_subscriptions WHERE org_id = ?1 AND id = ?2")
            .bind(org_id)
            .bind(sub_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn ack(&self, org_id: &str, event_id: &str) -> Result<()> {
        sqlx::query(
            r#"
UPDATE horizons_events
SET status = ?3, last_attempt_at = ?4
WHERE org_id = ?1 AND id = ?2
"#,
        )
        .bind(org_id)
        .bind(event_id)
        .bind(status_to_str(EventStatus::Delivered))
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn nack(&self, org_id: &str, event_id: &str, _reason: &str) -> Result<()> {
        sqlx::query(
            r#"
UPDATE horizons_events
SET status = ?3,
    retry_count = retry_count + 1,
    last_attempt_at = ?4
WHERE org_id = ?1 AND id = ?2
"#,
        )
        .bind(org_id)
        .bind(event_id)
        .bind(status_to_str(EventStatus::Failed))
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn query(&self, filter: EventQuery) -> Result<Vec<Event>> {
        filter.validate()?;

        let rows = sqlx::query(
            r#"
SELECT id, org_id, project_id, timestamp, received_at, direction, topic, source,
       payload_json, dedupe_key, status, retry_count, metadata_json, last_attempt_at
FROM horizons_events
WHERE org_id = ?1
ORDER BY received_at ASC
"#,
        )
        .bind(&filter.org_id)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::new();
        for row in rows {
            let event = row_to_event(&row)?;
            if let Some(pid) = &filter.project_id {
                if event.project_id.as_deref() != Some(pid.as_str()) {
                    continue;
                }
            }
            if let Some(topic) = &filter.topic {
                if &event.topic != topic {
                    continue;
                }
            }
            if let Some(direction) = filter.direction {
                if event.direction != direction {
                    continue;
                }
            }
            if let Some(since) = filter.since {
                if event.received_at < since {
                    continue;
                }
            }
            if let Some(until) = filter.until {
                if event.received_at > until {
                    continue;
                }
            }
            if let Some(status) = filter.status {
                if event.status != status {
                    continue;
                }
            }
            out.push(event);
            if out.len() >= filter.limit {
                break;
            }
        }
        Ok(out)
    }

    async fn list_subscriptions(&self, org_id: &str) -> Result<Vec<Subscription>> {
        let rows = sqlx::query(
            r#"
SELECT id, org_id, topic_pattern, direction, handler_json, config_json, filter_json
FROM horizons_subscriptions
WHERE org_id = ?1
ORDER BY id ASC
"#,
        )
        .bind(org_id)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let direction = direction_from_str(row.get::<String, _>("direction").as_str())?;
            let handler_json: String = row.get("handler_json");
            let config_json: String = row.get("config_json");
            let filter_json: Option<String> = row.get("filter_json");
            out.push(Subscription {
                id: row.get("id"),
                org_id: row.get("org_id"),
                topic_pattern: row.get("topic_pattern"),
                direction,
                handler: serde_json::from_str(&handler_json)
                    .map_err(|e| Error::backend("decode subscription handler", e))?,
                config: serde_json::from_str(&config_json)
                    .map_err(|e| Error::backend("decode subscription config", e))?,
                filter: filter_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|e| Error::backend("decode subscription filter", e))?,
            });
        }
        Ok(out)
    }
}

fn row_to_event(row: &sqlx::sqlite::SqliteRow) -> Result<Event> {
    let timestamp = parse_dt(row.get::<String, _>("timestamp").as_str())?;
    let received_at = parse_dt(row.get::<String, _>("received_at").as_str())?;
    let direction = direction_from_str(row.get::<String, _>("direction").as_str())?;
    let status = status_from_str(row.get::<String, _>("status").as_str())?;
    let payload_json: String = row.get("payload_json");
    let metadata_json: String = row.get("metadata_json");
    let last_attempt_at: Option<String> = row.get("last_attempt_at");

    Ok(Event {
        id: row.get("id"),
        org_id: row.get("org_id"),
        project_id: row.get("project_id"),
        timestamp,
        received_at,
        direction,
        topic: row.get("topic"),
        source: row.get("source"),
        payload: serde_json::from_str(&payload_json)
            .map_err(|e| Error::backend("decode event payload", e))?,
        dedupe_key: row.get("dedupe_key"),
        status,
        retry_count: row.get::<i64, _>("retry_count").max(0) as u32,
        metadata: serde_json::from_str(&metadata_json)
            .map_err(|e| Error::backend("decode event metadata", e))?,
        last_attempt_at: last_attempt_at.as_deref().map(parse_dt).transpose()?,
    })
}

fn parse_dt(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| Error::backend("parse datetime", e))
}

fn direction_to_str(v: EventDirection) -> &'static str {
    match v {
        EventDirection::Inbound => "inbound",
        EventDirection::Outbound => "outbound",
    }
}

fn direction_from_str(s: &str) -> Result<EventDirection> {
    match s {
        "inbound" => Ok(EventDirection::Inbound),
        "outbound" => Ok(EventDirection::Outbound),
        _ => Err(Error::backend(
            "direction_from_str",
            format!("unknown: {s}"),
        )),
    }
}

fn status_to_str(v: EventStatus) -> &'static str {
    match v {
        EventStatus::Pending => "pending",
        EventStatus::Processing => "processing",
        EventStatus::Delivered => "delivered",
        EventStatus::Failed => "failed",
        EventStatus::DeadLettered => "dead_lettered",
    }
}

fn status_from_str(s: &str) -> Result<EventStatus> {
    match s {
        "pending" => Ok(EventStatus::Pending),
        "processing" => Ok(EventStatus::Processing),
        "delivered" => Ok(EventStatus::Delivered),
        "failed" => Ok(EventStatus::Failed),
        "dead_lettered" => Ok(EventStatus::DeadLettered),
        _ => Err(Error::backend("status_from_str", format!("unknown: {s}"))),
    }
}
