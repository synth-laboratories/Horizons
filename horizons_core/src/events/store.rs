use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Postgres, Row, Transaction};

use super::models::{Event, EventDirection, EventQuery, EventStatus};
use super::traits::EventStore;
use super::{Error, Result};

#[derive(Debug, Clone)]
pub struct PgEventStore {
    pool: PgPool,
}

impl PgEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn connect(postgres_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(postgres_url)
            .await?;
        Ok(Self { pool })
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn migrate(&self) -> Result<()> {
        // Core events table (append-only, except status/retry counters).
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS events (
                id              TEXT PRIMARY KEY,
                org_id          TEXT NOT NULL,
                project_id      TEXT NULL,
                timestamp       TIMESTAMPTZ NOT NULL,
                received_at     TIMESTAMPTZ NOT NULL,
                direction       TEXT NOT NULL,
                topic           TEXT NOT NULL,
                source          TEXT NOT NULL,
                payload         JSONB NOT NULL,
                dedupe_key      TEXT NOT NULL,
                status          TEXT NOT NULL,
                retry_count     INTEGER NOT NULL,
                metadata        JSONB NOT NULL,
                last_attempt_at TIMESTAMPTZ NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Idempotency: prevent storing duplicate events for the same org+dedupe_key.
        // Callers should pick a dedupe_key that's unique enough for their domain.
        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS events_org_dedupe_key_idx
              ON events (org_id, dedupe_key);
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS events_org_status_received_idx
              ON events (org_id, status, received_at);
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS events_org_topic_received_idx
              ON events (org_id, topic, received_at);
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS event_dlq (
                org_id           TEXT NOT NULL,
                event_id         TEXT NOT NULL,
                reason           TEXT NOT NULL,
                dead_lettered_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (org_id, event_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Append an event with dedupe semantics.
    ///
    /// Returns `(event_id, inserted)` where `inserted=false` means an existing event
    /// with the same `(org_id, dedupe_key)` already exists and was returned instead.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn append_deduped(&self, event: &Event) -> Result<(String, bool)> {
        let payload = sqlx::types::Json(&event.payload);
        let metadata = sqlx::types::Json(&event.metadata);

        let res = sqlx::query(
            r#"
            INSERT INTO events
                (id, org_id, project_id, timestamp, received_at, direction, topic, source,
                 payload, dedupe_key, status, retry_count, metadata, last_attempt_at)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8,
                 $9, $10, $11, $12, $13, $14)
            ON CONFLICT (org_id, dedupe_key) DO NOTHING
            "#,
        )
        .bind(&event.id)
        .bind(&event.org_id)
        .bind(&event.project_id)
        .bind(event.timestamp)
        .bind(event.received_at)
        .bind(direction_to_str(&event.direction))
        .bind(&event.topic)
        .bind(&event.source)
        .bind(payload)
        .bind(&event.dedupe_key)
        .bind(status_to_str(&event.status))
        .bind(event.retry_count as i32)
        .bind(metadata)
        .bind(event.last_attempt_at)
        .execute(&self.pool)
        .await?;

        if res.rows_affected() > 0 {
            return Ok((event.id.clone(), true));
        }

        // Conflict: fetch the existing row and return its ID.
        let existing = self
            .get_by_dedupe_key(&event.org_id, &event.dedupe_key)
            .await?
            .ok_or_else(|| Error::message("event dedupe conflict but row not found".to_string()))?;
        Ok((existing.id, false))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn append(&self, event: &Event) -> Result<()> {
        let _ = self.append_deduped(event).await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn get_by_dedupe_key(&self, org_id: &str, dedupe_key: &str) -> Result<Option<Event>> {
        let row = sqlx::query(
            r#"
            SELECT id, org_id, project_id, timestamp, received_at, direction, topic, source,
                   payload, dedupe_key, status, retry_count, metadata, last_attempt_at
              FROM events
             WHERE org_id = $1 AND dedupe_key = $2
             ORDER BY received_at DESC
             LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(dedupe_key)
        .fetch_optional(&self.pool)
        .await?;

        row.map(event_from_row).transpose()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn get(&self, org_id: &str, event_id: &str) -> Result<Option<Event>> {
        let row = sqlx::query(
            r#"
            SELECT id, org_id, project_id, timestamp, received_at, direction, topic, source,
                   payload, dedupe_key, status, retry_count, metadata, last_attempt_at
              FROM events
             WHERE org_id = $1 AND id = $2
             LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(event_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(event_from_row).transpose()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query(&self, filter: &EventQuery) -> Result<Vec<Event>> {
        filter.validate()?;

        let mut qb = sqlx::QueryBuilder::<Postgres>::new(
            "SELECT id, org_id, project_id, timestamp, received_at, direction, topic, source, \
             payload, dedupe_key, status, retry_count, metadata, last_attempt_at \
             FROM events WHERE org_id = ",
        );
        qb.push_bind(&filter.org_id);

        if let Some(project_id) = &filter.project_id {
            qb.push(" AND project_id = ");
            qb.push_bind(project_id);
        }
        if let Some(topic) = &filter.topic {
            qb.push(" AND topic = ");
            qb.push_bind(topic);
        }
        if let Some(direction) = &filter.direction {
            qb.push(" AND direction = ");
            qb.push_bind(direction_to_str(direction));
        }
        if let Some(status) = &filter.status {
            qb.push(" AND status = ");
            qb.push_bind(status_to_str(status));
        }
        if let Some(since) = filter.since {
            qb.push(" AND received_at >= ");
            qb.push_bind(since);
        }
        if let Some(until) = filter.until {
            qb.push(" AND received_at <= ");
            qb.push_bind(until);
        }

        qb.push(" ORDER BY received_at ASC LIMIT ");
        qb.push_bind(filter.limit as i64);

        let rows = qb.build().fetch_all(&self.pool).await?;
        rows.into_iter().map(event_from_row).collect()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn update_status(
        &self,
        org_id: &str,
        event_id: &str,
        status: EventStatus,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE events
               SET status = $3
             WHERE org_id = $1 AND id = $2
            "#,
        )
        .bind(org_id)
        .bind(event_id)
        .bind(status_to_str(&status))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn increment_retry_and_mark_failed(
        &self,
        org_id: &str,
        event_id: &str,
        attempted_at: DateTime<Utc>,
    ) -> Result<u32> {
        let row = sqlx::query(
            r#"
            UPDATE events
               SET status = 'failed',
                   retry_count = retry_count + 1,
                   last_attempt_at = $3
             WHERE org_id = $1 AND id = $2
         RETURNING retry_count
            "#,
        )
        .bind(org_id)
        .bind(event_id)
        .bind(attempted_at)
        .fetch_one(&self.pool)
        .await?;

        let retry_count: i32 = row.try_get("retry_count")?;
        Ok(retry_count.max(0) as u32)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn mark_processing(
        &self,
        org_id: &str,
        event_id: &str,
        attempted_at: DateTime<Utc>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE events
               SET status = 'processing',
                   last_attempt_at = $3
             WHERE org_id = $1 AND id = $2
            "#,
        )
        .bind(org_id)
        .bind(event_id)
        .bind(attempted_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn reset_to_pending(&self, org_id: &str, event_id: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE events
               SET status = 'pending'
             WHERE org_id = $1 AND id = $2
            "#,
        )
        .bind(org_id)
        .bind(event_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn claim_next_pending(&self, org_id: &str) -> Result<Option<Event>> {
        let mut tx = self.pool.begin().await?;
        let maybe_row = claim_next_pending_row(&mut tx, org_id).await?;

        let Some(row) = maybe_row else {
            tx.commit().await?;
            return Ok(None);
        };

        let event = event_from_row(row)?;
        let now = Utc::now();
        sqlx::query(
            r#"
            UPDATE events
               SET status = 'processing',
                   last_attempt_at = $3
             WHERE org_id = $1 AND id = $2
            "#,
        )
        .bind(org_id)
        .bind(&event.id)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        let mut event = event;
        event.status = EventStatus::Processing;
        event.last_attempt_at = Some(now);
        Ok(Some(event))
    }
}

async fn claim_next_pending_row(
    tx: &mut Transaction<'_, Postgres>,
    org_id: &str,
) -> Result<Option<sqlx::postgres::PgRow>> {
    let row = sqlx::query(
        r#"
        SELECT id, org_id, project_id, timestamp, received_at, direction, topic, source,
               payload, dedupe_key, status, retry_count, metadata, last_attempt_at
          FROM events
         WHERE org_id = $1 AND status = 'pending'
         ORDER BY received_at ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(org_id)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(row)
}

pub(crate) fn direction_to_str(d: &EventDirection) -> &'static str {
    match d {
        EventDirection::Inbound => "inbound",
        EventDirection::Outbound => "outbound",
    }
}

pub(crate) fn direction_from_str(s: &str) -> Result<EventDirection> {
    match s {
        "inbound" => Ok(EventDirection::Inbound),
        "outbound" => Ok(EventDirection::Outbound),
        other => Err(Error::message(format!("unknown direction: {other}"))),
    }
}

pub(crate) fn status_to_str(s: &EventStatus) -> &'static str {
    match s {
        EventStatus::Pending => "pending",
        EventStatus::Processing => "processing",
        EventStatus::Delivered => "delivered",
        EventStatus::Failed => "failed",
        EventStatus::DeadLettered => "dead_lettered",
    }
}

pub(crate) fn status_from_str(s: &str) -> Result<EventStatus> {
    match s {
        "pending" => Ok(EventStatus::Pending),
        "processing" => Ok(EventStatus::Processing),
        "delivered" => Ok(EventStatus::Delivered),
        "failed" => Ok(EventStatus::Failed),
        "dead_lettered" => Ok(EventStatus::DeadLettered),
        other => Err(Error::message(format!("unknown status: {other}"))),
    }
}

pub(crate) fn event_from_row_ref(row: &sqlx::postgres::PgRow) -> Result<Event> {
    let id: String = row.try_get("id")?;
    let org_id: String = row.try_get("org_id")?;
    let project_id: Option<String> = row.try_get("project_id")?;
    let timestamp: DateTime<Utc> = row.try_get("timestamp")?;
    let received_at: DateTime<Utc> = row.try_get("received_at")?;
    let direction_s: String = row.try_get("direction")?;
    let direction = direction_from_str(&direction_s)?;
    let topic: String = row.try_get("topic")?;
    let source: String = row.try_get("source")?;
    let payload: sqlx::types::Json<serde_json::Value> = row.try_get("payload")?;
    let dedupe_key: String = row.try_get("dedupe_key")?;
    let status_s: String = row.try_get("status")?;
    let status = status_from_str(&status_s)?;
    let retry_count: i32 = row.try_get("retry_count")?;
    let metadata: sqlx::types::Json<serde_json::Value> = row.try_get("metadata")?;
    let last_attempt_at: Option<DateTime<Utc>> = row.try_get("last_attempt_at")?;

    Ok(Event {
        id,
        org_id,
        project_id,
        timestamp,
        received_at,
        direction,
        topic,
        source,
        payload: payload.0,
        dedupe_key,
        status,
        retry_count: retry_count.max(0) as u32,
        metadata: metadata.0,
        last_attempt_at,
    })
}

pub(crate) fn event_from_row(row: sqlx::postgres::PgRow) -> Result<Event> {
    event_from_row_ref(&row)
}

pub type SharedEventStore = Arc<PgEventStore>;

#[async_trait::async_trait]
impl EventStore for PgEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn append(&self, event: &Event) -> Result<()> {
        Self::append(self, event).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get(&self, org_id: &str, event_id: &str) -> Result<Option<Event>> {
        Self::get(self, org_id, event_id).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn query(&self, filter: &EventQuery) -> Result<Vec<Event>> {
        Self::query(self, filter).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn update_status(&self, org_id: &str, event_id: &str, status: EventStatus) -> Result<()> {
        Self::update_status(self, org_id, event_id, status).await
    }
}
