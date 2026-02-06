use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::Row;

use super::models::{Event, EventQuery, EventStatus};
use super::store::PgEventStore;
use super::{Error, Result};

#[derive(Debug, Clone)]
pub struct DeadLetterQueue {
    store: Arc<PgEventStore>,
}

#[derive(Debug, Clone)]
pub struct DlqItem {
    pub event: Event,
    pub reason: String,
    pub dead_lettered_at: DateTime<Utc>,
}

impl DeadLetterQueue {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(store: Arc<PgEventStore>) -> Self {
        Self { store }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn move_to_dlq(&self, org_id: &str, event_id: &str, reason: &str) -> Result<()> {
        if org_id.trim().is_empty() {
            return Err(Error::InvalidOrgId);
        }
        if event_id.trim().is_empty() {
            return Err(Error::message("event_id is empty"));
        }

        let mut tx = self.store.pool().begin().await?;
        sqlx::query(
            r#"
            UPDATE events
               SET status = 'dead_lettered'
             WHERE org_id = $1 AND id = $2
            "#,
        )
        .bind(org_id)
        .bind(event_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO event_dlq (org_id, event_id, reason, dead_lettered_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (org_id, event_id) DO UPDATE
              SET reason = EXCLUDED.reason,
                  dead_lettered_at = EXCLUDED.dead_lettered_at
            "#,
        )
        .bind(org_id)
        .bind(event_id)
        .bind(reason)
        .bind(Utc::now())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn list(&self, org_id: &str, limit: usize) -> Result<Vec<DlqItem>> {
        if org_id.trim().is_empty() {
            return Err(Error::InvalidOrgId);
        }
        if limit == 0 {
            return Err(Error::InvalidQuery);
        }

        // Query dead-lettered events joined with their DLQ metadata.
        let rows = sqlx::query(
            r#"
            SELECT
              e.id, e.org_id, e.project_id, e.timestamp, e.received_at, e.direction, e.topic, e.source,
              e.payload, e.dedupe_key, e.status, e.retry_count, e.metadata, e.last_attempt_at,
              d.reason, d.dead_lettered_at
            FROM events e
            JOIN event_dlq d
              ON d.org_id = e.org_id AND d.event_id = e.id
            WHERE e.org_id = $1 AND e.status = 'dead_lettered'
            ORDER BY d.dead_lettered_at DESC
            LIMIT $2
            "#,
        )
        .bind(org_id)
        .bind(limit as i64)
        .fetch_all(self.store.pool())
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let reason: String = row.try_get("reason")?;
            let dead_lettered_at: DateTime<Utc> = row.try_get("dead_lettered_at")?;
            let event = crate::events::store::event_from_row_ref(&row)?;
            out.push(DlqItem {
                event,
                reason,
                dead_lettered_at,
            });
        }

        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn replay(&self, org_id: &str, event_id: &str) -> Result<()> {
        if org_id.trim().is_empty() {
            return Err(Error::InvalidOrgId);
        }
        if event_id.trim().is_empty() {
            return Err(Error::message("event_id is empty"));
        }

        let mut tx = self.store.pool().begin().await?;
        sqlx::query(
            r#"
            UPDATE events
               SET status = 'pending'
             WHERE org_id = $1 AND id = $2 AND status = 'dead_lettered'
            "#,
        )
        .bind(org_id)
        .bind(event_id)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM event_dlq
             WHERE org_id = $1 AND event_id = $2
            "#,
        )
        .bind(org_id)
        .bind(event_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn is_dead_lettered(&self, org_id: &str, event_id: &str) -> Result<bool> {
        let ev = self.store.get(org_id, event_id).await?;
        Ok(matches!(
            ev.map(|e| e.status),
            Some(EventStatus::DeadLettered)
        ))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn query_dead_lettered_events(
        &self,
        org_id: &str,
        limit: usize,
    ) -> Result<Vec<Event>> {
        let filter = EventQuery {
            org_id: org_id.to_string(),
            project_id: None,
            topic: None,
            direction: None,
            since: None,
            until: None,
            status: Some(EventStatus::DeadLettered),
            limit,
        };
        self.store.query(&filter).await
    }
}
