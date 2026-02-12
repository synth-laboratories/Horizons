use std::sync::Arc;

use async_trait::async_trait;

use super::Result;
use super::models::{Event, EventQuery, EventStatus, Subscription};
use super::store::PgEventStore;
use super::traits::EventBus;

/// Postgres-backed EventBus without Redis routing.
///
/// This provides durable publish/query/ack/nack/subscription storage for
/// single-node deployments that do not require Redis-based fanout.
#[derive(Clone)]
pub struct PostgresEventBus {
    store: Arc<PgEventStore>,
}

impl PostgresEventBus {
    pub async fn connect(postgres_url: &str) -> Result<Self> {
        let store = Arc::new(PgEventStore::connect(postgres_url).await?);
        store.migrate().await?;
        Ok(Self { store })
    }

    pub fn store(&self) -> Arc<PgEventStore> {
        self.store.clone()
    }
}

#[async_trait]
impl EventBus for PostgresEventBus {
    async fn publish(&self, event: Event) -> Result<String> {
        let (id, _inserted) = self.store.append_deduped(&event).await?;
        Ok(id)
    }

    async fn subscribe(&self, sub: Subscription) -> Result<String> {
        self.store.upsert_subscription(&sub).await?;
        Ok(sub.id)
    }

    async fn unsubscribe(&self, org_id: &str, sub_id: &str) -> Result<()> {
        self.store.delete_subscription(org_id, sub_id).await
    }

    async fn ack(&self, org_id: &str, event_id: &str) -> Result<()> {
        self.store
            .update_status(org_id, event_id, EventStatus::Delivered)
            .await
    }

    async fn nack(&self, org_id: &str, event_id: &str, _reason: &str) -> Result<()> {
        self.store
            .increment_retry_and_mark_failed(org_id, event_id, chrono::Utc::now())
            .await?;
        Ok(())
    }

    async fn query(&self, filter: EventQuery) -> Result<Vec<Event>> {
        self.store.query(&filter).await
    }

    async fn list_subscriptions(&self, org_id: &str) -> Result<Vec<Subscription>> {
        self.store.list_subscriptions(org_id).await
    }
}
