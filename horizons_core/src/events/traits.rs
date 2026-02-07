use async_trait::async_trait;

use super::Result;
use super::models::{Event, EventQuery, EventStatus, Subscription};

#[async_trait]
pub trait EventBus: Send + Sync {
    /// Publish an event (inbound or outbound).
    ///
    /// Implementations must provide at-least-once delivery.
    async fn publish(&self, event: Event) -> Result<String>; // event_id

    /// Subscribe to events matching a topic pattern.
    async fn subscribe(&self, sub: Subscription) -> Result<String>; // sub_id

    /// Unsubscribe.
    async fn unsubscribe(&self, org_id: &str, sub_id: &str) -> Result<()>;

    /// Acknowledge successful processing.
    async fn ack(&self, org_id: &str, event_id: &str) -> Result<()>;

    /// Negative-acknowledge (retry or dead-letter).
    async fn nack(&self, org_id: &str, event_id: &str, reason: &str) -> Result<()>;

    /// Query events (for replay, debugging, audit).
    async fn query(&self, filter: EventQuery) -> Result<Vec<Event>>;

    /// List current subscriptions for an org.
    ///
    /// Default implementation returns an empty list. Implementations that
    /// persist subscriptions (e.g. Postgres-backed) should override.
    async fn list_subscriptions(&self, _org_id: &str) -> Result<Vec<Subscription>> {
        Ok(Vec::new())
    }
}

#[async_trait]
pub trait EventStore: Send + Sync {
    async fn append(&self, event: &Event) -> Result<()>;

    async fn get(&self, org_id: &str, event_id: &str) -> Result<Option<Event>>;

    async fn query(&self, filter: &EventQuery) -> Result<Vec<Event>>;

    async fn update_status(&self, org_id: &str, event_id: &str, status: EventStatus) -> Result<()>;
}
