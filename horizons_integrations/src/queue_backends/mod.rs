//! Alternative queue backends for the Event Sync layer.
//!
//! These backends implement a lightweight transport abstraction that mirrors
//! the behavior expected by `horizons_events::traits::EventBus` without changing
//! the core traits. They are opt-in and feature-gated.

use async_trait::async_trait;
use horizons_core::events::Result;
use horizons_core::events::models::Event;

#[async_trait]
pub trait QueueBackend: Send + Sync {
    /// Publish an event payload to the transport.
    async fn publish(&self, event: &Event) -> Result<String>;

    /// Retrieve a batch of events for processing.
    async fn receive(&self, org_id: &str, max: usize) -> Result<Vec<(Event, String)>>;

    /// Acknowledge successful processing (delete from queue).
    async fn ack(&self, org_id: &str, receipt: &str) -> Result<()>;

    /// Negative-acknowledge: make available again or dead-letter.
    async fn nack(&self, org_id: &str, receipt: &str, reason: &str) -> Result<()>;
}

#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;
#[cfg(feature = "sqs")]
pub mod sqs;
