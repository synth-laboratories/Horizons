use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::models::{Event, EventQuery, Subscription};
use super::traits::EventBus;
use super::{Error, Result};

/// In-memory EventBus for local development and unit tests.
///
/// Semantics:
/// - at-least-once is approximated by durable storage in-memory for the process lifetime
/// - subscriptions are stored but not actively delivered (query-based consumers)
#[derive(Clone, Default)]
pub struct MemoryEventBus {
    events: Arc<Mutex<Vec<Event>>>,
    subs: Arc<Mutex<Vec<Subscription>>>,
}

impl MemoryEventBus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a snapshot of all events (primarily for tests).
    pub async fn all_events(&self) -> Vec<Event> {
        self.events.lock().await.clone()
    }
}

#[async_trait]
impl EventBus for MemoryEventBus {
    async fn publish(&self, event: Event) -> Result<String> {
        if event.org_id.trim().is_empty() {
            return Err(Error::InvalidOrgId);
        }
        if event.topic.trim().is_empty() {
            return Err(Error::InvalidTopic);
        }
        let id = event.id.clone();
        self.events.lock().await.push(event);
        Ok(id)
    }

    async fn subscribe(&self, sub: Subscription) -> Result<String> {
        if sub.org_id.trim().is_empty() {
            return Err(Error::InvalidOrgId);
        }
        let id = sub.id.clone();
        self.subs.lock().await.push(sub);
        Ok(id)
    }

    async fn unsubscribe(&self, org_id: &str, sub_id: &str) -> Result<()> {
        let mut subs = self.subs.lock().await;
        subs.retain(|s| !(s.org_id == org_id && s.id == sub_id));
        Ok(())
    }

    async fn ack(&self, _org_id: &str, _event_id: &str) -> Result<()> {
        Ok(())
    }

    async fn nack(&self, _org_id: &str, _event_id: &str, _reason: &str) -> Result<()> {
        Ok(())
    }

    async fn query(&self, filter: EventQuery) -> Result<Vec<Event>> {
        filter.validate()?;

        let events = self.events.lock().await;
        let mut out = Vec::new();
        for e in events.iter() {
            if e.org_id != filter.org_id {
                continue;
            }
            if let Some(pid) = &filter.project_id {
                if e.project_id.as_deref() != Some(pid.as_str()) {
                    continue;
                }
            }
            if let Some(topic) = &filter.topic {
                if &e.topic != topic {
                    continue;
                }
            }
            if let Some(dir) = filter.direction {
                if e.direction != dir {
                    continue;
                }
            }
            if let Some(since) = filter.since {
                if e.received_at < since {
                    continue;
                }
            }
            if let Some(until) = filter.until {
                if e.received_at > until {
                    continue;
                }
            }
            if let Some(status) = filter.status {
                if e.status != status {
                    continue;
                }
            }

            out.push(e.clone());
            if out.len() >= filter.limit {
                break;
            }
        }
        Ok(out)
    }

    async fn list_subscriptions(&self, org_id: &str) -> Result<Vec<Subscription>> {
        let subs = self.subs.lock().await;
        Ok(subs
            .iter()
            .filter(|s| s.org_id == org_id)
            .cloned()
            .collect())
    }
}
