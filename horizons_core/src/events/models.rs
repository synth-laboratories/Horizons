use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventStatus {
    Pending,
    Processing,
    Delivered,
    Failed,
    DeadLettered,
}

/// A durable event routed through Horizons' Event Sync Layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// ULID (sortable by time).
    pub id: String,
    pub org_id: String,
    pub project_id: Option<String>,
    /// When the event occurred (domain timestamp).
    pub timestamp: DateTime<Utc>,
    /// When Horizons received it (ingestion timestamp).
    pub received_at: DateTime<Utc>,
    pub direction: EventDirection,
    /// Dot-delimited topic, e.g. "crm.contact.updated".
    pub topic: String,
    /// Source system identifier, e.g. "hubspot" or "agent:research-bot".
    pub source: String,
    pub payload: serde_json::Value,
    /// Idempotency key (for consumer-side dedupe).
    pub dedupe_key: String,
    pub status: EventStatus,
    pub retry_count: u32,
    /// Headers, trace context, etc.
    pub metadata: serde_json::Value,
    /// Last delivery attempt time (if any).
    pub last_attempt_at: Option<DateTime<Utc>>,
}

impl Event {
    #[tracing::instrument(level = "debug", skip(payload, metadata))]
    pub fn new(
        org_id: impl Into<String> + std::fmt::Debug,
        project_id: Option<String>,
        direction: EventDirection,
        topic: impl Into<String> + std::fmt::Debug,
        source: impl Into<String> + std::fmt::Debug,
        payload: serde_json::Value,
        dedupe_key: impl Into<String> + std::fmt::Debug,
        metadata: serde_json::Value,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<Self> {
        let org_id = org_id.into();
        if org_id.trim().is_empty() {
            return Err(Error::InvalidOrgId);
        }

        let topic = topic.into();
        if topic.trim().is_empty() {
            return Err(Error::InvalidTopic);
        }

        let source = source.into();
        if source.trim().is_empty() {
            return Err(Error::InvalidSource);
        }

        let dedupe_key = dedupe_key.into();
        if dedupe_key.trim().is_empty() {
            return Err(Error::InvalidDedupeKey);
        }

        let now = Utc::now();
        Ok(Self {
            id: ulid::Ulid::new().to_string(),
            org_id,
            project_id,
            timestamp: timestamp.unwrap_or(now),
            received_at: now,
            direction,
            topic,
            source,
            payload,
            dedupe_key,
            status: EventStatus::Pending,
            retry_count: 0,
            metadata,
            last_attempt_at: None,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub id: String,
    pub org_id: String,
    /// Glob topic pattern, e.g. "action.*" or "agent.*.completed".
    pub topic_pattern: String,
    pub direction: EventDirection,
    pub handler: SubscriptionHandler,
    pub config: SubscriptionConfig,
    /// Optional JSON filter (implementation-defined).
    pub filter: Option<serde_json::Value>,
}

impl Subscription {
    #[tracing::instrument(level = "debug", skip(handler, filter))]
    pub fn new(
        org_id: impl Into<String> + std::fmt::Debug,
        topic_pattern: impl Into<String> + std::fmt::Debug,
        direction: EventDirection,
        handler: SubscriptionHandler,
        config: SubscriptionConfig,
        filter: Option<serde_json::Value>,
    ) -> Result<Self> {
        let org_id = org_id.into();
        if org_id.trim().is_empty() {
            return Err(Error::InvalidOrgId);
        }

        let topic_pattern = topic_pattern.into();
        if topic_pattern.trim().is_empty() {
            return Err(Error::InvalidTopicPattern);
        }

        if !handler.is_valid() {
            return Err(Error::InvalidSubscriptionHandler);
        }

        Ok(Self {
            id: ulid::Ulid::new().to_string(),
            org_id,
            topic_pattern,
            direction,
            handler,
            config,
            filter,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionHandler {
    Webhook {
        url: String,
        headers: Vec<(String, String)>,
        /// Request timeout override for this subscription.
        timeout_ms: Option<u64>,
    },
    InternalQueue {
        queue_name: String,
    },
    /// In-process handler registration (e.g. embedded in customer code).
    Callback {
        handler_id: String,
    },
}

impl SubscriptionHandler {
    fn is_valid(&self) -> bool {
        match self {
            Self::Webhook { url, .. } => !url.trim().is_empty(),
            Self::InternalQueue { queue_name } => !queue_name.trim().is_empty(),
            Self::Callback { handler_id } => !handler_id.trim().is_empty(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionConfig {
    pub max_retries: u32,
    pub retry_backoff_base_ms: u64,
    pub retry_backoff_max_ms: u64,
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            retry_backoff_base_ms: 1_000,
            retry_backoff_max_ms: 60_000,
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Consecutive failures required to open the breaker.
    pub failure_threshold: u32,
    /// How long to remain open before attempting half-open delivery.
    pub open_duration_ms: u64,
    /// Successes required in half-open to close the breaker.
    pub success_threshold: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 10,
            open_duration_ms: 30_000,
            success_threshold: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventQuery {
    pub org_id: String,
    pub project_id: Option<String>,
    pub topic: Option<String>,
    pub direction: Option<EventDirection>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub status: Option<EventStatus>,
    pub limit: usize,
}

impl EventQuery {
    #[tracing::instrument(level = "debug")]
    pub fn validate(&self) -> Result<()> {
        if self.org_id.trim().is_empty() {
            return Err(Error::InvalidOrgId);
        }
        if self.limit == 0 {
            return Err(Error::InvalidQuery);
        }
        if let (Some(since), Some(until)) = (self.since, self.until) {
            if since > until {
                return Err(Error::InvalidQuery);
            }
        }
        Ok(())
    }
}
