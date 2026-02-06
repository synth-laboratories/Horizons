use crate::{
    Event, EventDirection, EventStatus, HorizonsClient, HorizonsError, SubscriptionConfig,
    SubscriptionHandler,
};
use reqwest::Method;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct EventsApi {
    client: HorizonsClient,
}

impl EventsApi {
    pub(crate) fn new(client: HorizonsClient) -> Self {
        Self { client }
    }

    pub async fn list(
        &self,
        project_id: Option<String>,
        topic: Option<String>,
        direction: Option<EventDirection>,
        since: Option<String>,
        until: Option<String>,
        status: Option<EventStatus>,
        limit: i64,
    ) -> Result<Vec<Event>, HorizonsError> {
        #[derive(Serialize)]
        struct Query {
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            topic: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            direction: Option<EventDirection>,
            #[serde(skip_serializing_if = "Option::is_none")]
            since: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            until: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            status: Option<EventStatus>,
            limit: i64,
        }
        let q = Query {
            project_id,
            topic,
            direction,
            since,
            until,
            status,
            limit,
        };
        self.client
            .request_json(Method::GET, "/api/v1/events", Some(&q), None::<&()>)
            .await
    }

    pub async fn publish(
        &self,
        topic: impl Into<String>,
        source: impl Into<String>,
        payload: Value,
        dedupe_key: impl Into<String>,
        direction: EventDirection,
        project_id: Option<String>,
        metadata: Option<std::collections::HashMap<String, Value>>,
        timestamp: Option<String>,
    ) -> Result<String, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            topic: String,
            source: String,
            payload: Value,
            dedupe_key: String,
            direction: EventDirection,
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            metadata: Option<std::collections::HashMap<String, Value>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            timestamp: Option<String>,
        }
        let body = Body {
            topic: topic.into(),
            source: source.into(),
            payload,
            dedupe_key: dedupe_key.into(),
            direction,
            project_id,
            metadata,
            timestamp,
        };
        let v = self
            .client
            .request_value(
                Method::POST,
                "/api/v1/events/publish",
                None::<&()>,
                Some(&body),
            )
            .await?;
        Ok(v.get("event_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string())
    }

    pub async fn subscribe(
        &self,
        topic_pattern: impl Into<String>,
        direction: EventDirection,
        handler: SubscriptionHandler,
        config: Option<SubscriptionConfig>,
        filter: Option<std::collections::HashMap<String, Value>>,
    ) -> Result<String, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            topic_pattern: String,
            direction: EventDirection,
            handler: SubscriptionHandler,
            #[serde(skip_serializing_if = "Option::is_none")]
            config: Option<SubscriptionConfig>,
            #[serde(skip_serializing_if = "Option::is_none")]
            filter: Option<std::collections::HashMap<String, Value>>,
        }
        let body = Body {
            topic_pattern: topic_pattern.into(),
            direction,
            handler,
            config,
            filter,
        };
        let v = self
            .client
            .request_value(
                Method::POST,
                "/api/v1/subscriptions",
                None::<&()>,
                Some(&body),
            )
            .await?;
        Ok(v.get("subscription_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string())
    }
}
