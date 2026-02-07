use crate::error::ApiError;
use crate::extract::OrgIdHeader;
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use horizons_core::events::models::{
    Event, EventDirection, EventQuery, EventStatus, Subscription, SubscriptionConfig,
    SubscriptionHandler,
};
use serde::Deserialize;
use std::sync::Arc;

#[tracing::instrument(level = "debug", skip_all)]
pub fn api_router() -> axum::Router {
    axum::Router::new()
        .route("/events", get(list_events))
        .route("/events/publish", post(publish))
        .route("/subscriptions", post(subscribe).get(list_subscriptions))
        .route("/subscriptions/{id}", axum::routing::delete(unsubscribe))
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn inbound_router() -> axum::Router {
    axum::Router::new().route("/events/inbound", post(inbound))
}

#[derive(Debug, Deserialize)]
pub struct ListEventsQuery {
    pub project_id: Option<String>,
    pub topic: Option<String>,
    pub direction: Option<EventDirection>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub status: Option<EventStatus>,
    pub limit: Option<usize>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn list_events(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<ListEventsQuery>,
) -> Result<Json<Vec<Event>>, ApiError> {
    let filter = EventQuery {
        org_id: org_id.to_string(),
        project_id: q.project_id,
        topic: q.topic,
        direction: q.direction,
        since: q.since,
        until: q.until,
        status: q.status,
        limit: q.limit.unwrap_or(100),
    };
    filter.validate()?;
    let events = state.event_bus.query(filter).await?;
    Ok(Json(events))
}

#[derive(Debug, Deserialize)]
pub struct PublishRequest {
    pub project_id: Option<String>,
    pub direction: EventDirection,
    pub topic: String,
    pub source: String,
    pub payload: serde_json::Value,
    pub dedupe_key: String,
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub timestamp: Option<DateTime<Utc>>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn publish(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<PublishRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let event = Event::new(
        org_id.to_string(),
        req.project_id,
        req.direction,
        req.topic,
        req.source,
        req.payload,
        req.dedupe_key,
        req.metadata,
        req.timestamp,
    )?;
    let id = state.event_bus.publish(event).await?;
    Ok(Json(serde_json::json!({ "event_id": id })))
}

#[derive(Debug, Deserialize)]
pub struct InboundRequest {
    pub project_id: Option<String>,
    pub topic: String,
    pub source: String,
    pub payload: serde_json::Value,
    pub dedupe_key: String,
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub timestamp: Option<DateTime<Utc>>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn inbound(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    // Optional: enforce inbound webhook signature if configured.
    if let Ok(secret) = std::env::var("HORIZONS_WEBHOOK_SIGNING_SECRET") {
        horizons_core::events::webhook::verify_signature_from_headers(&secret, &headers, &body)?;
    }

    // Align payload-size guardrail with the events subsystem default (or allow override).
    let max_bytes = std::env::var("HORIZONS_WEBHOOK_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000);
    if body.len() > max_bytes {
        return Err(ApiError::Events(
            horizons_core::events::Error::PayloadTooLarge,
        ));
    }

    let req: InboundRequest = serde_json::from_slice(&body)
        .map_err(|e| ApiError::InvalidInput(format!("invalid json body: {e}")))?;

    let event = Event::new(
        org_id.to_string(),
        req.project_id,
        EventDirection::Inbound,
        req.topic,
        req.source,
        req.payload,
        req.dedupe_key,
        req.metadata,
        req.timestamp,
    )?;
    let id = state.event_bus.publish(event).await?;
    Ok(Json(serde_json::json!({ "event_id": id })))
}

#[derive(Debug, Deserialize)]
pub struct SubscribeRequest {
    pub topic_pattern: String,
    pub direction: EventDirection,
    pub handler: SubscriptionHandler,
    #[serde(default)]
    pub config: Option<SubscriptionConfig>,
    pub filter: Option<serde_json::Value>,
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn subscribe(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Json(req): Json<SubscribeRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let sub = Subscription::new(
        org_id.to_string(),
        req.topic_pattern,
        req.direction,
        req.handler,
        req.config.unwrap_or_default(),
        req.filter,
    )?;
    let id = state.event_bus.subscribe(sub).await?;
    Ok(Json(serde_json::json!({ "subscription_id": id })))
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn list_subscriptions(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
) -> Result<Json<Vec<Subscription>>, ApiError> {
    let subs = state
        .event_bus
        .list_subscriptions(&org_id.to_string())
        .await?;
    Ok(Json(subs))
}

#[tracing::instrument(level = "info", skip_all)]
pub async fn unsubscribe(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state
        .event_bus
        .unsubscribe(&org_id.to_string(), &id)
        .await?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}
