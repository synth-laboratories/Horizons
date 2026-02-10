use crate::error::ApiError;
use crate::extract::OrgIdHeader;
use crate::server::AppState;
use axum::Extension;
use axum::Json;
use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use horizons_core::events::models::{
    Event, EventDirection, EventQuery, EventStatus, Subscription, SubscriptionConfig,
    SubscriptionHandler,
};
use horizons_core::models::{OrgId, ProjectId};
use serde::Deserialize;
use serde::Serialize;
use std::str::FromStr;
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
    /// Consumer-friendly alias for `project_id` (slug or UUID).
    #[serde(default, alias = "project")]
    pub project: Option<String>,
    pub topic: Option<String>,
    pub direction: Option<EventDirection>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    /// Consumer-friendly "cursor" for polling (epoch seconds).
    #[serde(default)]
    pub after: Option<i64>,
    pub status: Option<EventStatus>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct CompatEvent {
    pub project_slug: String,
    pub event_type: String,
    pub event_id: String,
    pub timestamp: i64,
    pub payload: serde_json::Value,
}

async fn resolve_project_id_from_query(
    state: &AppState,
    org_id: OrgId,
    q: &ListEventsQuery,
) -> Result<(Option<String>, Option<String>), ApiError> {
    if let Some(pid) = q.project_id.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        // If the caller supplied a UUID string, we can also look up a slug for compat responses.
        let slug = if let Ok(uuid) = uuid::Uuid::parse_str(pid) {
            state
                .central_db
                .get_project_by_id(org_id, ProjectId(uuid))
                .await?
                .map(|p| p.slug)
        } else {
            None
        };
        return Ok((Some(pid.to_string()), slug));
    }

    let Some(p) = q.project.as_deref().map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok((None, None));
    };

    // If the slug is actually a UUID, treat it as project_id directly.
    if let Ok(uuid) = uuid::Uuid::parse_str(p) {
        let slug = state
            .central_db
            .get_project_by_id(org_id, ProjectId(uuid))
            .await?
            .map(|p| p.slug);
        return Ok((Some(p.to_string()), slug));
    }

    // Otherwise, resolve via CentralDb slug mapping.
    let rec = state
        .central_db
        .get_project_by_slug(org_id, p)
        .await?
        .ok_or_else(|| {
            ApiError::Core(horizons_core::Error::NotFound(format!(
                "project not found for slug '{p}'"
            )))
        })?;
    Ok((Some(rec.project_id.to_string()), Some(rec.slug)))
}

fn epoch_seconds_to_dt(after: i64) -> Option<DateTime<Utc>> {
    // Vistas uses i64 seconds. Be defensive on invalid ranges.
    chrono::DateTime::<Utc>::from_timestamp(after, 0)
}

#[tracing::instrument(level = "debug", skip_all)]
pub async fn list_events(
    OrgIdHeader(org_id): OrgIdHeader,
    Extension(state): Extension<Arc<AppState>>,
    Query(q): Query<ListEventsQuery>,
) -> Result<axum::response::Response, ApiError> {
    let (project_id, project_slug) = resolve_project_id_from_query(&state, org_id, &q).await?;

    let since_after = q.after.and_then(epoch_seconds_to_dt);
    let since = match (q.since, since_after) {
        (Some(a), Some(b)) => Some(std::cmp::max(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };

    let filter = EventQuery {
        org_id: org_id.to_string(),
        project_id,
        topic: q.topic,
        direction: q.direction,
        since,
        until: q.until,
        status: q.status,
        limit: q.limit.unwrap_or(100),
    };
    filter.validate()?;
    let events = state.event_bus.query(filter).await?;

    // Compatibility mode: if the caller uses the consumer-friendly polling params, return a
    // simplified projection that matches older external consumers (e.g. Vistas).
    if q.after.is_some() || q.project.is_some() {
        let slug = project_slug
            .or_else(|| q.project.clone())
            .unwrap_or_else(|| "unknown".to_string());
        let out: Vec<CompatEvent> = events
            .into_iter()
            .map(|e| CompatEvent {
                project_slug: slug.clone(),
                event_type: e.topic,
                event_id: e.id,
                timestamp: e.timestamp.timestamp(),
                payload: e.payload,
            })
            .collect();
        return Ok(Json(out).into_response());
    }

    Ok(Json(events).into_response())
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
    let project_id_str = req.project_id.clone();
    let event = Event::new(
        org_id.to_string(),
        project_id_str.clone(),
        req.direction,
        req.topic,
        req.source,
        req.payload,
        req.dedupe_key,
        req.metadata,
        req.timestamp,
    )?;
    let id = state.event_bus.publish(event).await?;
    maybe_wake_scheduler_on_inbound(&state, org_id, project_id_str.as_deref(), req.direction).await;
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

    let project_id_str = req.project_id.clone();
    let event = Event::new(
        org_id.to_string(),
        project_id_str.clone(),
        EventDirection::Inbound,
        req.topic,
        req.source,
        req.payload,
        req.dedupe_key,
        req.metadata,
        req.timestamp,
    )?;
    let id = state.event_bus.publish(event).await?;
    maybe_wake_scheduler_on_inbound(
        &state,
        org_id,
        project_id_str.as_deref(),
        EventDirection::Inbound,
    )
    .await;
    Ok(Json(serde_json::json!({ "event_id": id })))
}

async fn maybe_wake_scheduler_on_inbound(
    state: &AppState,
    org_id: horizons_core::OrgId,
    project_id_str: Option<&str>,
    direction: EventDirection,
) {
    if direction != EventDirection::Inbound {
        return;
    }
    let enabled = std::env::var("HORIZONS_CORE_SCHEDULER_WAKE_ON_INBOUND")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if !enabled {
        return;
    }
    let Some(pid) = project_id_str else {
        return;
    };
    let Ok(project_id) = ProjectId::from_str(pid) else {
        return;
    };
    let max_runs: usize = std::env::var("HORIZONS_CORE_SCHEDULER_WAKE_MAX_RUNS_PER_TICK")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let _ = state
        .core_scheduler
        .tick_project(org_id, project_id, chrono::Utc::now(), max_runs.max(1))
        .await;
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
