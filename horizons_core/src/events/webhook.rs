use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use super::config::EventSyncConfig;
use super::models::{Event, EventDirection};
use super::traits::EventBus;
use super::{Error, Result};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct InboundWebhookConfig {
    pub signing_secret: Option<String>,
    pub max_payload_bytes: usize,
}

impl InboundWebhookConfig {
    #[tracing::instrument(level = "debug")]
    pub fn from_event_sync(cfg: &EventSyncConfig) -> Self {
        Self {
            signing_secret: cfg.webhook_signing_secret.clone(),
            max_payload_bytes: cfg.max_payload_bytes,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InboundEventRequest {
    pub org_id: String,
    pub project_id: Option<String>,
    pub timestamp: Option<DateTime<Utc>>,
    pub topic: String,
    pub source: String,
    pub payload: serde_json::Value,
    pub dedupe_key: String,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InboundEventResponse {
    pub event_id: String,
}

#[derive(Clone)]
struct WebhookState {
    bus: Arc<dyn EventBus>,
    cfg: InboundWebhookConfig,
}

/// Builds an Axum router that exposes `POST /events/inbound`.
#[tracing::instrument(level = "debug", skip_all)]
pub fn inbound_router(bus: Arc<dyn EventBus>, cfg: InboundWebhookConfig) -> axum::Router {
    axum::Router::new()
        .route("/events/inbound", post(inbound_handler))
        .with_state(WebhookState { bus, cfg })
}

#[tracing::instrument(level = "debug", skip_all)]
async fn inbound_handler(
    State(state): State<WebhookState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    match handle_inbound(state, headers, body).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(err) => {
            let status = match err {
                Error::SignatureVerificationFailed => StatusCode::UNAUTHORIZED,
                Error::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
                Error::InvalidOrgId
                | Error::InvalidTopic
                | Error::InvalidSource
                | Error::InvalidDedupeKey
                | Error::InvalidQuery
                | Error::InvalidTopicPattern
                | Error::InvalidSubscriptionHandler => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (
                status,
                Json(serde_json::json!({ "error": err.to_string() })),
            )
                .into_response()
        }
    }
}

#[tracing::instrument(level = "debug", skip_all)]
async fn handle_inbound(
    state: WebhookState,
    headers: HeaderMap,
    body: Bytes,
) -> Result<InboundEventResponse> {
    if body.len() > state.cfg.max_payload_bytes {
        return Err(Error::PayloadTooLarge);
    }

    verify_signature_if_configured(&state.cfg, &headers, &body)?;

    let req: InboundEventRequest = serde_json::from_slice(&body)
        .map_err(|e| Error::message(format!("invalid json body: {e}")))?;

    let mut metadata = req.metadata.unwrap_or_else(|| serde_json::json!({}));
    // Include selected request headers as metadata for diagnostics.
    let mut hdrs = HashMap::new();
    for (k, v) in headers.iter() {
        if let Ok(s) = v.to_str() {
            hdrs.insert(k.as_str().to_string(), s.to_string());
        }
    }
    if let serde_json::Value::Object(map) = &mut metadata {
        map.insert("headers".to_string(), serde_json::json!(hdrs));
    }

    let event = Event::new(
        req.org_id,
        req.project_id,
        EventDirection::Inbound,
        req.topic,
        req.source,
        req.payload,
        req.dedupe_key,
        metadata,
        req.timestamp,
    )?;

    let event_id = state.bus.publish(event).await?;
    Ok(InboundEventResponse { event_id })
}

#[tracing::instrument(level = "debug", skip_all)]
fn verify_signature_if_configured(
    cfg: &InboundWebhookConfig,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<()> {
    let Some(secret) = &cfg.signing_secret else {
        return Ok(());
    };

    verify_signature_from_headers(secret, headers, body)
}

/// Verify a Horizons inbound webhook signature.
///
/// Accepts `x-horizons-signature` in either raw hex or `sha256=<hex>` format.
#[tracing::instrument(level = "debug", skip(body))]
pub fn verify_horizons_signature(secret: &str, signature_header: &str, body: &[u8]) -> Result<()> {
    let sig = signature_header.trim();

    // We accept both raw hex and "sha256=<hex>" formats.
    let sig_hex = sig.strip_prefix("sha256=").unwrap_or(sig);
    let provided = hex::decode(sig_hex).map_err(|_| Error::SignatureVerificationFailed)?;

    // `hmac` provides constant-time verification.
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| Error::SignatureVerificationFailed)?;
    mac.update(body);
    mac.verify_slice(&provided)
        .map_err(|_| Error::SignatureVerificationFailed)?;

    Ok(())
}

/// Verify a signature using the standard Horizons header name.
#[tracing::instrument(level = "debug", skip(body))]
pub fn verify_signature_from_headers(secret: &str, headers: &HeaderMap, body: &[u8]) -> Result<()> {
    let Some(sig) = headers.get("x-horizons-signature") else {
        return Err(Error::SignatureVerificationFailed);
    };
    let sig = sig
        .to_str()
        .map_err(|_| Error::SignatureVerificationFailed)?;
    verify_horizons_signature(secret, sig, body)
}

/// Compute a Horizons webhook signature header value for `body`.
///
/// Format: `sha256=<hex>`
#[tracing::instrument(level = "debug", skip(body))]
pub fn sign_horizons_body(secret: &str, body: &[u8]) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| Error::SignatureVerificationFailed)?;
    mac.update(body);
    let bytes = mac.finalize().into_bytes();
    Ok(format!("sha256={}", hex::encode(bytes)))
}

#[derive(Debug, Clone)]
pub struct WebhookSender {
    client: reqwest::Client,
    default_timeout: Duration,
    signing_secret: Option<String>,
}

impl WebhookSender {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn new(default_timeout: Duration, signing_secret: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("horizons_events/0.0.0")
            .build()
            .expect("reqwest client build should not fail");
        Self {
            client,
            default_timeout,
            signing_secret,
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn send(
        &self,
        url: &str,
        headers: &[(String, String)],
        timeout_ms: Option<u64>,
        event: &Event,
    ) -> Result<()> {
        let timeout = timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(self.default_timeout);

        let body = serde_json::to_vec(event).map_err(|e| Error::message(format!("json: {e}")))?;
        let mut req = self
            .client
            .post(url)
            .timeout(timeout)
            .header(CONTENT_TYPE, "application/json")
            .body(body.clone());

        // Optional signing: attach `x-horizons-signature` so consumers can verify authenticity.
        if let Some(secret) = self.signing_secret.as_deref() {
            let sig = sign_horizons_body(secret, &body)?;
            req = req.header("x-horizons-signature", sig);
        }

        for (k, v) in headers {
            req = req.header(k, v);
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            return Err(Error::WebhookNonSuccess {
                status: resp.status().as_u16(),
            });
        }
        Ok(())
    }
}
