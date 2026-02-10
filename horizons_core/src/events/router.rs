use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use futures_util::StreamExt;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

use super::config::EventSyncConfig;
use super::dlq::DeadLetterQueue;
use super::models::{Event, EventStatus, Subscription, SubscriptionHandler};
use super::operations::OperationExecutor;
use super::store::PgEventStore;
use super::webhook::WebhookSender;
use super::{Error, Result};

pub(crate) type Callback = Arc<
    dyn Fn(Event) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

#[derive(Debug, Clone)]
pub(crate) struct CircuitBreakerState {
    consecutive_failures: u32,
    open_until_ms: Option<u128>,
    half_open_successes: u32,
}

impl CircuitBreakerState {
    fn new() -> Self {
        Self {
            consecutive_failures: 0,
            open_until_ms: None,
            half_open_successes: 0,
        }
    }
}

#[derive(Clone)]
pub struct EventRouter {
    cfg: EventSyncConfig,
    store: Arc<PgEventStore>,
    dlq: DeadLetterQueue,
    webhook_sender: WebhookSender,
    operation_executor: Option<Arc<dyn OperationExecutor>>,
    redis: redis::Client,

    subscriptions: Arc<DashMap<String, Vec<Subscription>>>, // org_id -> subs
    callbacks: Arc<DashMap<String, Callback>>,              // handler_id -> callback
    breakers: Arc<DashMap<String, CircuitBreakerState>>,    // "{org_id}:{sub_id}" -> state
    org_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,        // org_id -> mutex

    wake_tx: mpsc::Sender<String>,
}

pub struct EventRouterHandle {
    router: EventRouter,
    tasks: Vec<JoinHandle<()>>,
}

impl EventRouter {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn start(
        cfg: EventSyncConfig,
        store: Arc<PgEventStore>,
        redis: redis::Client,
        dlq: DeadLetterQueue,
        operation_executor: Option<Arc<dyn OperationExecutor>>,
    ) -> EventRouterHandle {
        let (wake_tx, wake_rx) = mpsc::channel::<String>(1024);
        let webhook_sender = WebhookSender::new(
            Duration::from_millis(cfg.outbound_webhook_timeout_ms),
            cfg.webhook_signing_secret.clone(),
        );

        let router = Self {
            cfg,
            store,
            dlq,
            webhook_sender,
            operation_executor,
            redis,
            subscriptions: Arc::new(DashMap::new()),
            callbacks: Arc::new(DashMap::new()),
            breakers: Arc::new(DashMap::new()),
            org_locks: Arc::new(DashMap::new()),
            wake_tx,
        };

        let tasks = vec![
            tokio::spawn(router.clone().run_worker(wake_rx)),
            tokio::spawn(router.clone().run_redis_listener()),
            tokio::spawn(router.clone().run_sweeper()),
        ];

        EventRouterHandle { router, tasks }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn wake_org(&self, org_id: &str) {
        let _ = self.wake_tx.send(org_id.to_string()).await;
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn upsert_subscription(&self, sub: Subscription) {
        self.subscriptions
            .entry(sub.org_id.clone())
            .and_modify(|subs| {
                // Replace if same id exists, otherwise append.
                if let Some(i) = subs.iter().position(|s| s.id == sub.id) {
                    subs[i] = sub.clone();
                } else {
                    subs.push(sub.clone());
                }
            })
            .or_insert_with(|| vec![sub]);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn remove_subscription(&self, org_id: &str, sub_id: &str) {
        if let Some(mut entry) = self.subscriptions.get_mut(org_id) {
            entry.retain(|s| s.id != sub_id);
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn register_callback_handler(&self, handler_id: &str, cb: Callback) {
        self.callbacks.insert(handler_id.to_string(), cb);
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run_redis_listener(self) {
        // Best-effort wakeups. If Redis is unavailable, sweeper still polls.
        let mut backoff_ms = 250u64;
        loop {
            match self.redis.get_async_pubsub().await {
                Ok(mut pubsub) => {
                    backoff_ms = 250;
                    if let Err(err) = pubsub.psubscribe("horizons_events:*").await {
                        tracing::warn!(error = %err, "redis psubscribe failed");
                        tokio::time::sleep(Duration::from_millis(1_000)).await;
                        continue;
                    }
                    if let Err(err) = pubsub.psubscribe("horizons_subscriptions:*").await {
                        tracing::warn!(error = %err, "redis psubscribe failed (subscriptions)");
                        tokio::time::sleep(Duration::from_millis(1_000)).await;
                        continue;
                    }

                    let mut stream = pubsub.on_message();
                    while let Some(msg) = stream.next().await {
                        let channel = msg.get_channel_name();
                        if let Some(org_id) = channel.strip_prefix("horizons_events:") {
                            self.wake_org(org_id).await;
                        } else if let Some(org_id) = channel.strip_prefix("horizons_subscriptions:")
                        {
                            // Reload subscriptions (best-effort) and then wake the org to process any pending events.
                            if let Err(err) = self.reload_subscriptions_for_org(org_id).await {
                                tracing::warn!(
                                    org_id = %org_id,
                                    error = %err,
                                    "failed reloading subscriptions"
                                );
                            }
                            self.wake_org(org_id).await;
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "redis connection failed for pubsub listener");
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(5_000);
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn reload_subscriptions_for_org(&self, org_id: &str) -> Result<()> {
        let subs = self.store.list_subscriptions(org_id).await?;
        if subs.is_empty() {
            self.subscriptions.remove(org_id);
        } else {
            self.subscriptions.insert(org_id.to_string(), subs);
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run_sweeper(self) {
        let interval = Duration::from_millis(self.cfg.poll_interval_ms);
        loop {
            tokio::time::sleep(interval).await;
            for org_id in self.subscriptions.iter().map(|e| e.key().clone()) {
                self.wake_org(&org_id).await;
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run_worker(self, mut wake_rx: mpsc::Receiver<String>) {
        while let Some(org_id) = wake_rx.recv().await {
            let lock = self
                .org_locks
                .entry(org_id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone();

            let _guard = lock.lock().await;
            if let Err(err) = self.process_org(&org_id).await {
                tracing::warn!(org_id = %org_id, error = %err, "failed processing org events");
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn process_org(&self, org_id: &str) -> Result<()> {
        loop {
            let Some(mut event) = self.store.claim_next_pending(org_id).await? else {
                return Ok(());
            };

            event.status = EventStatus::Processing;
            let subs = self.matching_subscriptions(&event);

            if subs.is_empty() {
                self.store
                    .update_status(org_id, &event.id, EventStatus::Delivered)
                    .await?;
                continue;
            }

            // Circuit breaker check: if any matching sub is open, postpone the event without
            // consuming retries.
            if let Some(delay) = self.postpone_if_any_breaker_open(&event, &subs)? {
                self.store.reset_to_pending(org_id, &event.id).await?;
                tokio::spawn({
                    let router = self.clone();
                    let org = org_id.to_string();
                    async move {
                        tokio::time::sleep(delay).await;
                        router.wake_org(&org).await;
                    }
                });
                continue;
            }

            let mut ok = true;
            let mut last_err: Option<Error> = None;
            for sub in &subs {
                if let Err(err) = self.deliver_to_subscription(&event, sub).await {
                    ok = false;
                    last_err = Some(err);
                    break;
                }
            }

            if ok {
                self.store
                    .update_status(org_id, &event.id, EventStatus::Delivered)
                    .await?;
                continue;
            }

            let attempted_at = Utc::now();
            let retry_count = self
                .store
                .increment_retry_and_mark_failed(org_id, &event.id, attempted_at)
                .await?;

            if retry_count > self.cfg.max_retries {
                self.dlq
                    .move_to_dlq(org_id, &event.id, &last_err.unwrap().to_string())
                    .await?;
                continue;
            }

            // Backoff before re-queueing.
            let delay_ms = self.cfg.retry_backoff_ms(retry_count - 1);
            self.store.reset_to_pending(org_id, &event.id).await?;

            tokio::spawn({
                let router = self.clone();
                let org = org_id.to_string();
                async move {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    router.wake_org(&org).await;
                }
            });
        }
    }

    fn matching_subscriptions(&self, event: &Event) -> Vec<Subscription> {
        let Some(subs) = self.subscriptions.get(&event.org_id) else {
            return vec![];
        };
        subs.iter()
            .filter(|s| s.direction == event.direction)
            .filter(|s| topic_glob_matches(&s.topic_pattern, &event.topic))
            .filter(|s| filter_matches(&event.payload, s.filter.as_ref()))
            .cloned()
            .collect()
    }

    fn postpone_if_any_breaker_open(
        &self,
        event: &Event,
        subs: &[Subscription],
    ) -> Result<Option<Duration>> {
        let now_ms = now_epoch_ms();
        let mut max_wait_ms: Option<u128> = None;
        for sub in subs {
            let key = breaker_key(&event.org_id, &sub.id);
            if let Some(state) = self.breakers.get(&key) {
                if let Some(open_until) = state.open_until_ms {
                    if open_until > now_ms {
                        let wait = open_until - now_ms;
                        max_wait_ms = Some(max_wait_ms.map(|m| m.max(wait)).unwrap_or(wait));
                    }
                }
            }
        }
        Ok(max_wait_ms.map(|ms| Duration::from_millis(ms as u64)))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn deliver_to_subscription(&self, event: &Event, sub: &Subscription) -> Result<()> {
        match &sub.handler {
            SubscriptionHandler::Webhook {
                url,
                headers,
                timeout_ms,
            } => {
                let key = breaker_key(&event.org_id, &sub.id);
                let res: Result<()> = self
                    .webhook_sender
                    .send(url, headers, *timeout_ms, event)
                    .await;
                self.record_delivery_result(&key, res.as_ref().err(), &sub.config.circuit_breaker);
                res
            }
            SubscriptionHandler::Operation {
                operation_id,
                environment,
            } => {
                let key = breaker_key(&event.org_id, &sub.id);
                let exec = self.operation_executor.as_ref().ok_or_else(|| {
                    Error::message("operation executor not configured".to_string())
                })?;
                let res = exec
                    .execute(&event.org_id, operation_id, environment.as_deref(), event)
                    .await;
                self.record_delivery_result(&key, res.as_ref().err(), &sub.config.circuit_breaker);
                res
            }
            SubscriptionHandler::Callback { handler_id } => {
                let cb = self
                    .callbacks
                    .get(handler_id)
                    .map(|r| r.value().clone())
                    .ok_or_else(|| {
                        Error::message(format!("callback handler not registered: {handler_id}"))
                    })?;
                (cb)(event.clone()).await
            }
            SubscriptionHandler::InternalQueue { queue_name } => {
                let key = format!(
                    "horizons_events:queue:{org_id}:{queue_name}",
                    org_id = event.org_id
                );
                let payload = serde_json::to_string(event)
                    .map_err(|e| Error::message(format!("failed to serialize event: {e}")))?;

                let mut conn = self.redis.get_connection_manager().await?;
                redis::cmd("LPUSH")
                    .arg(&key)
                    .arg(payload)
                    .query_async::<()>(&mut conn)
                    .await?;
                Ok(())
            }
        }
    }

    fn record_delivery_result(
        &self,
        key: &str,
        err: Option<&Error>,
        cfg: &crate::events::models::CircuitBreakerConfig,
    ) {
        let mut entry = self
            .breakers
            .entry(key.to_string())
            .or_insert_with(CircuitBreakerState::new);

        if err.is_none() {
            entry.consecutive_failures = 0;
            if entry.open_until_ms.is_some() {
                entry.half_open_successes += 1;
                if entry.half_open_successes >= cfg.success_threshold {
                    entry.open_until_ms = None;
                    entry.half_open_successes = 0;
                }
            }
            return;
        }

        entry.half_open_successes = 0;
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        if entry.consecutive_failures >= cfg.failure_threshold {
            entry.open_until_ms = Some(now_epoch_ms().saturating_add(cfg.open_duration_ms as u128));
            entry.consecutive_failures = 0;
        }
    }
}

impl EventRouterHandle {
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn router(&self) -> &EventRouter {
        &self.router
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn shutdown(mut self) {
        // `EventRouterHandle` implements `Drop`, so avoid moving fields out directly.
        let tasks = std::mem::take(&mut self.tasks);
        for t in tasks {
            t.abort();
        }
    }
}

impl Drop for EventRouterHandle {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

fn breaker_key(org_id: &str, sub_id: &str) -> String {
    format!("{org_id}:{sub_id}")
}

fn now_epoch_ms() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis()
}

fn filter_matches(payload: &serde_json::Value, filter: Option<&serde_json::Value>) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let serde_json::Value::Object(filter_obj) = filter else {
        return false;
    };
    let serde_json::Value::Object(payload_obj) = payload else {
        return false;
    };

    for (k, v) in filter_obj {
        if payload_obj.get(k) != Some(v) {
            return false;
        }
    }
    true
}

/// Glob match for dot-delimited topics.
///
/// Semantics:
/// - `*` matches any sequence of characters (including `.`)
/// - `?` matches any single character
/// - other characters match literally
fn topic_glob_matches(pattern: &str, text: &str) -> bool {
    glob_match(pattern.as_bytes(), text.as_bytes())
}

fn glob_match(pat: &[u8], text: &[u8]) -> bool {
    // Iterative glob matcher with backtracking on '*'.
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star_pi: Option<usize> = None;
    let mut star_ti: Option<usize> = None;

    while ti < text.len() {
        if pi < pat.len() && (pat[pi] == b'?' || pat[pi] == text[ti]) {
            pi += 1;
            ti += 1;
            continue;
        }

        if pi < pat.len() && pat[pi] == b'*' {
            star_pi = Some(pi);
            pi += 1;
            star_ti = Some(ti);
            continue;
        }

        if let (Some(sp), Some(st)) = (star_pi, star_ti) {
            pi = sp + 1;
            let next_ti = st + 1;
            star_ti = Some(next_ti);
            ti = next_ti;
            continue;
        }

        return false;
    }

    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }

    pi == pat.len()
}
