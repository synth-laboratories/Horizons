use std::sync::Arc;

use redis::aio::ConnectionManager;
use tokio::sync::Mutex;

use super::config::EventSyncConfig;
use super::dlq::DeadLetterQueue;
use super::models::{Event, EventQuery, EventStatus, Subscription};
use super::router::{EventRouter, EventRouterHandle};
use super::store::PgEventStore;
use super::traits::EventBus;
use super::{Error, Result};

pub struct RedisEventBus {
    cfg: EventSyncConfig,
    store: Arc<PgEventStore>,
    publisher: Mutex<ConnectionManager>,
    dlq: DeadLetterQueue,
    router: EventRouterHandle,
}

impl RedisEventBus {
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn connect(cfg: EventSyncConfig) -> Result<Self> {
        cfg.validate()?;

        let store = Arc::new(PgEventStore::connect(&cfg.postgres_url).await?);
        store.migrate().await?;

        let redis_client = redis::Client::open(cfg.redis_url.clone())
            .map_err(|e| Error::message(format!("invalid redis url: {e}")))?;
        let publisher = redis_client.get_connection_manager().await?;

        let dlq = DeadLetterQueue::new(store.clone());
        let router = EventRouter::start(
            cfg.clone(),
            store.clone(),
            redis_client.clone(),
            dlq.clone(),
        );

        Ok(Self {
            cfg,
            store,
            publisher: Mutex::new(publisher),
            dlq,
            router,
        })
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn store(&self) -> Arc<PgEventStore> {
        self.store.clone()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn router(&self) -> &EventRouter {
        self.router.router()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn register_callback_handler<F, Fut>(&self, handler_id: &str, f: F)
    where
        F: Fn(Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let cb: crate::events::router::Callback = Arc::new(move |ev: Event| Box::pin(f(ev)));
        self.router
            .router()
            .register_callback_handler(handler_id, cb);
    }
}

#[async_trait::async_trait]
impl EventBus for RedisEventBus {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn publish(&self, event: Event) -> Result<String> {
        // Basic guardrail: bound payload size.
        let payload_bytes = serde_json::to_vec(&event.payload)
            .map_err(|e| Error::message(format!("invalid payload json: {e}")))?;
        if payload_bytes.len() > self.cfg.max_payload_bytes {
            return Err(Error::PayloadTooLarge);
        }

        self.store.append(&event).await?;

        // Best-effort pub/sub wakeup. Router also sweeps periodically.
        let channel = format!("horizons_events:{}", &event.org_id);
        let msg = event.id.clone();
        {
            let mut conn = self.publisher.lock().await;
            redis::cmd("PUBLISH")
                .arg(&channel)
                .arg(&msg)
                .query_async::<()>(&mut *conn)
                .await?;
        }

        self.router.router().wake_org(&event.org_id).await;

        Ok(event.id)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn subscribe(&self, sub: Subscription) -> Result<String> {
        self.router.router().upsert_subscription(sub.clone());
        self.router.router().wake_org(&sub.org_id).await;
        Ok(sub.id)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn unsubscribe(&self, org_id: &str, sub_id: &str) -> Result<()> {
        self.router.router().remove_subscription(org_id, sub_id);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn ack(&self, org_id: &str, event_id: &str) -> Result<()> {
        self.store
            .update_status(org_id, event_id, EventStatus::Delivered)
            .await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn nack(&self, org_id: &str, event_id: &str, reason: &str) -> Result<()> {
        let retry_count = self
            .store
            .increment_retry_and_mark_failed(org_id, event_id, chrono::Utc::now())
            .await?;

        if retry_count > self.cfg.max_retries {
            self.dlq.move_to_dlq(org_id, event_id, reason).await?;
            return Ok(());
        }

        let delay_ms = self.cfg.retry_backoff_ms(retry_count - 1);
        self.store.reset_to_pending(org_id, event_id).await?;
        let org = org_id.to_string();
        let router = self.router.router().clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            router.wake_org(&org).await;
        });

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn query(&self, filter: EventQuery) -> Result<Vec<Event>> {
        self.store.query(&filter).await
    }
}
