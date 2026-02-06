//! RabbitMQ-backed queue transport.
//!
//! Uses AMQP to publish and consume event messages. The adapter is feature-gated
//! and keeps the contract minimal for easy substitution.
//!
//! See: specifications/horizons/specs/event_sync.md

use async_trait::async_trait;
use horizons_core::events::Result;
use horizons_core::events::models::Event;
use tracing::instrument;

use super::QueueBackend;

#[async_trait]
pub trait AmqpApi: Send + Sync {
    async fn publish(&self, queue: &str, body: Vec<u8>) -> Result<()>;
    async fn receive(&self, queue: &str, max: u16) -> Result<Vec<(Vec<u8>, String)>>;
    async fn ack(&self, delivery_tag: &str) -> Result<()>;
    async fn nack(&self, delivery_tag: &str, requeue: bool) -> Result<()>;
}

#[derive(Clone)]
pub struct RabbitMqBackend<C: AmqpApi> {
    client: C,
    queue: String,
}

impl<C: AmqpApi> RabbitMqBackend<C> {
    pub fn new(client: C, queue: impl Into<String>) -> Self {
        Self {
            client,
            queue: queue.into(),
        }
    }
}

#[async_trait]
impl<C: AmqpApi> QueueBackend for RabbitMqBackend<C> {
    #[instrument(level = "info", skip(self, event))]
    async fn publish(&self, event: &Event) -> Result<String> {
        let body = serde_json::to_vec(event)
            .map_err(|e| horizons_core::events::Error::Backend(format!("serialize event: {e}")))?;
        self.client.publish(&self.queue, body).await?;
        Ok(event.id.clone())
    }

    #[instrument(level = "debug", skip(self))]
    async fn receive(&self, _org_id: &str, max: usize) -> Result<Vec<(Event, String)>> {
        let deliveries = self.client.receive(&self.queue, max as u16).await?;
        let mut out = Vec::with_capacity(deliveries.len());
        for (body, tag) in deliveries {
            let evt: Event = serde_json::from_slice(&body).map_err(|e| {
                horizons_core::events::Error::Backend(format!("deserialize event: {e}"))
            })?;
            out.push((evt, tag));
        }
        Ok(out)
    }

    #[instrument(level = "debug", skip(self))]
    async fn ack(&self, _org_id: &str, receipt: &str) -> Result<()> {
        self.client.ack(receipt).await
    }

    #[instrument(level = "debug", skip(self, _reason))]
    async fn nack(&self, _org_id: &str, receipt: &str, _reason: &str) -> Result<()> {
        self.client.nack(receipt, true).await
    }
}

/// Real AMQP adapter (only compiled when the `rabbitmq` feature is enabled).
#[cfg(feature = "rabbitmq")]
pub mod real {
    use super::AmqpApi;
    use futures_util::StreamExt;
    use horizons_core::events::Result;
    use lapin::{
        BasicProperties, Channel, Connection, ConnectionProperties, options::*, types::FieldTable,
    };
    use tracing::instrument;

    #[derive(Clone)]
    pub struct LapinAmqpApi {
        channel: Channel,
    }

    impl LapinAmqpApi {
        pub async fn connect(uri: &str, queue: &str) -> Result<Self> {
            let conn = Connection::connect(uri, ConnectionProperties::default())
                .await
                .map_err(|e| horizons_core::events::Error::Backend(format!("amqp connect: {e}")))?;
            let ch = conn
                .create_channel()
                .await
                .map_err(|e| horizons_core::events::Error::Backend(format!("amqp channel: {e}")))?;
            ch.queue_declare(queue, QueueDeclareOptions::default(), FieldTable::default())
                .await
                .map_err(|e| horizons_core::events::Error::Backend(format!("amqp declare: {e}")))?;
            Ok(Self { channel: ch })
        }
    }

    #[async_trait::async_trait]
    impl AmqpApi for LapinAmqpApi {
        #[instrument(level = "debug", skip(self, body))]
        async fn publish(&self, queue: &str, body: Vec<u8>) -> Result<()> {
            self.channel
                .basic_publish(
                    "",
                    queue,
                    BasicPublishOptions::default(),
                    &body,
                    BasicProperties::default(),
                )
                .await
                .map_err(|e| horizons_core::events::Error::Backend(format!("amqp publish: {e}")))?;
            Ok(())
        }

        #[instrument(level = "debug", skip(self))]
        async fn receive(&self, queue: &str, max: u16) -> Result<Vec<(Vec<u8>, String)>> {
            let mut consumer = self
                .channel
                .basic_consume(
                    queue,
                    "horizons",
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| horizons_core::events::Error::Backend(format!("amqp consume: {e}")))?;
            let mut out = Vec::new();
            while let Some(delivery) = consumer.next().await {
                let d = delivery.map_err(|e| {
                    horizons_core::events::Error::Backend(format!("amqp delivery: {e}"))
                })?;
                out.push((d.data.clone(), d.delivery_tag.to_string()));
                if out.len() as u16 >= max {
                    break;
                }
            }
            Ok(out)
        }

        #[instrument(level = "debug", skip(self))]
        async fn ack(&self, delivery_tag: &str) -> Result<()> {
            let tag: u64 = delivery_tag.parse().unwrap_or_default();
            self.channel
                .basic_ack(tag, BasicAckOptions::default())
                .await
                .map_err(|e| horizons_core::events::Error::Backend(format!("amqp ack: {e}")))?;
            Ok(())
        }

        #[instrument(level = "debug", skip(self))]
        async fn nack(&self, delivery_tag: &str, requeue: bool) -> Result<()> {
            let tag: u64 = delivery_tag.parse().unwrap_or_default();
            self.channel
                .basic_nack(
                    tag,
                    BasicNackOptions {
                        requeue,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| horizons_core::events::Error::Backend(format!("amqp nack: {e}")))?;
            Ok(())
        }
    }
}
