//! AWS SQS-backed queue transport.
//!
//! Maps Horizons events onto SQS messages. Message bodies are serialized events;
//! receipt handles are returned for ack/nack operations.
//!
//! See: specifications/horizons/specs/event_sync.md

use async_trait::async_trait;
use horizons_core::events::Result;
use horizons_core::events::models::Event;
use tracing::instrument;

use super::QueueBackend;

#[async_trait]
pub trait SqsApi: Send + Sync {
    async fn send_message(&self, queue_url: &str, body: String) -> Result<String>;
    async fn receive_messages(&self, queue_url: &str, max: i32) -> Result<Vec<(String, String)>>; // (body, receipt)
    async fn delete_message(&self, queue_url: &str, receipt: &str) -> Result<()>;
    async fn change_visibility(&self, queue_url: &str, receipt: &str, timeout: i32) -> Result<()>;
}

#[derive(Clone)]
pub struct SqsQueueBackend<C: SqsApi> {
    client: C,
    queue_url: String,
}

impl<C: SqsApi> SqsQueueBackend<C> {
    pub fn new(client: C, queue_url: impl Into<String>) -> Self {
        Self {
            client,
            queue_url: queue_url.into(),
        }
    }
}

#[async_trait]
impl<C: SqsApi> QueueBackend for SqsQueueBackend<C> {
    #[instrument(level = "info", skip(self, event))]
    async fn publish(&self, event: &Event) -> Result<String> {
        let body = serde_json::to_string(event)
            .map_err(|e| horizons_core::events::Error::Backend(format!("serialize event: {e}")))?;
        self.client.send_message(&self.queue_url, body).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn receive(&self, _org_id: &str, max: usize) -> Result<Vec<(Event, String)>> {
        let messages = self
            .client
            .receive_messages(&self.queue_url, max as i32)
            .await?;
        let mut out = Vec::with_capacity(messages.len());
        for (body, receipt) in messages {
            let evt: Event = serde_json::from_str(&body).map_err(|e| {
                horizons_core::events::Error::Backend(format!("deserialize event: {e}"))
            })?;
            out.push((evt, receipt));
        }
        Ok(out)
    }

    #[instrument(level = "debug", skip(self))]
    async fn ack(&self, _org_id: &str, receipt: &str) -> Result<()> {
        self.client.delete_message(&self.queue_url, receipt).await
    }

    #[instrument(level = "debug", skip(self, _reason))]
    async fn nack(&self, _org_id: &str, receipt: &str, _reason: &str) -> Result<()> {
        self.client
            .change_visibility(&self.queue_url, receipt, 0)
            .await
    }
}

/// Real SQS client adapter (only compiled when the `sqs` feature is enabled).
#[cfg(feature = "sqs")]
pub mod real {
    use super::SqsApi;
    use aws_sdk_sqs::Client as SqsClient;
    use horizons_core::events::Result;
    use tracing::instrument;

    #[derive(Clone)]
    pub struct AwsSqsApi {
        inner: SqsClient,
    }

    impl AwsSqsApi {
        pub fn new(inner: SqsClient) -> Self {
            Self { inner }
        }
    }

    #[async_trait::async_trait]
    impl SqsApi for AwsSqsApi {
        #[instrument(level = "debug", skip(self))]
        async fn send_message(&self, queue_url: &str, body: String) -> Result<String> {
            let resp = self
                .inner
                .send_message()
                .queue_url(queue_url)
                .message_body(body)
                .send()
                .await
                .map_err(|e| horizons_core::events::Error::Backend(format!("sqs send: {e}")))?;
            Ok(resp.message_id.unwrap_or_default())
        }

        #[instrument(level = "debug", skip(self))]
        async fn receive_messages(
            &self,
            queue_url: &str,
            max: i32,
        ) -> Result<Vec<(String, String)>> {
            let resp = self
                .inner
                .receive_message()
                .queue_url(queue_url)
                .max_number_of_messages(max)
                .wait_time_seconds(1)
                .send()
                .await
                .map_err(|e| horizons_core::events::Error::Backend(format!("sqs receive: {e}")))?;
            let mut out = Vec::new();
            if let Some(msgs) = resp.messages {
                for m in msgs {
                    if let (Some(body), Some(receipt)) = (m.body, m.receipt_handle) {
                        out.push((body, receipt));
                    }
                }
            }
            Ok(out)
        }

        #[instrument(level = "debug", skip(self))]
        async fn delete_message(&self, queue_url: &str, receipt: &str) -> Result<()> {
            self.inner
                .delete_message()
                .queue_url(queue_url)
                .receipt_handle(receipt)
                .send()
                .await
                .map_err(|e| horizons_core::events::Error::Backend(format!("sqs delete: {e}")))?;
            Ok(())
        }

        #[instrument(level = "debug", skip(self))]
        async fn change_visibility(
            &self,
            queue_url: &str,
            receipt: &str,
            timeout: i32,
        ) -> Result<()> {
            self.inner
                .change_message_visibility()
                .queue_url(queue_url)
                .receipt_handle(receipt)
                .visibility_timeout(timeout)
                .send()
                .await
                .map_err(|e| {
                    horizons_core::events::Error::Backend(format!("sqs visibility: {e}"))
                })?;
            Ok(())
        }
    }
}
