use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("invalid org_id")]
    InvalidOrgId,

    #[error("invalid event topic")]
    InvalidTopic,

    #[error("invalid event source")]
    InvalidSource,

    #[error("invalid dedupe_key")]
    InvalidDedupeKey,

    #[error("invalid topic pattern")]
    InvalidTopicPattern,

    #[error("invalid subscription handler")]
    InvalidSubscriptionHandler,

    #[error("invalid query filter")]
    InvalidQuery,

    #[error("signature verification failed")]
    SignatureVerificationFailed,

    #[error("payload too large")]
    PayloadTooLarge,

    #[error("webhook returned non-success status: {status}")]
    WebhookNonSuccess { status: u16 },

    #[error(transparent)]
    Redis(#[from] redis::RedisError),

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    #[error("{message}")]
    Message { message: String },

    #[error("backend: {0}")]
    Backend(String),
}

impl Error {
    #[tracing::instrument(level = "debug")]
    pub fn message(message: impl Into<String> + std::fmt::Debug) -> Self {
        Self::Message {
            message: message.into(),
        }
    }
}
