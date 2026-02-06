use std::error::Error as StdError;

/// Common error type for `horizons_core`.
///
/// Concrete backend implementations (Postgres, Redis, etc.) should preserve the
/// underlying error chain where possible via `Error::backend`.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("backend error: {context}")]
    Backend {
        context: String,
        #[source]
        source: Box<dyn StdError + Send + Sync + 'static>,
    },

    #[error("backend error: {0}")]
    BackendMessage(String),
}

impl Error {
    #[tracing::instrument(level = "debug", name = "horizons.error.backend", skip(source))]
    pub fn backend(
        context: impl Into<String> + std::fmt::Debug,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::Backend {
            context: context.into(),
            source: Box::new(source),
        }
    }

    /// Convenience: wrap any error into `Backend` with "reqwest" context.
    pub fn backend_reqwest(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::Backend {
            context: "reqwest".into(),
            source: Box::new(source),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
