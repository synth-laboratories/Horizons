use thiserror::Error;

use crate::ir::Diagnostic;

pub type Result<T> = std::result::Result<T, GraphError>;

#[derive(Error, Debug, Clone)]
pub enum GraphError {
    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("invalid graph")]
    InvalidGraph { diagnostics: Vec<Diagnostic> },

    #[error("internal error: {0}")]
    Internal(String),

    #[error("unavailable: {0}")]
    Unavailable(String),
}

impl GraphError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub fn invalid_graph(diagnostics: Vec<Diagnostic>) -> Self {
        Self::InvalidGraph { diagnostics }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::Unavailable(message.into())
    }
}
