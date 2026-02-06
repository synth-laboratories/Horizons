//! Voyager: general-purpose long-term agent memory.
//!
//! This crate is intentionally standalone (no Horizons dependency).
//! It provides:
//! - Append-only memory items (per-org, per-agent scope)
//! - Semantic retrieval (vector similarity + recency bias)
//! - Optional batch summarization (compress older items into snapshots)

#![forbid(unsafe_code)]

pub mod config;
pub mod memory;
pub mod models;
pub mod retrieval;
pub mod store;
pub mod summarizer;

use std::error::Error as StdError;

pub type Result<T> = std::result::Result<T, VoyagerError>;

pub use config::{
    HelixHttpConfig, RetrievalConfig, SummarizationConfig, VectorHttpConfig, VoyagerConfig,
};
pub use memory::{EmbeddingModel, ImportanceModel, VoyagerMemory};
pub use models::{MemoryItem, MemoryType, RetrievalQuery, Scope, Summary, VectorMatch};
pub use store::{
    HelixCypherStore, HttpVectorIndex, InMemoryStore, InMemoryVectorIndex, MemoryStore, VectorIndex,
};
pub use summarizer::{SummarizationModel, parse_horizon};

#[derive(thiserror::Error, Debug)]
pub enum VoyagerError {
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("embedding failed: {0}")]
    EmbeddingFailed(String),

    #[error("summarization failed: {0}")]
    SummarizationFailed(String),

    #[error("store error: {0}")]
    Store(String),

    #[error("vector index error: {0}")]
    VectorIndex(String),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("unexpected error: {0}")]
    Unexpected(String),
}

impl VoyagerError {
    #[tracing::instrument(level = "debug", skip(err))]
    pub fn store<E: StdError>(err: E) -> Self {
        Self::Store(err.to_string())
    }

    #[tracing::instrument(level = "debug", skip(err))]
    pub fn vector<E: StdError>(err: E) -> Self {
        Self::VectorIndex(err.to_string())
    }
}
