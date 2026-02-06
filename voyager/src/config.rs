use crate::{Result, VoyagerError};

#[derive(Debug, Clone)]
pub struct RetrievalConfig {
    /// Weight for vector relevance in [0,1].
    pub relevance_weight: f32,
    /// Weight for recency in [0,1].
    pub recency_weight: f32,
    /// Weight for item importance in [0,1].
    pub importance_weight: f32,
    /// Exponential half-life for recency scoring.
    pub recency_half_life_ms: u64,
    /// Number of candidates requested from the vector index before applying recency scoring.
    pub candidate_pool_size: usize,
}

impl Default for RetrievalConfig {
    fn default() -> Self {
        Self {
            relevance_weight: 0.7,
            recency_weight: 0.2,
            importance_weight: 0.1,
            recency_half_life_ms: 7 * 24 * 60 * 60 * 1000,
            candidate_pool_size: 50,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SummarizationConfig {
    /// Maximum number of items included in a single summary batch.
    pub max_items: usize,
    /// Minimum number of eligible items required to produce a summary.
    pub min_items: usize,
}

impl Default for SummarizationConfig {
    fn default() -> Self {
        Self {
            max_items: 100,
            min_items: 8,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HelixHttpConfig {
    /// Base URL for Helix (e.g. http://localhost:8787).
    pub base_url: String,
    /// Path for Cypher queries (default: /query).
    pub query_path: String,
    /// Request timeout.
    pub timeout_ms: u64,
}

impl Default for HelixHttpConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            query_path: "/api/v1/query".to_string(),
            timeout_ms: 10_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VectorHttpConfig {
    /// Base URL for the vector index service.
    pub base_url: String,
    pub upsert_path: String,
    pub search_path: String,
    pub timeout_ms: u64,
}

impl Default for VectorHttpConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            upsert_path: "/upsert".to_string(),
            search_path: "/search".to_string(),
            timeout_ms: 10_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VoyagerConfig {
    pub helix: HelixHttpConfig,
    pub vector: VectorHttpConfig,
    pub retrieval: RetrievalConfig,
    pub summarization: SummarizationConfig,
}

impl Default for VoyagerConfig {
    fn default() -> Self {
        Self {
            helix: HelixHttpConfig::default(),
            vector: VectorHttpConfig::default(),
            retrieval: RetrievalConfig::default(),
            summarization: SummarizationConfig::default(),
        }
    }
}

impl VoyagerConfig {
    #[tracing::instrument]
    pub fn validate(&self) -> Result<()> {
        if self.helix.base_url.trim().is_empty() {
            return Err(VoyagerError::InvalidConfig(
                "helix.base_url is required".to_string(),
            ));
        }
        if self.vector.base_url.trim().is_empty() {
            return Err(VoyagerError::InvalidConfig(
                "vector.base_url is required".to_string(),
            ));
        }
        for (name, w) in [
            (
                "retrieval.relevance_weight",
                self.retrieval.relevance_weight,
            ),
            ("retrieval.recency_weight", self.retrieval.recency_weight),
            (
                "retrieval.importance_weight",
                self.retrieval.importance_weight,
            ),
        ] {
            if !w.is_finite() || w < 0.0 || w > 1.0 {
                return Err(VoyagerError::InvalidConfig(format!(
                    "{name} must be finite and in [0,1]"
                )));
            }
        }
        if self.retrieval.relevance_weight == 0.0
            && self.retrieval.recency_weight == 0.0
            && self.retrieval.importance_weight == 0.0
        {
            return Err(VoyagerError::InvalidConfig(
                "retrieval weights must not all be zero".to_string(),
            ));
        }
        if self.retrieval.recency_half_life_ms == 0 {
            return Err(VoyagerError::InvalidConfig(
                "retrieval.recency_half_life_ms must be > 0".to_string(),
            ));
        }
        if self.retrieval.candidate_pool_size == 0 {
            return Err(VoyagerError::InvalidConfig(
                "retrieval.candidate_pool_size must be > 0".to_string(),
            ));
        }
        if self.summarization.max_items == 0 {
            return Err(VoyagerError::InvalidConfig(
                "summarization.max_items must be > 0".to_string(),
            ));
        }
        if self.summarization.min_items == 0 {
            return Err(VoyagerError::InvalidConfig(
                "summarization.min_items must be > 0".to_string(),
            ));
        }
        if self.summarization.min_items > self.summarization.max_items {
            return Err(VoyagerError::InvalidConfig(
                "summarization.min_items must be <= summarization.max_items".to_string(),
            ));
        }
        Ok(())
    }
}
