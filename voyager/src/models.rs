use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MemoryType(pub String);

impl MemoryType {
    #[tracing::instrument]
    pub fn episode_summary() -> Self {
        Self("episode_summary".to_string())
    }

    #[tracing::instrument]
    pub fn tool_trace() -> Self {
        Self("tool_trace".to_string())
    }

    #[tracing::instrument]
    pub fn observation() -> Self {
        Self("observation".to_string())
    }

    #[tracing::instrument]
    pub fn outcome() -> Self {
        Self("outcome".to_string())
    }

    /// Inspired by Reflexion-style episodic reflection memory.
    #[tracing::instrument]
    pub fn reflection() -> Self {
        Self("reflection".to_string())
    }

    /// Inspired by Voyager-style skill library entries.
    #[tracing::instrument]
    pub fn skill() -> Self {
        Self("skill".to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Scope {
    pub org_id: String,
    pub agent_id: String,
}

impl Scope {
    #[tracing::instrument]
    pub fn new(
        org_id: impl Into<String> + std::fmt::Debug,
        agent_id: impl Into<String> + std::fmt::Debug,
    ) -> Self {
        Self {
            org_id: org_id.into(),
            agent_id: agent_id.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryItem {
    pub id: Ulid,
    pub org_id: String,
    pub agent_id: String,
    pub item_type: MemoryType,
    /// Optional text used for vector indexing. This allows storing non-textual `content`
    /// (e.g. code skills) while indexing on a natural-language description.
    #[serde(default)]
    pub index_text: Option<String>,
    /// Optional importance score in [0,1] used during retrieval reranking.
    /// This is commonly produced by a model at write time (Voyager/Reflexion patterns).
    #[serde(default)]
    pub importance_0_to_1: Option<f32>,
    pub content: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

impl MemoryItem {
    #[tracing::instrument(skip_all)]
    pub fn new(
        scope: &Scope,
        item_type: MemoryType,
        content: serde_json::Value,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id: Ulid::new(),
            org_id: scope.org_id.clone(),
            agent_id: scope.agent_id.clone(),
            item_type,
            index_text: None,
            importance_0_to_1: None,
            content,
            created_at,
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn with_index_text(mut self, index_text: impl Into<String>) -> Self {
        self.index_text = Some(index_text.into());
        self
    }

    #[tracing::instrument(skip_all)]
    pub fn with_importance(mut self, importance_0_to_1: f32) -> Self {
        self.importance_0_to_1 = Some(importance_0_to_1);
        self
    }

    #[tracing::instrument(skip_all)]
    pub fn scope(&self) -> Scope {
        Scope::new(self.org_id.clone(), self.agent_id.clone())
    }

    #[tracing::instrument(skip_all)]
    pub fn content_as_text(&self) -> String {
        match &self.content {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn index_text_or_fallback(&self) -> String {
        self.index_text
            .clone()
            .unwrap_or_else(|| self.content_as_text())
    }
}

#[derive(Debug, Clone)]
pub struct RetrievalQuery {
    pub query_text: String,
    /// Optional extra context appended to `query_text` for embedding.
    #[allow(dead_code)]
    pub context: Vec<String>,
    pub limit: usize,
    pub type_filter: Option<Vec<MemoryType>>,
    pub include_episode_summaries: bool,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
}

impl RetrievalQuery {
    #[tracing::instrument]
    pub fn new(query_text: impl Into<String> + std::fmt::Debug, limit: usize) -> Self {
        Self {
            query_text: query_text.into(),
            context: vec![],
            limit,
            type_filter: None,
            include_episode_summaries: true,
            created_after: None,
            created_before: None,
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn with_context(mut self, context: Vec<String>) -> Self {
        self.context = context;
        self
    }

    #[tracing::instrument(skip_all)]
    pub fn embedding_text(&self) -> String {
        if self.context.is_empty() {
            return self.query_text.clone();
        }
        format!("{}\n\n{}", self.query_text, self.context.join("\n"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub id: Ulid,
    pub org_id: String,
    pub agent_id: String,
    pub created_at: DateTime<Utc>,
    pub horizon_start: DateTime<Utc>,
    pub horizon_end: DateTime<Utc>,
    pub item_ids: Vec<Ulid>,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct VectorMatch {
    pub id: Ulid,
    pub similarity: f32,
    pub created_at: DateTime<Utc>,
    pub item_type: MemoryType,
}
