use crate::config::VoyagerConfig;
use crate::models::{MemoryItem, MemoryType, RetrievalQuery, Scope, Summary};
use crate::retrieval::rerank;
use crate::store::{
    DynMemoryStore, DynVectorIndex, HelixCypherStore, HttpVectorIndex, StoreListFilter,
    VectorSearchFilter,
};
use crate::summarizer::{build_summary_item, parse_horizon, select_eligible_items_for_summary};
use crate::{Result, VoyagerError};
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashSet;
use std::sync::Arc;
use ulid::Ulid;

#[async_trait]
pub trait EmbeddingModel: Send + Sync {
    async fn embed(&self, scope: &Scope, text: &str) -> Result<Vec<f32>>;
    fn name(&self) -> &'static str;
}

#[async_trait]
pub trait ImportanceModel: Send + Sync {
    async fn score_importance_0_to_1(&self, scope: &Scope, item: &MemoryItem) -> Result<f32>;
    fn name(&self) -> &'static str;
}

pub struct VoyagerMemory {
    store: DynMemoryStore,
    vectors: DynVectorIndex,
    embedder: Arc<dyn EmbeddingModel>,
    summarizer: Arc<dyn crate::summarizer::SummarizationModel>,
    importance: Option<Arc<dyn ImportanceModel>>,
    cfg: VoyagerConfig,
}

impl VoyagerMemory {
    #[tracing::instrument(skip_all)]
    pub fn new(
        store: DynMemoryStore,
        vectors: DynVectorIndex,
        embedder: Arc<dyn EmbeddingModel>,
        summarizer: Arc<dyn crate::summarizer::SummarizationModel>,
        importance: Option<Arc<dyn ImportanceModel>>,
        cfg: VoyagerConfig,
    ) -> Self {
        Self {
            store,
            vectors,
            embedder,
            summarizer,
            importance,
            cfg,
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn config(&self) -> &VoyagerConfig {
        &self.cfg
    }

    #[tracing::instrument(skip_all)]
    pub fn store(&self) -> &DynMemoryStore {
        &self.store
    }

    #[tracing::instrument(skip_all)]
    pub fn vectors(&self) -> &DynVectorIndex {
        &self.vectors
    }

    #[tracing::instrument(skip_all)]
    pub fn embedder(&self) -> &Arc<dyn EmbeddingModel> {
        &self.embedder
    }

    #[tracing::instrument(skip_all)]
    pub fn summarizer(&self) -> &Arc<dyn crate::summarizer::SummarizationModel> {
        &self.summarizer
    }

    #[tracing::instrument(skip_all)]
    pub fn importance_model(&self) -> Option<&Arc<dyn ImportanceModel>> {
        self.importance.as_ref()
    }

    #[tracing::instrument(skip_all)]
    pub fn from_config(
        cfg: VoyagerConfig,
        embedder: Arc<dyn EmbeddingModel>,
        summarizer: Arc<dyn crate::summarizer::SummarizationModel>,
        importance: Option<Arc<dyn ImportanceModel>>,
    ) -> Result<Self> {
        cfg.validate()?;
        let store = Arc::new(HelixCypherStore::new(cfg.helix.clone())?) as DynMemoryStore;
        let vectors = Arc::new(HttpVectorIndex::new(cfg.vector.clone())?) as DynVectorIndex;
        Ok(Self::new(
            store, vectors, embedder, summarizer, importance, cfg,
        ))
    }

    #[tracing::instrument(skip_all)]
    pub async fn append(&self, mut item: MemoryItem) -> Result<Ulid> {
        if item.org_id.trim().is_empty() {
            return Err(VoyagerError::InvalidArgument(
                "org_id is required".to_string(),
            ));
        }
        if item.agent_id.trim().is_empty() {
            return Err(VoyagerError::InvalidArgument(
                "agent_id is required".to_string(),
            ));
        }

        let scope = item.scope();
        if item.importance_0_to_1.is_none() {
            if let Some(model) = self.importance.as_ref() {
                let s = model.score_importance_0_to_1(&scope, &item).await?;
                item.importance_0_to_1 = Some(s);
            }
        }

        let text = text_for_embedding(&item);
        let embedding = self.embedder.embed(&scope, &text).await?;

        self.store.append(item.clone()).await?;
        self.vectors.upsert(&scope, &item, embedding).await?;
        Ok(item.id)
    }

    #[tracing::instrument(skip_all)]
    pub async fn append_text(
        &self,
        scope: &Scope,
        item_type: MemoryType,
        text: impl Into<String>,
    ) -> Result<Ulid> {
        let item = MemoryItem::new(
            scope,
            item_type,
            serde_json::Value::String(text.into()),
            Utc::now(),
        );
        self.append(item).await
    }

    #[tracing::instrument(skip_all)]
    pub async fn retrieve(&self, scope: &Scope, query: RetrievalQuery) -> Result<Vec<MemoryItem>> {
        if scope.org_id.trim().is_empty() || scope.agent_id.trim().is_empty() {
            return Err(VoyagerError::InvalidArgument(
                "scope org_id and agent_id are required".to_string(),
            ));
        }
        if query.limit == 0 {
            return Err(VoyagerError::InvalidArgument(
                "query.limit must be > 0".to_string(),
            ));
        }

        let query_embedding = self
            .embedder
            .embed(scope, query.embedding_text().trim())
            .await?;

        let pool = self.cfg.retrieval.candidate_pool_size.max(query.limit);
        let vector_filter = VectorSearchFilter {
            type_filter: query.type_filter.clone(),
            include_episode_summaries: query.include_episode_summaries,
            created_after: query.created_after,
            created_before: query.created_before,
            limit: pool,
        };

        let matches = self
            .vectors
            .search(scope, query_embedding, &vector_filter)
            .await?;

        let mut pairs = Vec::with_capacity(matches.len());
        for m in matches {
            if let Some(item) = self.store.get(scope, m.id).await? {
                pairs.push((item, m));
            }
        }

        let now = Utc::now();
        let reranked = rerank(pairs, now, &self.cfg.retrieval);
        Ok(reranked
            .into_iter()
            .take(query.limit)
            .map(|s| s.item)
            .collect())
    }

    #[tracing::instrument(skip_all)]
    pub async fn summarize(&self, scope: &Scope, horizon: &str) -> Result<Summary> {
        if scope.org_id.trim().is_empty() || scope.agent_id.trim().is_empty() {
            return Err(VoyagerError::InvalidArgument(
                "scope org_id and agent_id are required".to_string(),
            ));
        }

        let horizon_dur = parse_horizon(horizon)?;
        let now = Utc::now();

        let all = self
            .store
            .list(
                scope,
                &StoreListFilter {
                    include_episode_summaries: true,
                    ..StoreListFilter::default()
                },
            )
            .await?;

        // Exclude items already summarized (best-effort by reading summary item_ids).
        let already_summarized = summarized_item_ids(&all);
        let remaining: Vec<MemoryItem> = all
            .iter()
            .filter(|it| it.item_type != MemoryType::episode_summary())
            .filter(|it| !already_summarized.contains(&it.id))
            .cloned()
            .collect();

        let (horizon_start, horizon_end, candidates) = select_eligible_items_for_summary(
            &remaining,
            now,
            horizon_dur,
            &self.cfg.summarization,
        );

        if candidates.len() < self.cfg.summarization.min_items {
            return Err(VoyagerError::InvalidArgument(format!(
                "not enough eligible items to summarize (need {}, have {})",
                self.cfg.summarization.min_items,
                candidates.len()
            )));
        }

        let text = self.summarizer.summarize(scope, &candidates).await?;
        let item_ids = candidates.iter().map(|i| i.id).collect::<Vec<_>>();
        let built = build_summary_item(scope, now, horizon_start, horizon_end, item_ids, text);

        // Store the summary as a normal memory item.
        let _ = self.append(built.item).await?;
        Ok(built.summary)
    }
}

fn text_for_embedding(item: &MemoryItem) -> String {
    if item.item_type == MemoryType::episode_summary() {
        if let Some(v) = item.content.get("text") {
            if let Some(s) = v.as_str() {
                return s.to_string();
            }
        }
    }
    item.index_text_or_fallback()
}

fn summarized_item_ids(items: &[MemoryItem]) -> HashSet<Ulid> {
    let mut out = HashSet::new();
    for it in items
        .iter()
        .filter(|i| i.item_type == MemoryType::episode_summary())
    {
        let Some(arr) = it.content.get("item_ids").and_then(|v| v.as_array()) else {
            continue;
        };
        for v in arr {
            if let Some(s) = v.as_str() {
                if let Ok(id) = s.parse::<Ulid>() {
                    out.insert(id);
                }
            }
        }
    }
    out
}
