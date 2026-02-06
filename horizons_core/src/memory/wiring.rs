use crate::onboard::traits::{GraphStore, VectorStore};
use crate::{Error, OrgId, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use ulid::Ulid;
use voyager::config::VoyagerConfig;
use voyager::store::{
    DynMemoryStore, DynVectorIndex, MemoryStore, StoreListFilter, VectorIndex, VectorSearchFilter,
};
use voyager::{MemoryItem, MemoryType, RetrievalQuery, Scope, Summary, VoyagerMemory};

use super::traits::HorizonsMemory;

const VOYAGER_VECTOR_COLLECTION_PREFIX: &str = "voyager_memory__";

fn parse_org_id(org_id: &str) -> std::result::Result<OrgId, Error> {
    org_id
        .parse()
        .map_err(|e| Error::InvalidInput(format!("invalid org_id {org_id}: {e:?}")))
}

fn sanitize_collection_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    out
}

fn vector_collection_for_agent(agent_id: &str) -> String {
    format!(
        "{VOYAGER_VECTOR_COLLECTION_PREFIX}{}",
        sanitize_collection_component(agent_id)
    )
}

#[derive(Clone)]
struct HelixGraphStoreAdapter {
    graph: Arc<dyn GraphStore>,
}

impl HelixGraphStoreAdapter {
    #[tracing::instrument(skip_all)]
    fn new(graph: Arc<dyn GraphStore>) -> Self {
        Self { graph }
    }

    #[tracing::instrument(skip_all)]
    async fn cypher(
        &self,
        org_id: OrgId,
        cypher: &str,
        params: serde_json::Value,
    ) -> std::result::Result<Vec<serde_json::Value>, Error> {
        self.graph.query(org_id, None, cypher, params).await
    }
}

#[async_trait]
impl MemoryStore for HelixGraphStoreAdapter {
    #[tracing::instrument(skip_all)]
    async fn append(&self, item: MemoryItem) -> voyager::Result<()> {
        let org_id =
            parse_org_id(&item.org_id).map_err(|e| voyager::VoyagerError::Store(e.to_string()))?;
        let item_json = serde_json::to_string(&item)?;
        let cypher = r#"
MERGE (o:Org {org_id: $org_id})
MERGE (a:Agent {org_id: $org_id, agent_id: $agent_id})
MERGE (a)-[:BELONGS_TO]->(o)
CREATE (m:MemoryItem {id: $id, org_id: $org_id, agent_id: $agent_id, item_type: $item_type, created_at: $created_at, item_json: $item_json})
CREATE (a)-[:HAS_MEMORY]->(m)
RETURN m.id as id
"#;
        let params = serde_json::json!({
            "id": item.id.to_string(),
            "org_id": item.org_id,
            "agent_id": item.agent_id,
            "item_type": item.item_type.0,
            "created_at": item.created_at.to_rfc3339(),
            "item_json": item_json,
        });
        self.cypher(org_id, cypher, params)
            .await
            .map_err(|e| voyager::VoyagerError::Store(e.to_string()))?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn get(&self, scope: &Scope, id: Ulid) -> voyager::Result<Option<MemoryItem>> {
        let org_id =
            parse_org_id(&scope.org_id).map_err(|e| voyager::VoyagerError::Store(e.to_string()))?;
        let cypher = r#"
MATCH (m:MemoryItem {id: $id, org_id: $org_id, agent_id: $agent_id})
RETURN m.item_json as item_json
LIMIT 1
"#;
        let params = serde_json::json!({
            "id": id.to_string(),
            "org_id": scope.org_id,
            "agent_id": scope.agent_id,
        });
        let rows = self
            .cypher(org_id, cypher, params)
            .await
            .map_err(|e| voyager::VoyagerError::Store(e.to_string()))?;

        if rows.is_empty() {
            return Ok(None);
        }
        let v = &rows[0];
        let item_json = v.get("item_json").and_then(|v| v.as_str()).ok_or_else(|| {
            voyager::VoyagerError::Store("helix response missing item_json".to_string())
        })?;
        let item: MemoryItem = serde_json::from_str(item_json)?;
        Ok(Some(item))
    }

    #[tracing::instrument(skip_all)]
    async fn list(
        &self,
        scope: &Scope,
        filter: &StoreListFilter,
    ) -> voyager::Result<Vec<MemoryItem>> {
        let org_id =
            parse_org_id(&scope.org_id).map_err(|e| voyager::VoyagerError::Store(e.to_string()))?;
        let cypher = r#"
MATCH (m:MemoryItem {org_id: $org_id, agent_id: $agent_id})
RETURN m.item_json as item_json
"#;
        let params = serde_json::json!({
            "org_id": scope.org_id,
            "agent_id": scope.agent_id,
        });
        let rows = self
            .cypher(org_id, cypher, params)
            .await
            .map_err(|e| voyager::VoyagerError::Store(e.to_string()))?;

        let mut items: Vec<MemoryItem> = Vec::with_capacity(rows.len());
        for row in rows {
            let Some(item_json) = row.get("item_json").and_then(|v| v.as_str()) else {
                continue;
            };
            let it: MemoryItem = serde_json::from_str(item_json)?;
            items.push(it);
        }

        // Apply filters client-side for maximum compatibility.
        items.retain(|it| {
            if !filter.include_episode_summaries && it.item_type == MemoryType::episode_summary() {
                return false;
            }
            if let Some(types) = &filter.type_filter {
                if !types.contains(&it.item_type) {
                    return false;
                }
            }
            if let Some(after) = filter.created_after.as_ref() {
                if it.created_at < *after {
                    return false;
                }
            }
            if let Some(before) = filter.created_before.as_ref() {
                if it.created_at > *before {
                    return false;
                }
            }
            true
        });
        items.sort_by_key(|it| std::cmp::Reverse(it.created_at));
        if let Some(limit) = filter.limit {
            items.truncate(limit);
        }
        Ok(items)
    }
}

#[derive(Clone)]
struct HorizonsVectorIndexAdapter {
    vectors: Arc<dyn VectorStore>,
}

impl HorizonsVectorIndexAdapter {
    #[tracing::instrument(skip_all)]
    fn new(vectors: Arc<dyn VectorStore>) -> Self {
        Self { vectors }
    }

    #[tracing::instrument(skip_all)]
    async fn upsert_inner(
        &self,
        org_id: OrgId,
        collection: &str,
        id: &str,
        embedding: Vec<f32>,
        metadata: serde_json::Value,
    ) -> std::result::Result<(), Error> {
        self.vectors
            .upsert(org_id, None, collection, id, embedding, metadata)
            .await
    }

    #[tracing::instrument(skip_all)]
    async fn search_inner(
        &self,
        org_id: OrgId,
        collection: &str,
        embedding: Vec<f32>,
        limit: usize,
    ) -> std::result::Result<Vec<crate::onboard::traits::VectorMatch>, Error> {
        self.vectors
            .search(org_id, None, collection, embedding, limit, None)
            .await
    }
}

#[async_trait]
impl VectorIndex for HorizonsVectorIndexAdapter {
    #[tracing::instrument(skip_all)]
    async fn upsert(
        &self,
        scope: &Scope,
        item: &MemoryItem,
        embedding: Vec<f32>,
    ) -> voyager::Result<()> {
        let org_id = parse_org_id(&scope.org_id)
            .map_err(|e| voyager::VoyagerError::VectorIndex(e.to_string()))?;
        let collection = vector_collection_for_agent(&scope.agent_id);

        let metadata = serde_json::json!({
            "created_at": item.created_at.to_rfc3339(),
            "item_type": item.item_type.0,
            "agent_id": item.agent_id,
        });

        self.upsert_inner(
            org_id,
            &collection,
            &item.id.to_string(),
            embedding,
            metadata,
        )
        .await
        .map_err(|e| voyager::VoyagerError::VectorIndex(e.to_string()))?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn search(
        &self,
        scope: &Scope,
        query_embedding: Vec<f32>,
        filter: &VectorSearchFilter,
    ) -> voyager::Result<Vec<voyager::VectorMatch>> {
        let org_id = parse_org_id(&scope.org_id)
            .map_err(|e| voyager::VoyagerError::VectorIndex(e.to_string()))?;
        let collection = vector_collection_for_agent(&scope.agent_id);

        // Over-fetch so client-side filtering doesn't accidentally drop everything.
        let fetch_limit = filter.limit.saturating_mul(10).max(filter.limit);
        let raw = self
            .search_inner(org_id, &collection, query_embedding, fetch_limit)
            .await
            .map_err(|e| voyager::VoyagerError::VectorIndex(e.to_string()))?;

        let mut out = vec![];
        for m in raw {
            let Ok(id) = m.id.parse::<Ulid>() else {
                continue;
            };
            let created_at = m
                .metadata
                .get("created_at")
                .and_then(|v| v.as_str())
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);
            let item_type = m
                .metadata
                .get("item_type")
                .and_then(|v| v.as_str())
                .map(|s| MemoryType(s.to_string()))
                .unwrap_or_else(MemoryType::observation);

            let vm = voyager::VectorMatch {
                id,
                similarity: m.score,
                created_at,
                item_type,
            };
            out.push(vm);
        }

        // Apply the voyager-side filter semantics.
        out.retain(|m| {
            if !filter.include_episode_summaries && m.item_type == MemoryType::episode_summary() {
                return false;
            }
            if let Some(types) = &filter.type_filter {
                if !types.contains(&m.item_type) {
                    return false;
                }
            }
            if let Some(after) = filter.created_after.as_ref() {
                if m.created_at < *after {
                    return false;
                }
            }
            if let Some(before) = filter.created_before.as_ref() {
                if m.created_at > *before {
                    return false;
                }
            }
            true
        });

        out.sort_by(|a, b| {
            b.similarity
                .partial_cmp(&a.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.created_at.cmp(&a.created_at))
                .then_with(|| b.id.cmp(&a.id))
        });
        out.truncate(filter.limit);
        Ok(out)
    }
}

pub struct VoyagerBackedHorizonsMemory {
    inner: VoyagerMemory,
}

impl VoyagerBackedHorizonsMemory {
    #[tracing::instrument(skip_all)]
    pub fn new(inner: VoyagerMemory) -> Self {
        Self { inner }
    }

    #[tracing::instrument(skip_all)]
    pub fn inner(&self) -> &VoyagerMemory {
        &self.inner
    }
}

#[async_trait]
impl HorizonsMemory for VoyagerBackedHorizonsMemory {
    #[tracing::instrument(skip_all)]
    async fn append_item(&self, org_id: OrgId, mut item: MemoryItem) -> Result<Ulid> {
        // Enforce tenant isolation at the boundary.
        item.org_id = org_id.to_string();
        let id = self
            .inner
            .append(item)
            .await
            .map_err(|e| Error::BackendMessage(format!("voyager append failed: {e}")))?;
        Ok(id)
    }

    #[tracing::instrument(skip_all)]
    async fn append(
        &self,
        org_id: OrgId,
        agent_id: &str,
        item_type: MemoryType,
        content: serde_json::Value,
        created_at: DateTime<Utc>,
    ) -> Result<Ulid> {
        let scope = Scope::new(org_id.to_string(), agent_id.to_string());
        let item = MemoryItem::new(&scope, item_type, content, created_at);
        self.append_item(org_id, item).await
    }

    #[tracing::instrument(skip_all)]
    async fn retrieve(
        &self,
        org_id: OrgId,
        agent_id: &str,
        query: RetrievalQuery,
    ) -> Result<Vec<MemoryItem>> {
        let scope = Scope::new(org_id.to_string(), agent_id.to_string());
        self.inner
            .retrieve(&scope, query)
            .await
            .map_err(|e| Error::BackendMessage(format!("voyager retrieve failed: {e}")))
    }

    #[tracing::instrument(skip_all)]
    async fn summarize(&self, org_id: OrgId, agent_id: &str, horizon: &str) -> Result<Summary> {
        let scope = Scope::new(org_id.to_string(), agent_id.to_string());
        self.inner
            .summarize(&scope, horizon)
            .await
            .map_err(|e| Error::BackendMessage(format!("voyager summarize failed: {e}")))
    }
}

/// Construct a `voyager::VoyagerMemory` that stores items in Helix via `GraphStore` and
/// uses `VectorStore` for similarity search.
#[tracing::instrument(skip_all)]
pub fn build_voyager_memory(
    graph: Arc<dyn GraphStore>,
    vectors: Arc<dyn VectorStore>,
    embedder: Arc<dyn voyager::EmbeddingModel>,
    summarizer: Arc<dyn voyager::SummarizationModel>,
    cfg: VoyagerConfig,
) -> VoyagerMemory {
    let store = Arc::new(HelixGraphStoreAdapter::new(graph)) as DynMemoryStore;
    let v = Arc::new(HorizonsVectorIndexAdapter::new(vectors)) as DynVectorIndex;
    VoyagerMemory::new(store, v, embedder, summarizer, None, cfg)
}
