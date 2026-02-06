use crate::{OrgId, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub use ulid::Ulid;
pub use voyager::{MemoryItem, MemoryType, RetrievalQuery, Scope, Summary};
pub use voyager::{VoyagerError, VoyagerMemory};

/// Tenant-scoped, agent-scoped convenience wrapper around `voyager::VoyagerMemory`.
#[async_trait]
pub trait HorizonsMemory: Send + Sync {
    async fn append_item(&self, org_id: OrgId, item: MemoryItem) -> Result<Ulid>;

    async fn append(
        &self,
        org_id: OrgId,
        agent_id: &str,
        item_type: MemoryType,
        content: serde_json::Value,
        created_at: DateTime<Utc>,
    ) -> Result<Ulid>;

    async fn retrieve(
        &self,
        org_id: OrgId,
        agent_id: &str,
        query: RetrievalQuery,
    ) -> Result<Vec<MemoryItem>>;

    async fn summarize(&self, org_id: OrgId, agent_id: &str, horizon: &str) -> Result<Summary>;
}
