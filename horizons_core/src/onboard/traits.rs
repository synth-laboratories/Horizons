use crate::context_refresh::models::{RefreshRun, RefreshRunQuery, SourceConfig};
use crate::models::{AuditEntry, OrgId, PlatformConfig, ProjectDbHandle, ProjectId};
use crate::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use std::pin::Pin;
use std::time::Duration;
use uuid::Uuid;

pub use crate::onboard::models::{
    ApiKeyRecord, AuditQuery, ConnectorCredential, ListQuery, OperationRecord, OperationRunRecord,
    OrgRecord, ProjectDbParam, ProjectDbRow, ProjectDbRowExt, ProjectDbValue, ResourceRecord,
    SyncState, SyncStateKey, UserRecord, UserRole, VectorMatch, project_db_migrate,
};

/// Stream of bytes messages (e.g. Redis pub/sub).
pub type BytesStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + 'static>>;

/// Central platform database (Postgres). Stores platform-level entities for a tenant:
/// org/user records, configuration, audit log, connector credentials, and sync cursors.
#[async_trait]
pub trait CentralDb: Send + Sync {
    async fn upsert_org(&self, org: &OrgRecord) -> Result<()>;
    async fn get_org(&self, org_id: OrgId) -> Result<Option<OrgRecord>>;

    async fn upsert_user(&self, user: &UserRecord) -> Result<()>;
    async fn get_user(&self, org_id: OrgId, user_id: Uuid) -> Result<Option<UserRecord>>;
    async fn list_users(&self, org_id: OrgId, query: ListQuery) -> Result<Vec<UserRecord>>;

    // Authn: API keys (bearer tokens).
    async fn upsert_api_key(&self, key: &ApiKeyRecord) -> Result<()>;
    async fn get_api_key_by_id(&self, key_id: Uuid) -> Result<Option<ApiKeyRecord>>;
    async fn list_api_keys(&self, org_id: OrgId, query: ListQuery) -> Result<Vec<ApiKeyRecord>>;
    async fn delete_api_key(&self, org_id: OrgId, key_id: Uuid) -> Result<()>;
    async fn touch_api_key_last_used(
        &self,
        key_id: Uuid,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<()>;

    async fn get_platform_config(&self, org_id: OrgId) -> Result<Option<PlatformConfig>>;
    async fn set_platform_config(&self, config: &PlatformConfig) -> Result<()>;

    async fn append_audit_entry(&self, entry: &AuditEntry) -> Result<()>;
    async fn list_audit_entries(&self, org_id: OrgId, query: AuditQuery)
    -> Result<Vec<AuditEntry>>;

    async fn upsert_connector_credential(&self, credential: &ConnectorCredential) -> Result<()>;
    async fn get_connector_credential(
        &self,
        org_id: OrgId,
        connector_id: &str,
    ) -> Result<Option<ConnectorCredential>>;
    async fn delete_connector_credential(&self, org_id: OrgId, connector_id: &str) -> Result<()>;
    async fn list_connector_credentials(&self, org_id: OrgId) -> Result<Vec<ConnectorCredential>>;

    // Managed assets (Resources + Operations).
    async fn upsert_resource(&self, resource: &ResourceRecord) -> Result<()>;
    async fn get_resource(
        &self,
        org_id: OrgId,
        resource_id: &str,
    ) -> Result<Option<ResourceRecord>>;
    async fn list_resources(&self, org_id: OrgId, query: ListQuery) -> Result<Vec<ResourceRecord>>;

    async fn upsert_operation(&self, operation: &OperationRecord) -> Result<()>;
    async fn get_operation(
        &self,
        org_id: OrgId,
        operation_id: &str,
    ) -> Result<Option<OperationRecord>>;
    async fn list_operations(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> Result<Vec<OperationRecord>>;

    async fn append_operation_run(&self, run: &OperationRunRecord) -> Result<()>;
    async fn list_operation_runs(
        &self,
        org_id: OrgId,
        operation_id: Option<&str>,
        query: ListQuery,
    ) -> Result<Vec<OperationRunRecord>>;

    async fn upsert_sync_state(&self, state: &SyncState) -> Result<()>;
    async fn get_sync_state(&self, key: &SyncStateKey) -> Result<Option<SyncState>>;
    async fn delete_sync_state(&self, key: &SyncStateKey) -> Result<()>;

    // Context Refresh (System 1) metadata. Stored in the central DB for durable scheduling,
    // status tracking, and crash recovery.
    async fn upsert_source_config(&self, source: &SourceConfig) -> Result<()>;
    async fn get_source_config(
        &self,
        org_id: OrgId,
        source_id: &str,
    ) -> Result<Option<SourceConfig>>;
    async fn list_source_configs(
        &self,
        org_id: OrgId,
        query: ListQuery,
    ) -> Result<Vec<SourceConfig>>;
    async fn delete_source_config(&self, org_id: OrgId, source_id: &str) -> Result<()>;

    async fn upsert_refresh_run(&self, run: &RefreshRun) -> Result<()>;
    async fn list_refresh_runs(
        &self,
        org_id: OrgId,
        query: RefreshRunQuery,
    ) -> Result<Vec<RefreshRun>>;
}

/// Per-project database (Turso/libSQL). Horizons provisions an empty database per project.
/// Applications define their own schema and interact via raw SQL.
#[async_trait]
pub trait ProjectDb: Send + Sync {
    async fn provision(&self, org_id: OrgId, project_id: ProjectId) -> Result<ProjectDbHandle>;

    async fn get_handle(
        &self,
        org_id: OrgId,
        project_id: ProjectId,
    ) -> Result<Option<ProjectDbHandle>>;

    async fn list_projects(&self, org_id: OrgId, query: ListQuery) -> Result<Vec<ProjectDbHandle>>;

    async fn query(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        sql: &str,
        params: &[ProjectDbParam],
    ) -> Result<Vec<ProjectDbRow>>;

    async fn execute(
        &self,
        org_id: OrgId,
        handle: &ProjectDbHandle,
        sql: &str,
        params: &[ProjectDbParam],
    ) -> Result<u64>;
}

/// Object / file storage (S3-compatible).
#[async_trait]
pub trait Filestore: Send + Sync {
    async fn put(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
        data: Bytes,
    ) -> Result<()>;

    async fn get(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
    ) -> Result<Option<Bytes>>;

    async fn list(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        prefix: &str,
    ) -> Result<Vec<String>>;

    async fn delete(&self, org_id: OrgId, project_id: Option<ProjectId>, key: &str) -> Result<()>;
}

/// Cache + pub/sub (Redis). Keys are tenant scoped.
#[async_trait]
pub trait Cache: Send + Sync {
    async fn get(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
    ) -> Result<Option<Bytes>>;

    async fn set(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<()>;

    async fn del(&self, org_id: OrgId, project_id: Option<ProjectId>, key: &str) -> Result<u64>;

    async fn publish(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        channel: &str,
        message: Bytes,
    ) -> Result<()>;

    async fn subscribe(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        channel: &str,
    ) -> Result<BytesStream>;
}

/// Graph store (Helix). All reads/writes are tenant scoped.
#[async_trait]
pub trait GraphStore: Send + Sync {
    async fn query(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        cypher: &str,
        params: serde_json::Value,
    ) -> Result<Vec<serde_json::Value>>;

    async fn upsert_node(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        label: &str,
        identity: serde_json::Value,
        props: serde_json::Value,
    ) -> Result<String>;

    async fn upsert_edge(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        from: &str,
        to: &str,
        rel: &str,
        props: serde_json::Value,
    ) -> Result<()>;
}

/// Vector search index. All reads/writes are tenant scoped.
#[async_trait]
pub trait VectorStore: Send + Sync {
    async fn upsert(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        collection: &str,
        id: &str,
        embedding: Vec<f32>,
        metadata: serde_json::Value,
    ) -> Result<()>;

    async fn search(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        collection: &str,
        embedding: Vec<f32>,
        limit: usize,
        filter: Option<serde_json::Value>,
    ) -> Result<Vec<VectorMatch>>;

    async fn delete(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        collection: &str,
        id: &str,
    ) -> Result<()>;
}

/// Defensive helper for implementations that want to enforce handle scoping.
#[tracing::instrument(level = "debug")]
pub fn ensure_handle_org(handle: &ProjectDbHandle, org_id: OrgId) -> Result<()> {
    if handle.org_id != org_id {
        return Err(Error::Unauthorized(format!(
            "project db handle org_id {} does not match requested org_id {}",
            handle.org_id, org_id
        )));
    }
    Ok(())
}
