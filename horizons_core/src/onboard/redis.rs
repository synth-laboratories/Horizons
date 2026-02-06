use crate::models::{OrgId, ProjectId};
use crate::onboard::config::RedisConfig;
use crate::onboard::traits::{BytesStream, Cache};
use crate::{Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use std::time::Duration;

#[derive(Clone)]
pub struct RedisCache {
    client: redis::Client,
    manager: ConnectionManager,
    prefix: Option<String>,
}

impl RedisCache {
    #[tracing::instrument(level = "debug", skip(cfg))]
    pub async fn new(cfg: &RedisConfig) -> Result<Self> {
        let client = redis::Client::open(cfg.url.clone())
            .map_err(|e| Error::backend("redis client open", e))?;
        let manager = ConnectionManager::new(client.clone())
            .await
            .map_err(|e| Error::backend("redis connect", e))?;
        Ok(Self {
            client,
            manager,
            prefix: cfg.key_prefix.clone(),
        })
    }

    #[tracing::instrument(level = "debug")]
    fn validate_name(name: &str) -> Result<()> {
        if name.trim().is_empty() {
            return Err(Error::InvalidInput(
                "redis key/channel is empty".to_string(),
            ));
        }
        if name.contains(' ') {
            return Err(Error::InvalidInput(
                "redis key/channel must not contain spaces".to_string(),
            ));
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug")]
    fn scoped_name(
        prefix: Option<&str>,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        name: &str,
    ) -> Result<String> {
        Self::validate_name(name)?;
        let mut out = String::new();
        if let Some(p) = prefix {
            if !p.trim().is_empty() {
                out.push_str(p.trim());
                out.push(':');
            }
        }
        out.push_str("org:");
        out.push_str(&org_id.to_string());
        out.push(':');
        match project_id {
            None => out.push_str("global:"),
            Some(pid) => {
                out.push_str("project:");
                out.push_str(&pid.to_string());
                out.push(':');
            }
        }
        out.push_str(name);
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn scoped(&self, org_id: OrgId, project_id: Option<ProjectId>, name: &str) -> Result<String> {
        Self::scoped_name(self.prefix.as_deref(), org_id, project_id, name)
    }
}

#[async_trait]
impl Cache for RedisCache {
    #[tracing::instrument(level = "debug", skip(self))]
    async fn get(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
    ) -> Result<Option<Bytes>> {
        let key = self.scoped(org_id, project_id, key)?;
        let mut conn = self.manager.clone();
        let val: Option<Vec<u8>> = conn
            .get(key)
            .await
            .map_err(|e| Error::backend("redis get", e))?;
        Ok(val.map(Bytes::from))
    }

    #[tracing::instrument(level = "debug", skip(self, value))]
    async fn set(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let key = self.scoped(org_id, project_id, key)?;
        let mut conn = self.manager.clone();
        if let Some(ttl) = ttl {
            let secs = ttl.as_secs().max(1);
            let _: () = conn
                .set_ex(key, value.to_vec(), secs)
                .await
                .map_err(|e| Error::backend("redis set_ex", e))?;
        } else {
            let _: () = conn
                .set(key, value.to_vec())
                .await
                .map_err(|e| Error::backend("redis set", e))?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn del(&self, org_id: OrgId, project_id: Option<ProjectId>, key: &str) -> Result<u64> {
        let key = self.scoped(org_id, project_id, key)?;
        let mut conn = self.manager.clone();
        let n: i64 = conn
            .del(key)
            .await
            .map_err(|e| Error::backend("redis del", e))?;
        Ok(n.max(0) as u64)
    }

    #[tracing::instrument(level = "debug", skip(self, message))]
    async fn publish(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        channel: &str,
        message: Bytes,
    ) -> Result<()> {
        let channel = self.scoped(org_id, project_id, channel)?;
        let mut conn = self.manager.clone();
        let _: i64 = conn
            .publish(channel, message.to_vec())
            .await
            .map_err(|e| Error::backend("redis publish", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn subscribe(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        channel: &str,
    ) -> Result<BytesStream> {
        let channel = self.scoped(org_id, project_id, channel)?;
        let mut pubsub = self
            .client
            .get_async_pubsub()
            .await
            .map_err(|e| Error::backend("redis pubsub connect", e))?;
        pubsub
            .subscribe(&channel)
            .await
            .map_err(|e| Error::backend("redis subscribe", e))?;

        let stream = pubsub
            .into_on_message()
            .map(|msg| Ok(Bytes::copy_from_slice(msg.get_payload_bytes())));
        Ok(Box::pin(stream))
    }
}
