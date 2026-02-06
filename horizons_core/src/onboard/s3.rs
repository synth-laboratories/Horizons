use crate::models::{OrgId, ProjectId};
use crate::onboard::config::S3Config;
use crate::onboard::traits::Filestore;
use crate::{Error, Result};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;

#[derive(Clone)]
pub struct S3Filestore {
    client: Client,
    bucket: String,
    prefix: Option<String>,
}

impl S3Filestore {
    #[tracing::instrument(level = "debug", skip(cfg))]
    pub async fn new(cfg: &S3Config) -> Result<Self> {
        let creds = Credentials::new(
            cfg.access_key_id.clone(),
            cfg.secret_access_key.clone(),
            None,
            None,
            "horizons_static",
        );

        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(cfg.region.clone()))
            .credentials_provider(creds);

        if let Some(endpoint) = &cfg.endpoint {
            loader = loader.endpoint_url(endpoint);
        }

        let shared = loader.load().await;
        let s3_cfg = aws_sdk_s3::Config::from(&shared);

        Ok(Self {
            client: Client::from_conf(s3_cfg),
            bucket: cfg.bucket.clone(),
            prefix: cfg.prefix.clone(),
        })
    }

    #[tracing::instrument(level = "debug")]
    fn validate_key(key: &str) -> Result<()> {
        if key.trim().is_empty() {
            return Err(Error::InvalidInput("filestore key is empty".to_string()));
        }
        if key.starts_with('/') {
            return Err(Error::InvalidInput(
                "filestore key must not start with '/'".to_string(),
            ));
        }
        if key.contains('\\') {
            return Err(Error::InvalidInput(
                "filestore key must not contain '\\\\'".to_string(),
            ));
        }
        if key.split('/').any(|seg| seg == "..") {
            return Err(Error::InvalidInput(
                "filestore key must not contain '..' segments".to_string(),
            ));
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn scoped_prefix(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key_prefix: &str,
    ) -> Result<String> {
        Self::validate_key(key_prefix)?;

        let mut out = String::new();
        if let Some(p) = &self.prefix {
            if !p.trim().is_empty() {
                out.push_str(p.trim().trim_matches('/'));
                out.push('/');
            }
        }
        out.push_str("org/");
        out.push_str(&org_id.to_string());
        out.push('/');
        match project_id {
            None => out.push_str("global/"),
            Some(pid) => {
                out.push_str("project/");
                out.push_str(&pid.to_string());
                out.push('/');
            }
        }
        out.push_str(key_prefix);
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn scoped_key(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
    ) -> Result<String> {
        Self::validate_key(key)?;
        self.scoped_prefix(org_id, project_id, key)
    }
}

#[async_trait]
impl Filestore for S3Filestore {
    #[tracing::instrument(level = "debug", skip(self, data))]
    async fn put(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
        data: Bytes,
    ) -> Result<()> {
        let object_key = self.scoped_key(org_id, project_id, key)?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(object_key)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| Error::backend("s3 put_object", e))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        key: &str,
    ) -> Result<Option<Bytes>> {
        let object_key = self.scoped_key(org_id, project_id, key)?;
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(object_key)
            .send()
            .await;

        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                // Normalize not-found to None.
                let msg = e.to_string();
                if msg.contains("NoSuchKey") || msg.contains("NotFound") {
                    return Ok(None);
                }
                return Err(Error::backend("s3 get_object", e));
            }
        };

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| Error::backend("s3 collect body", e))?
            .into_bytes();
        Ok(Some(Bytes::from(data)))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        prefix: &str,
    ) -> Result<Vec<String>> {
        let scoped_prefix = self.scoped_prefix(org_id, project_id, prefix)?;

        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&scoped_prefix);
            if let Some(t) = token.take() {
                req = req.continuation_token(t);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| Error::backend("s3 list_objects_v2", e))?;

            if let Some(contents) = resp.contents {
                for obj in contents {
                    if let Some(k) = obj.key {
                        keys.push(k);
                    }
                }
            }

            if resp.is_truncated.unwrap_or(false) {
                token = resp.next_continuation_token;
                if token.is_none() {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(keys)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete(&self, org_id: OrgId, project_id: Option<ProjectId>, key: &str) -> Result<()> {
        let object_key = self.scoped_key(org_id, project_id, key)?;
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(object_key)
            .send()
            .await
            .map_err(|e| Error::backend("s3 delete_object", e))?;
        Ok(())
    }
}
