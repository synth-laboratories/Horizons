//! Encrypted credential store with token resolution.
//!
//! `CredentialManager` encrypts credentials at rest using AES-256-GCM and
//! resolves `$cred:connector_id.key` tokens in arbitrary JSON values.

use crate::models::OrgId;
use crate::onboard::models::ConnectorCredential;
use crate::onboard::traits::CentralDb;
use crate::{Error, Result};
use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use chrono::Utc;
use rand::RngCore;
use std::path::Path;
use std::sync::Arc;

const NONCE_LEN: usize = 12;

/// Manages encrypted connector credentials stored in `CentralDb`.
#[derive(Clone)]
pub struct CredentialManager {
    central_db: Arc<dyn CentralDb>,
    cipher: Aes256Gcm,
}

impl CredentialManager {
    /// Create a new manager from a CentralDb and a 32-byte master key.
    pub fn new(central_db: Arc<dyn CentralDb>, master_key: &[u8; 32]) -> Self {
        let cipher =
            Aes256Gcm::new_from_slice(master_key).expect("32-byte key is always valid for AES-256");
        Self { central_db, cipher }
    }

    /// Load the master key from `path`, or generate one if it doesn't exist.
    pub fn generate_or_load_key(path: &Path) -> std::io::Result<[u8; 32]> {
        if path.exists() {
            let bytes = std::fs::read(path)?;
            if bytes.len() != 32 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "master key file must be exactly 32 bytes, got {}",
                        bytes.len()
                    ),
                ));
            }
            let mut key = [0u8; 32];
            key.copy_from_slice(&bytes);
            Ok(key)
        } else {
            let mut key = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut key);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, &key)?;
            tracing::info!(path = %path.display(), "generated new master key");
            Ok(key)
        }
    }

    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| Error::BackendMessage(format!("encrypt: {e}")))?;
        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.len() < NONCE_LEN {
            return Err(Error::BackendMessage(
                "ciphertext too short (missing nonce)".to_string(),
            ));
        }
        let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
        let nonce = Nonce::from_slice(nonce_bytes);
        self.cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| Error::BackendMessage(format!("decrypt: {e}")))
    }

    /// Store credentials for a connector (encrypts before persisting).
    pub async fn store(
        &self,
        org_id: OrgId,
        connector_id: &str,
        creds: serde_json::Value,
    ) -> Result<()> {
        let plaintext = serde_json::to_vec(&creds)
            .map_err(|e| Error::BackendMessage(format!("serialize credentials: {e}")))?;
        let ciphertext = self.encrypt(&plaintext)?;
        let now = Utc::now();
        let credential = ConnectorCredential {
            org_id,
            connector_id: connector_id.to_string(),
            ciphertext,
            created_at: now,
            updated_at: now,
        };
        self.central_db
            .upsert_connector_credential(&credential)
            .await
    }

    /// Retrieve and decrypt credentials for a connector.
    pub async fn retrieve(
        &self,
        org_id: OrgId,
        connector_id: &str,
    ) -> Result<Option<serde_json::Value>> {
        let cred = self
            .central_db
            .get_connector_credential(org_id, connector_id)
            .await?;
        match cred {
            None => Ok(None),
            Some(c) => {
                let plaintext = self.decrypt(&c.ciphertext)?;
                let val: serde_json::Value = serde_json::from_slice(&plaintext)
                    .map_err(|e| Error::BackendMessage(format!("deserialize credentials: {e}")))?;
                Ok(Some(val))
            }
        }
    }

    /// Delete credentials for a connector.
    pub async fn delete(&self, org_id: OrgId, connector_id: &str) -> Result<()> {
        self.central_db
            .delete_connector_credential(org_id, connector_id)
            .await
    }

    /// List all connector IDs that have stored credentials.
    pub async fn list_connector_ids(&self, org_id: OrgId) -> Result<Vec<String>> {
        let creds = self.central_db.list_connector_credentials(org_id).await?;
        Ok(creds.into_iter().map(|c| c.connector_id).collect())
    }

    /// Resolve `$cred:connector_id.key` tokens in a JSON value tree.
    ///
    /// Walks the value recursively. Any string matching `$cred:X.Y` is replaced
    /// with the decrypted value of connector `X`, key `Y`. Non-matching strings
    /// and non-string values are returned as-is.
    pub fn resolve<'a>(
        &'a self,
        org_id: OrgId,
        settings: &'a serde_json::Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value>> + Send + 'a>>
    {
        Box::pin(async move {
            match settings {
                serde_json::Value::String(s) => {
                    if let Some(rest) = s.strip_prefix("$cred:") {
                        if let Some(dot) = rest.find('.') {
                            let connector_id = &rest[..dot];
                            let key = &rest[dot + 1..];
                            let creds =
                                self.retrieve(org_id, connector_id).await?.ok_or_else(|| {
                                    Error::NotFound(format!(
                                        "credentials for connector '{connector_id}' not found"
                                    ))
                                })?;
                            let resolved =
                                creds.get(key).cloned().unwrap_or(serde_json::Value::Null);
                            return Ok(resolved);
                        }
                    }
                    Ok(settings.clone())
                }
                serde_json::Value::Object(map) => {
                    let mut out = serde_json::Map::with_capacity(map.len());
                    for (k, v) in map {
                        out.insert(k.clone(), self.resolve(org_id, v).await?);
                    }
                    Ok(serde_json::Value::Object(out))
                }
                serde_json::Value::Array(arr) => {
                    let mut out = Vec::with_capacity(arr.len());
                    for v in arr {
                        out.push(self.resolve(org_id, v).await?);
                    }
                    Ok(serde_json::Value::Array(out))
                }
                _ => Ok(settings.clone()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let key = [42u8; 32];
        let cipher = Aes256Gcm::new_from_slice(&key).unwrap();
        // Test encrypt/decrypt without needing a real CentralDb.
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let plaintext = b"hello world";
        let ciphertext = cipher.encrypt(nonce, plaintext.as_ref()).unwrap();

        let mut combined = Vec::new();
        combined.extend_from_slice(&nonce_bytes);
        combined.extend_from_slice(&ciphertext);

        // Decrypt.
        let (n, ct) = combined.split_at(NONCE_LEN);
        let decrypted = cipher.decrypt(Nonce::from_slice(n), ct).unwrap();
        assert_eq!(decrypted, plaintext);
    }
}
