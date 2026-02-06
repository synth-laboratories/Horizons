//! PostgreSQL + pgvector implementation of the `VectorStore` trait.
//!
//! This is feature-gated behind `horizons_integrations/pgvector`.

use async_trait::async_trait;
use horizons_core::models::{OrgId, ProjectId};
use horizons_core::onboard::models::VectorMatch;
use horizons_core::onboard::traits::VectorStore;
use horizons_core::{Error, Result};
use sqlx::{PgPool, Row};
use std::fmt::Write as _;
use tokio::sync::OnceCell;

const CENTRAL_PROJECT_SENTINEL: &str = "_central";

#[derive(Clone)]
pub struct PgVectorStore {
    pool: PgPool,
    dimension: usize,
    initialized: OnceCell<()>,
}

impl PgVectorStore {
    #[tracing::instrument(level = "debug", skip(pool))]
    pub fn new(pool: PgPool, dimension: usize) -> Self {
        Self {
            pool,
            dimension,
            initialized: OnceCell::new(),
        }
    }

    fn project_key(project_id: Option<ProjectId>) -> String {
        project_id
            .map(|p| p.to_string())
            .unwrap_or_else(|| CENTRAL_PROJECT_SENTINEL.to_string())
    }

    fn embedding_literal(dimension: usize, embedding: &[f32]) -> Result<String> {
        if embedding.len() != dimension {
            return Err(Error::InvalidInput(format!(
                "embedding dimension mismatch: expected {dimension}, got {}",
                embedding.len()
            )));
        }

        // pgvector accepts a string literal like "[1,2,3]" for the vector type.
        // Use a consistent formatting to avoid locale issues.
        let mut s = String::new();
        s.push('[');
        for (i, v) in embedding.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            // 8 dp is plenty for embeddings and keeps payload size reasonable.
            write!(&mut s, "{v:.8}").map_err(|e| Error::backend("format embedding literal", e))?;
        }
        s.push(']');
        Ok(s)
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn ensure_schema(&self) -> Result<()> {
        self.initialized
            .get_or_try_init(|| async {
                let dim = self.dimension;
                let ddl = format!(
                    r#"
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS horizons_vectors (
    org_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    collection TEXT NOT NULL,
    id TEXT NOT NULL,
    embedding vector({dim}),
    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_id, project_id, collection, id)
);
CREATE INDEX IF NOT EXISTS idx_vectors_embedding
    ON horizons_vectors USING ivfflat (embedding vector_cosine_ops);
"#
                );

                for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                    sqlx::query(stmt)
                        .execute(&self.pool)
                        .await
                        .map_err(|e| Error::backend("pgvector migrate", e))?;
                }

                Ok(())
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl VectorStore for PgVectorStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn upsert(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        collection: &str,
        id: &str,
        embedding: Vec<f32>,
        metadata: serde_json::Value,
    ) -> Result<()> {
        self.ensure_schema().await?;

        let project_key = Self::project_key(project_id);
        let embedding = Self::embedding_literal(self.dimension, &embedding)?;

        let sql = r#"
INSERT INTO horizons_vectors
  (org_id, project_id, collection, id, embedding, metadata)
VALUES
  ($1, $2, $3, $4, $5::vector, $6::jsonb)
ON CONFLICT (org_id, project_id, collection, id) DO UPDATE
  SET embedding = EXCLUDED.embedding,
      metadata = EXCLUDED.metadata,
      updated_at = NOW()
"#;

        sqlx::query(sql)
            .bind(org_id.to_string())
            .bind(project_key)
            .bind(collection)
            .bind(id)
            .bind(embedding)
            .bind(metadata)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("pgvector upsert", e))?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn search(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        collection: &str,
        embedding: Vec<f32>,
        limit: usize,
        filter: Option<serde_json::Value>,
    ) -> Result<Vec<VectorMatch>> {
        self.ensure_schema().await?;

        let project_key = Self::project_key(project_id);
        let query_vec = Self::embedding_literal(self.dimension, &embedding)?;
        let limit_i64: i64 = limit
            .try_into()
            .map_err(|_| Error::InvalidInput("limit too large".to_string()))?;

        // pgvector cosine operator (<=>) returns a distance; convert to similarity score.
        let (sql, rows) = if let Some(filter) = filter {
            let sql = r#"
SELECT id,
       (1.0 - (embedding <=> $1::vector))::float4 AS score,
       metadata
  FROM horizons_vectors
 WHERE org_id = $2
   AND project_id = $3
   AND collection = $4
   AND metadata @> $5::jsonb
 ORDER BY (embedding <=> $1::vector) ASC
 LIMIT $6
"#;
            let rows = sqlx::query(sql)
                .bind(&query_vec)
                .bind(org_id.to_string())
                .bind(&project_key)
                .bind(collection)
                .bind(filter)
                .bind(limit_i64)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Error::backend("pgvector search", e))?;
            (sql, rows)
        } else {
            let sql = r#"
SELECT id,
       (1.0 - (embedding <=> $1::vector))::float4 AS score,
       metadata
  FROM horizons_vectors
 WHERE org_id = $2
   AND project_id = $3
   AND collection = $4
 ORDER BY (embedding <=> $1::vector) ASC
 LIMIT $5
"#;
            let rows = sqlx::query(sql)
                .bind(&query_vec)
                .bind(org_id.to_string())
                .bind(&project_key)
                .bind(collection)
                .bind(limit_i64)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Error::backend("pgvector search", e))?;
            (sql, rows)
        };

        let _ = sql; // help trace spans; keep `sql` available for debugger.

        let mut out = Vec::new();
        for row in rows {
            let id: String = row.try_get("id").map_err(|e| Error::backend("id", e))?;
            let score: f32 = row
                .try_get("score")
                .map_err(|e| Error::backend("score", e))?;
            let metadata: serde_json::Value = row
                .try_get("metadata")
                .map_err(|e| Error::backend("metadata", e))?;
            out.push(VectorMatch {
                id,
                score,
                metadata,
            });
        }
        Ok(out)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn delete(
        &self,
        org_id: OrgId,
        project_id: Option<ProjectId>,
        collection: &str,
        id: &str,
    ) -> Result<()> {
        self.ensure_schema().await?;

        let project_key = Self::project_key(project_id);
        let sql = r#"
DELETE FROM horizons_vectors
 WHERE org_id = $1
   AND project_id = $2
   AND collection = $3
   AND id = $4
"#;
        sqlx::query(sql)
            .bind(org_id.to_string())
            .bind(project_key)
            .bind(collection)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::backend("pgvector delete", e))?;
        Ok(())
    }
}
