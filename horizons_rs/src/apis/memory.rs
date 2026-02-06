use crate::{HorizonsClient, HorizonsError, MemoryItem, MemoryType, Summary};
use reqwest::Method;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct MemoryApi {
    client: HorizonsClient,
}

impl MemoryApi {
    pub(crate) fn new(client: HorizonsClient) -> Self {
        Self { client }
    }

    pub async fn retrieve(
        &self,
        agent_id: impl Into<String>,
        q: impl Into<String>,
        limit: i64,
    ) -> Result<Vec<MemoryItem>, HorizonsError> {
        #[derive(Serialize)]
        struct Query {
            agent_id: String,
            q: String,
            limit: i64,
        }
        let query = Query {
            agent_id: agent_id.into(),
            q: q.into(),
            limit,
        };
        self.client
            .request_json(Method::GET, "/api/v1/memory", Some(&query), None::<&()>)
            .await
    }

    pub async fn append(
        &self,
        agent_id: impl Into<String>,
        item_type: MemoryType,
        content: Value,
        index_text: Option<String>,
        importance_0_to_1: Option<f64>,
        created_at: Option<String>,
    ) -> Result<String, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            agent_id: String,
            item_type: MemoryType,
            content: Value,
            #[serde(skip_serializing_if = "Option::is_none")]
            index_text: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            importance_0_to_1: Option<f64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            created_at: Option<String>,
        }
        let body = Body {
            agent_id: agent_id.into(),
            item_type,
            content,
            index_text,
            importance_0_to_1,
            created_at,
        };
        let v = self
            .client
            .request_value(Method::POST, "/api/v1/memory", None::<&()>, Some(&body))
            .await?;
        Ok(v.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string())
    }

    pub async fn summarize(
        &self,
        agent_id: impl Into<String>,
        horizon: impl Into<String>,
    ) -> Result<Summary, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            agent_id: String,
            horizon: String,
        }
        let body = Body {
            agent_id: agent_id.into(),
            horizon: horizon.into(),
        };
        self.client
            .request_json(Method::POST, "/api/v1/memory/summarize", None::<&()>, Some(&body))
            .await
    }
}

