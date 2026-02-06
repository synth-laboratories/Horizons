use crate::{AgentRunResult, HorizonsClient, HorizonsError};
use futures_util::Stream;
use reqwest::Method;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct AgentsApi {
    client: HorizonsClient,
}

impl AgentsApi {
    pub(crate) fn new(client: HorizonsClient) -> Self {
        Self { client }
    }

    pub async fn run(
        &self,
        agent_id: impl Into<String>,
        inputs: Option<std::collections::HashMap<String, Value>>,
        project_id: Option<Uuid>,
    ) -> Result<AgentRunResult, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            agent_id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            inputs: Option<std::collections::HashMap<String, Value>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<String>,
        }
        let body = Body {
            agent_id: agent_id.into(),
            inputs,
            project_id: project_id.map(|u| u.to_string()),
        };
        let v = self
            .client
            .request_value(Method::POST, "/api/v1/agents/run", None::<&()>, Some(&body))
            .await?;
        let result = v
            .get("result")
            .ok_or_else(|| crate::HorizonsError::new(crate::HorizonsErrorKind::Serialization, None, "missing result"))?;
        Ok(serde_json::from_value::<AgentRunResult>(result.clone())?)
    }

    pub fn chat_stream(
        &self,
        agent_id: impl Into<String>,
        inputs: Option<std::collections::HashMap<String, Value>>,
        project_id: Option<Uuid>,
    ) -> impl Stream<Item = Result<Value, HorizonsError>> + Send + 'static {
        #[derive(Serialize)]
        struct Body {
            agent_id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            inputs: Option<std::collections::HashMap<String, Value>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<String>,
        }
        let body = Body {
            agent_id: agent_id.into(),
            inputs,
            project_id: project_id.map(|u| u.to_string()),
        };
        self.client.sse_post("/api/v1/agents/chat", body)
    }

    pub async fn list_registered(&self) -> Result<Vec<String>, HorizonsError> {
        self.client
            .request_json(Method::GET, "/api/v1/agents", None::<&()>, None::<&()>)
            .await
    }
}

