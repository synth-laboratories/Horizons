use crate::{HorizonsClient, HorizonsError};
use chrono::{DateTime, Utc};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpServerConfig {
    pub name: String,
    pub transport: McpTransport,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum McpTransport {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: HashMap<String, String>,
    },
    Http {
        url: String,
        #[serde(default)]
        headers: HashMap<String, String>,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpConfigResponse {
    pub ok: bool,
    pub server_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpToolsResponse {
    #[serde(default)]
    pub tools: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpToolResult {
    pub request_id: String,
    pub ok: bool,
    pub output: Value,
    pub finished_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct McpApi {
    client: HorizonsClient,
}

impl McpApi {
    pub(crate) fn new(client: HorizonsClient) -> Self {
        Self { client }
    }

    pub async fn configure(
        &self,
        servers: Vec<McpServerConfig>,
    ) -> Result<McpConfigResponse, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            servers: Vec<McpServerConfig>,
        }
        let body = Body { servers };
        self.client
            .request_json(Method::POST, "/api/v1/mcp/config", None::<&()>, Some(&body))
            .await
    }

    pub async fn list_tools(&self) -> Result<Vec<String>, HorizonsError> {
        let resp: McpToolsResponse = self
            .client
            .request_json(Method::GET, "/api/v1/mcp/tools", None::<&()>, None::<&()>)
            .await?;
        Ok(resp.tools)
    }

    pub async fn call(
        &self,
        tool_name: impl Into<String>,
        arguments: Option<Value>,
        request_id: Option<String>,
        requested_at: Option<DateTime<Utc>>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<McpToolResult, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            tool_name: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            arguments: Option<Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            request_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            requested_at: Option<DateTime<Utc>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            metadata: Option<HashMap<String, String>>,
        }

        let body = Body {
            tool_name: tool_name.into(),
            arguments,
            request_id,
            requested_at,
            metadata,
        };

        self.client
            .request_json(Method::POST, "/api/v1/mcp/call", None::<&()>, Some(&body))
            .await
    }
}
