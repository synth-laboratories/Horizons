use crate::{HorizonsClient, HorizonsError};
use reqwest::Method;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ContextRefreshApi {
    client: HorizonsClient,
}

impl ContextRefreshApi {
    pub(crate) fn new(client: HorizonsClient) -> Self {
        Self { client }
    }

    pub async fn trigger(
        &self,
        source_id: impl Into<String>,
        trigger: Option<std::collections::HashMap<String, Value>>,
    ) -> Result<Value, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            source_id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            trigger: Option<std::collections::HashMap<String, Value>>,
        }
        let body = Body {
            source_id: source_id.into(),
            trigger,
        };
        self.client
            .request_value(
                Method::POST,
                "/api/v1/context-refresh/run",
                None::<&()>,
                Some(&body),
            )
            .await
    }

    pub async fn status(
        &self,
        source_id: Option<String>,
        limit: i64,
        offset: i64,
    ) -> Result<Value, HorizonsError> {
        #[derive(Serialize)]
        struct Query {
            #[serde(skip_serializing_if = "Option::is_none")]
            source_id: Option<String>,
            limit: i64,
            offset: i64,
        }
        let q = Query {
            source_id,
            limit,
            offset,
        };
        self.client
            .request_value(
                Method::GET,
                "/api/v1/context-refresh/status",
                Some(&q),
                None::<&()>,
            )
            .await
    }

    pub async fn register_source(
        &self,
        project_id: Uuid,
        source_id: impl Into<String>,
        connector_id: impl Into<String>,
        scope: impl Into<String>,
        enabled: bool,
        schedule_expr: Option<String>,
        event_triggers: Option<Vec<Value>>,
        settings: Option<std::collections::HashMap<String, Value>>,
    ) -> Result<Value, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            project_id: String,
            source_id: String,
            connector_id: String,
            scope: String,
            enabled: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            schedule_expr: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            event_triggers: Option<Vec<Value>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            settings: Option<std::collections::HashMap<String, Value>>,
        }
        let body = Body {
            project_id: project_id.to_string(),
            source_id: source_id.into(),
            connector_id: connector_id.into(),
            scope: scope.into(),
            enabled,
            schedule_expr,
            event_triggers,
            settings,
        };
        self.client
            .request_value(Method::POST, "/api/v1/connectors", None::<&()>, Some(&body))
            .await
    }

    pub async fn list_sources(&self) -> Result<Vec<Value>, HorizonsError> {
        let v = self
            .client
            .request_value(Method::GET, "/api/v1/connectors", None::<&()>, None::<&()>)
            .await?;
        Ok(v.get("sources")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default())
    }
}

