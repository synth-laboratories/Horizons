use crate::{HorizonsClient, HorizonsError, OptimizationRunRow};
use reqwest::Method;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct OptimizationApi {
    client: HorizonsClient,
}

impl OptimizationApi {
    pub(crate) fn new(client: HorizonsClient) -> Self {
        Self { client }
    }

    pub async fn run(
        &self,
        cfg: std::collections::HashMap<String, Value>,
        initial_policy: std::collections::HashMap<String, Value>,
        dataset: std::collections::HashMap<String, Value>,
        project_id: Option<Uuid>,
    ) -> Result<OptimizationRunRow, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            cfg: std::collections::HashMap<String, Value>,
            initial_policy: std::collections::HashMap<String, Value>,
            dataset: std::collections::HashMap<String, Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<String>,
        }
        let body = Body {
            cfg,
            initial_policy,
            dataset,
            project_id: project_id.map(|u| u.to_string()),
        };
        self.client
            .request_json(Method::POST, "/api/v1/optimization/run", None::<&()>, Some(&body))
            .await
    }

    pub async fn status(
        &self,
        project_id: Option<Uuid>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<OptimizationRunRow>, HorizonsError> {
        #[derive(Serialize)]
        struct Query {
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<String>,
            limit: i64,
            offset: i64,
        }
        let q = Query {
            project_id: project_id.map(|u| u.to_string()),
            limit,
            offset,
        };
        self.client
            .request_json(
                Method::GET,
                "/api/v1/optimization/status",
                Some(&q),
                None::<&()>,
            )
            .await
    }

    pub async fn reports(
        &self,
        project_id: Option<Uuid>,
        run_id: Option<Uuid>,
        limit: i64,
        offset: i64,
    ) -> Result<Value, HorizonsError> {
        #[derive(Serialize)]
        struct Query {
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            run_id: Option<String>,
            limit: i64,
            offset: i64,
        }
        let q = Query {
            project_id: project_id.map(|u| u.to_string()),
            run_id: run_id.map(|u| u.to_string()),
            limit,
            offset,
        };
        self.client
            .request_value(
                Method::GET,
                "/api/v1/optimization/reports",
                Some(&q),
                None::<&()>,
            )
            .await
    }
}

