use crate::{EvalReportRow, HorizonsClient, HorizonsError};
use reqwest::Method;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct EvaluationApi {
    client: HorizonsClient,
}

impl EvaluationApi {
    pub(crate) fn new(client: HorizonsClient) -> Self {
        Self { client }
    }

    pub async fn run(
        &self,
        case: std::collections::HashMap<String, Value>,
        project_id: Option<Uuid>,
    ) -> Result<EvalReportRow, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            case: std::collections::HashMap<String, Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<String>,
        }
        let body = Body {
            case,
            project_id: project_id.map(|u| u.to_string()),
        };
        self.client
            .request_json(Method::POST, "/api/v1/eval/run", None::<&()>, Some(&body))
            .await
    }

    pub async fn reports(
        &self,
        project_id: Option<Uuid>,
        report_id: Option<Uuid>,
        limit: i64,
        offset: i64,
    ) -> Result<Value, HorizonsError> {
        #[derive(Serialize)]
        struct Query {
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            report_id: Option<String>,
            limit: i64,
            offset: i64,
        }
        let q = Query {
            project_id: project_id.map(|u| u.to_string()),
            report_id: report_id.map(|u| u.to_string()),
            limit,
            offset,
        };
        self.client
            .request_value(Method::GET, "/api/v1/eval/reports", Some(&q), None::<&()>)
            .await
    }
}
