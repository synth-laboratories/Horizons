use crate::{HorizonsClient, HorizonsError, PipelineRun};
use reqwest::Method;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct PipelinesApi {
    client: HorizonsClient,
}

impl PipelinesApi {
    pub(crate) fn new(client: HorizonsClient) -> Self {
        Self { client }
    }

    pub async fn run(&self, spec: Value, inputs: Option<Value>) -> Result<PipelineRun, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            spec: Value,
            inputs: Value,
        }
        let body = Body {
            spec,
            inputs: inputs.unwrap_or_else(|| Value::Object(Default::default())),
        };
        let v = self
            .client
            .request_value(Method::POST, "/api/v1/pipelines/run", None::<&()>, Some(&body))
            .await?;
        Ok(serde_json::from_value::<PipelineRun>(v)?)
    }

    pub async fn get_run(&self, run_id: impl Into<String>) -> Result<PipelineRun, HorizonsError> {
        let run_id = run_id.into();
        self.client
            .request_json(
                Method::GET,
                &format!("/api/v1/pipelines/runs/{run_id}"),
                None::<&()>,
                None::<&()>,
            )
            .await
    }

    pub async fn approve(
        &self,
        run_id: impl Into<String>,
        step_id: impl Into<String>,
    ) -> Result<Value, HorizonsError> {
        let run_id = run_id.into();
        let step_id = step_id.into();
        self.client
            .request_value(
                Method::POST,
                &format!("/api/v1/pipelines/runs/{run_id}/approve/{step_id}"),
                None::<&()>,
                Some(&serde_json::json!({})),
            )
            .await
    }

    pub async fn cancel(&self, run_id: impl Into<String>) -> Result<Value, HorizonsError> {
        let run_id = run_id.into();
        self.client
            .request_value(
                Method::POST,
                &format!("/api/v1/pipelines/runs/{run_id}/cancel"),
                None::<&()>,
                Some(&serde_json::json!({})),
            )
            .await
    }
}

