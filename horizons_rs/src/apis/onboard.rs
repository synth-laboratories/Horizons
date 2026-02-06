use crate::{HorizonsClient, HorizonsError, ProjectDbHandle};
use reqwest::Method;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct OnboardApi {
    client: HorizonsClient,
}

impl OnboardApi {
    pub(crate) fn new(client: HorizonsClient) -> Self {
        Self { client }
    }

    pub async fn create_project(&self, project_id: Option<Uuid>) -> Result<ProjectDbHandle, HorizonsError> {
        #[derive(Serialize)]
        struct Body<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<&'a str>,
        }

        let pid = project_id.map(|u| u.to_string());
        let body = Body {
            project_id: pid.as_deref(),
        };

        let v = self
            .client
            .request_value(Method::POST, "/api/v1/projects", None::<&()>, Some(&body))
            .await?;

        let handle = v
            .get("handle")
            .ok_or_else(|| HorizonsError::new(crate::HorizonsErrorKind::Serialization, None, "missing handle"))?;
        Ok(serde_json::from_value::<ProjectDbHandle>(handle.clone())?)
    }

    pub async fn list_projects(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ProjectDbHandle>, HorizonsError> {
        #[derive(Serialize)]
        struct Query {
            limit: i64,
            offset: i64,
        }
        let q = Query { limit, offset };
        self.client
            .request_json(Method::GET, "/api/v1/projects", Some(&q), None::<&()>)
            .await
    }

    pub async fn query(
        &self,
        project_id: Uuid,
        sql: impl Into<String>,
        params: Option<Vec<Value>>,
    ) -> Result<Value, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            sql: String,
            params: Vec<Value>,
        }
        let body = Body {
            sql: sql.into(),
            params: params.unwrap_or_default(),
        };
        self.client
            .request_value(
                Method::POST,
                &format!("/api/v1/projects/{project_id}/query"),
                None::<&()>,
                Some(&body),
            )
            .await
    }

    pub async fn execute(
        &self,
        project_id: Uuid,
        sql: impl Into<String>,
        params: Option<Vec<Value>>,
    ) -> Result<Value, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            sql: String,
            params: Vec<Value>,
        }
        let body = Body {
            sql: sql.into(),
            params: params.unwrap_or_default(),
        };
        self.client
            .request_value(
                Method::POST,
                &format!("/api/v1/projects/{project_id}/execute"),
                None::<&()>,
                Some(&body),
            )
            .await
    }
}

