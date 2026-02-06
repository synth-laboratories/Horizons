use crate::{ActionProposal, HorizonsClient, HorizonsError, RiskLevel};
use reqwest::Method;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ActionsApi {
    client: HorizonsClient,
}

impl ActionsApi {
    pub(crate) fn new(client: HorizonsClient) -> Self {
        Self { client }
    }

    pub async fn propose(
        &self,
        agent_id: impl Into<String>,
        action_type: impl Into<String>,
        payload: Value,
        risk_level: RiskLevel,
        dedupe_key: Option<String>,
        context: Value,
        ttl_seconds: Option<i64>,
        project_id: Option<Uuid>,
    ) -> Result<Uuid, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            agent_id: String,
            action_type: String,
            payload: Value,
            risk_level: RiskLevel,
            context: Value,
            #[serde(skip_serializing_if = "Option::is_none")]
            dedupe_key: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            ttl_seconds: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<String>,
        }
        let body = Body {
            agent_id: agent_id.into(),
            action_type: action_type.into(),
            payload,
            risk_level,
            context,
            dedupe_key,
            ttl_seconds,
            project_id: project_id.map(|u| u.to_string()),
        };
        let v = self
            .client
            .request_value(
                Method::POST,
                "/api/v1/actions/propose",
                None::<&()>,
                Some(&body),
            )
            .await?;
        let s = v.get("action_id").and_then(|x| x.as_str()).ok_or_else(|| {
            crate::HorizonsError::new(
                crate::HorizonsErrorKind::Serialization,
                None,
                "missing action_id",
            )
        })?;
        Ok(Uuid::parse_str(s).map_err(|e| {
            crate::HorizonsError::new(crate::HorizonsErrorKind::Serialization, None, e.to_string())
        })?)
    }

    pub async fn approve(
        &self,
        action_id: Uuid,
        reason: impl Into<String>,
        project_id: Option<Uuid>,
    ) -> Result<Value, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            reason: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<String>,
        }
        let body = Body {
            reason: reason.into(),
            project_id: project_id.map(|u| u.to_string()),
        };
        self.client
            .request_value(
                Method::POST,
                &format!("/api/v1/actions/{action_id}/approve"),
                None::<&()>,
                Some(&body),
            )
            .await
    }

    pub async fn deny(
        &self,
        action_id: Uuid,
        reason: impl Into<String>,
        project_id: Option<Uuid>,
    ) -> Result<Value, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            reason: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            project_id: Option<String>,
        }
        let body = Body {
            reason: reason.into(),
            project_id: project_id.map(|u| u.to_string()),
        };
        self.client
            .request_value(
                Method::POST,
                &format!("/api/v1/actions/{action_id}/deny"),
                None::<&()>,
                Some(&body),
            )
            .await
    }

    pub async fn pending(
        &self,
        project_id: Option<Uuid>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ActionProposal>, HorizonsError> {
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
                "/api/v1/actions/pending",
                Some(&q),
                None::<&()>,
            )
            .await
    }
}
