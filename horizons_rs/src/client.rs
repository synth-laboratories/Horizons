use crate::error::{HorizonsError, HorizonsErrorKind};
use crate::types::Health;
use async_stream::try_stream;
use bytes::Bytes;
use futures_util::Stream;
use futures_util::StreamExt;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Default, Clone)]
pub struct ClientOptions {
    pub project_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub user_email: Option<String>,
    pub agent_id: Option<String>,
    pub api_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HorizonsClient {
    base_url: String,
    org_id: Uuid,
    opts: ClientOptions,
    http: reqwest::Client,
}

impl HorizonsClient {
    pub fn new(base_url: impl Into<String>, org_id: Uuid) -> Self {
        let base_url = base_url.into();
        let base_url = base_url.trim_end_matches('/').to_string();
        Self {
            base_url,
            org_id,
            opts: ClientOptions::default(),
            http: reqwest::Client::new(),
        }
    }

    pub fn with_project_id(mut self, project_id: Uuid) -> Self {
        self.opts.project_id = Some(project_id);
        self
    }

    pub fn with_user(mut self, user_id: Uuid, user_email: Option<impl Into<String>>) -> Self {
        self.opts.user_id = Some(user_id);
        self.opts.user_email = user_email.map(|s| s.into());
        self
    }

    pub fn with_agent_id(mut self, agent_id: impl Into<String>) -> Self {
        self.opts.agent_id = Some(agent_id.into());
        self
    }

    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.opts.api_key = Some(api_key.into());
        self
    }

    pub fn org_id(&self) -> Uuid {
        self.org_id
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn headers(&self, extra: Option<HeaderMap>) -> Result<HeaderMap, HorizonsError> {
        fn insert(
            headers: &mut HeaderMap,
            name: &'static str,
            value: impl AsRef<str>,
        ) -> Result<(), HorizonsError> {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
                HorizonsError::new(HorizonsErrorKind::Serialization, None, e.to_string())
            })?;
            let value = HeaderValue::from_str(value.as_ref()).map_err(|e| {
                HorizonsError::new(HorizonsErrorKind::Serialization, None, e.to_string())
            })?;
            headers.insert(name, value);
            Ok(())
        }

        let mut headers = HeaderMap::new();
        insert(&mut headers, "x-org-id", self.org_id.to_string())?;
        if let Some(project_id) = self.opts.project_id {
            insert(&mut headers, "x-project-id", project_id.to_string())?;
        }
        if let Some(user_id) = self.opts.user_id {
            insert(&mut headers, "x-user-id", user_id.to_string())?;
            if let Some(email) = self.opts.user_email.as_deref() {
                insert(&mut headers, "x-user-email", email)?;
            }
        }
        if let Some(agent_id) = self.opts.agent_id.as_deref() {
            insert(&mut headers, "x-agent-id", agent_id)?;
        }
        if let Some(api_key) = self.opts.api_key.as_deref() {
            let v = format!("Bearer {api_key}");
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&v).map_err(|e| {
                    HorizonsError::new(HorizonsErrorKind::Serialization, None, e.to_string())
                })?,
            );
        }
        if let Some(extra) = extra {
            headers.extend(extra);
        }
        Ok(headers)
    }

    fn url(&self, path: &str) -> String {
        if path.starts_with('/') {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}/{}", self.base_url, path)
        }
    }

    pub(crate) async fn send(
        &self,
        method: Method,
        path: &str,
        query: Option<&impl Serialize>,
        body: Option<&impl Serialize>,
        extra_headers: Option<HeaderMap>,
    ) -> Result<Response, HorizonsError> {
        let mut req = self.http.request(method, self.url(path));
        if let Some(q) = query {
            req = req.query(q);
        }
        if let Some(b) = body {
            req = req.json(b);
        }
        req = req.headers(self.headers(extra_headers)?);
        Ok(req.send().await?)
    }

    pub(crate) async fn map_error(&self, resp: Response) -> Result<HorizonsError, HorizonsError> {
        let status = resp.status();
        let code = status.as_u16();
        let text = resp.text().await.unwrap_or_default();
        let kind = if code == 404 {
            HorizonsErrorKind::NotFound
        } else if code == 401 || code == 403 {
            HorizonsErrorKind::Auth
        } else if (400..500).contains(&code) {
            HorizonsErrorKind::Validation
        } else {
            HorizonsErrorKind::Server
        };
        Ok(HorizonsError::new(
            kind,
            Some(code),
            if text.is_empty() {
                status.to_string()
            } else {
                text
            },
        ))
    }

    pub async fn request_json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: Option<&impl Serialize>,
        body: Option<&impl Serialize>,
    ) -> Result<T, HorizonsError> {
        let resp = self.send(method, path, query, body, None).await?;
        if resp.status().is_success() {
            if resp.status().as_u16() == 204 {
                // Can't deserialize an empty body. Match the Python SDK behavior.
                return Ok(serde_json::from_value(Value::Null)?);
            }
            return Ok(resp.json::<T>().await?);
        }
        Err(self.map_error(resp).await?)
    }

    pub async fn request_value(
        &self,
        method: Method,
        path: &str,
        query: Option<&impl Serialize>,
        body: Option<&impl Serialize>,
    ) -> Result<Value, HorizonsError> {
        self.request_json::<Value>(method, path, query, body).await
    }

    pub fn sse_post(
        &self,
        path: &str,
        body: impl Serialize + Send + Sync + 'static,
    ) -> impl Stream<Item = Result<Value, HorizonsError>> + Send + 'static {
        let client = self.clone();
        let path = path.to_string();
        try_stream! {
            let mut extra = HeaderMap::new();
            extra.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
            let resp = client
                .send(Method::POST, &path, None::<&()>, Some(&body), Some(extra))
                .await?;

            if resp.status().is_success() {
                let mut stream = resp.bytes_stream();
                let mut buf: Vec<u8> = Vec::new();
                let mut cur_event: Option<String> = None;

                while let Some(chunk) = stream.next().await {
                    let chunk: Bytes = chunk?;
                    buf.extend_from_slice(&chunk);

                    while let Some(pos) = buf.iter().position(|b| *b == b'\n') {
                        let mut line = buf.drain(..=pos).collect::<Vec<u8>>();
                        if line.ends_with(b"\n") { line.pop(); }
                        if line.ends_with(b"\r") { line.pop(); }
                        let line = String::from_utf8_lossy(&line).to_string();

                        if line.is_empty() {
                            cur_event = None;
                            continue;
                        }
                        if line.starts_with(':') {
                            continue;
                        }
                        if let Some(rest) = line.strip_prefix("event:") {
                            cur_event = Some(rest.trim().to_string());
                            continue;
                        }
                        if let Some(rest) = line.strip_prefix("data:") {
                            let data_str = rest.trim();
                            if data_str.is_empty() {
                                continue;
                            }

                            // Some streams may send control events.
                            if let Some(ev) = cur_event.as_deref() {
                                if ev == "done" {
                                    return;
                                }
                                if ev == "error" {
                                    Err(HorizonsError::new(
                                        HorizonsErrorKind::Stream,
                                        None,
                                        data_str.to_string(),
                                    ))?;
                                }
                            }

                            // Best-effort JSON decode; skip non-JSON lines.
                            if let Ok(v) = serde_json::from_str::<Value>(data_str) {
                                yield v;
                            }
                        }
                    }
                }
            } else {
                Err(client.map_error(resp).await?)?;
            }
        }
    }

    pub async fn health(&self) -> Result<Health, HorizonsError> {
        self.request_json(Method::GET, "/api/v1/health", None::<&()>, None::<&()>)
            .await
    }

    pub fn onboard(&self) -> crate::apis::OnboardApi {
        crate::apis::OnboardApi::new(self.clone())
    }

    pub fn events(&self) -> crate::apis::EventsApi {
        crate::apis::EventsApi::new(self.clone())
    }

    pub fn agents(&self) -> crate::apis::AgentsApi {
        crate::apis::AgentsApi::new(self.clone())
    }

    pub fn actions(&self) -> crate::apis::ActionsApi {
        crate::apis::ActionsApi::new(self.clone())
    }

    pub fn memory(&self) -> crate::apis::MemoryApi {
        crate::apis::MemoryApi::new(self.clone())
    }

    pub fn context_refresh(&self) -> crate::apis::ContextRefreshApi {
        crate::apis::ContextRefreshApi::new(self.clone())
    }

    pub fn optimization(&self) -> crate::apis::OptimizationApi {
        crate::apis::OptimizationApi::new(self.clone())
    }

    pub fn evaluation(&self) -> crate::apis::EvaluationApi {
        crate::apis::EvaluationApi::new(self.clone())
    }

    pub fn engine(&self) -> crate::apis::EngineApi {
        crate::apis::EngineApi::new(self.clone())
    }

    pub fn pipelines(&self) -> crate::apis::PipelinesApi {
        crate::apis::PipelinesApi::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_trailing_slash() {
        let c = HorizonsClient::new("http://localhost:8000/", Uuid::nil());
        assert_eq!(c.base_url(), "http://localhost:8000");
    }
}
