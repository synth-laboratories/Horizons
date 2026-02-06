use crate::{HorizonsClient, HorizonsError};
use futures_util::Stream;
use reqwest::Method;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct EngineApi {
    client: HorizonsClient,
}

impl EngineApi {
    pub(crate) fn new(client: HorizonsClient) -> Self {
        Self { client }
    }

    pub async fn run(
        &self,
        agent: impl Into<String>,
        instruction: impl Into<String>,
        model: Option<String>,
        permission_mode: String,
        image: Option<String>,
        env_vars: Option<std::collections::HashMap<String, String>>,
        timeout_seconds: i64,
    ) -> Result<Value, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            agent: String,
            instruction: String,
            permission_mode: String,
            timeout_seconds: i64,
            #[serde(skip_serializing_if = "Option::is_none")]
            model: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            image: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            env_vars: Option<std::collections::HashMap<String, String>>,
        }
        let body = Body {
            agent: agent.into(),
            instruction: instruction.into(),
            permission_mode,
            timeout_seconds,
            model,
            image,
            env_vars,
        };
        self.client
            .request_value(Method::POST, "/api/v1/engine/run", None::<&()>, Some(&body))
            .await
    }

    pub async fn start(
        &self,
        agent: impl Into<String>,
        instruction: impl Into<String>,
        model: Option<String>,
        permission_mode: String,
        image: Option<String>,
        env_vars: Option<std::collections::HashMap<String, String>>,
        timeout_seconds: i64,
    ) -> Result<Value, HorizonsError> {
        #[derive(Serialize)]
        struct Body {
            agent: String,
            instruction: String,
            permission_mode: String,
            timeout_seconds: i64,
            #[serde(skip_serializing_if = "Option::is_none")]
            model: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            image: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            env_vars: Option<std::collections::HashMap<String, String>>,
        }
        let body = Body {
            agent: agent.into(),
            instruction: instruction.into(),
            permission_mode,
            timeout_seconds,
            model,
            image,
            env_vars,
        };
        self.client
            .request_value(Method::POST, "/api/v1/engine/start", None::<&()>, Some(&body))
            .await
    }

    pub fn events(
        &self,
        handle_id: impl Into<String>,
    ) -> impl Stream<Item = Result<Value, HorizonsError>> + Send + 'static {
        // Engine uses GET for SSE and includes "event: done/error" frames.
        // The client SSE implementation is POST-only; for engine, we can use the same parser by POST-ing an empty body
        // to a dedicated endpoint in the future. For now, mimic the Python SDK by doing GET and parsing.
        //
        // To keep the surface stable for v0.1.x, we call the server as GET and return a stream of JSON events.
        let client = self.client.clone();
        let handle_id = handle_id.into();
        async_stream::try_stream! {
            use reqwest::header::{HeaderMap, HeaderValue, ACCEPT};
            use futures_util::StreamExt;
            use bytes::Bytes;

            let mut extra = HeaderMap::new();
            extra.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));

            let resp = client
                .send(Method::GET, &format!("/api/v1/engine/{handle_id}/events"), None::<&()>, None::<&()>, Some(extra))
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

                            if let Some(ev) = cur_event.as_deref() {
                                if ev == "done" {
                                    return;
                                }
                                if ev == "error" {
                                    Err(crate::HorizonsError::new(
                                        crate::HorizonsErrorKind::Stream,
                                        None,
                                        data_str.to_string(),
                                    ))?;
                                }
                            }

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

    pub async fn release(&self, handle_id: impl Into<String>) -> Result<Value, HorizonsError> {
        let handle_id = handle_id.into();
        self.client
            .request_value(
                Method::POST,
                &format!("/api/v1/engine/{handle_id}/release"),
                None::<&()>,
                None::<&()>,
            )
            .await
    }

    pub async fn health(&self, handle_id: impl Into<String>) -> Result<Value, HorizonsError> {
        let handle_id = handle_id.into();
        self.client
            .request_value(
                Method::GET,
                &format!("/api/v1/engine/{handle_id}/health"),
                None::<&()>,
                None::<&()>,
            )
            .await
    }
}
