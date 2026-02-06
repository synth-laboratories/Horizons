use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{json, Value};
use tiktoken_rs::{cl100k_base, get_bpe_from_model, CoreBPE};

use crate::error::{GraphError, Result};

#[derive(Clone, Debug, PartialEq)]
pub enum ApiMode {
    ChatCompletions,
    Responses,
}

#[derive(Clone, Debug)]
pub struct LlmEndpoint {
    pub api_base: String,
    pub api_key: String,
    pub mode: ApiMode,
    pub provider: String,
}

#[derive(Clone, Debug)]
pub struct LlmRequest {
    pub model: String,
    pub messages: Vec<Value>,
    pub temperature: f64,
    pub max_tokens: Option<i64>,
    pub response_format: Option<Value>,
    pub endpoint: LlmEndpoint,
    pub stream: bool,
    pub tools: Option<Vec<Value>>,
    pub tool_choice: Option<Value>,
    pub previous_response_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct UsageSummary {
    pub prompt_tokens: Option<i64>,
    pub completion_tokens: Option<i64>,
    pub total_tokens: Option<i64>,
    /// Cached prompt tokens (from prompt_tokens_details.cached_tokens for OpenAI)
    /// These tokens are typically 90% cheaper than regular prompt tokens
    pub cached_tokens: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct LlmResponse {
    pub text: String,
    pub usage: UsageSummary,
    pub raw: Option<Value>,
}

pub type LlmChunkCallback = Box<dyn FnMut(String) + Send>;

#[async_trait]
pub trait LlmClientApi: Send + Sync {
    async fn send(
        &self,
        request: &LlmRequest,
        on_chunk: Option<LlmChunkCallback>,
    ) -> Result<LlmResponse>;
}

#[derive(Clone)]
pub struct LlmClient {
    client: reqwest::Client,
}

impl Default for LlmClient {
    fn default() -> Self {
        Self::new()
    }
}

impl LlmClient {
    pub fn new() -> Self {
        LlmClient {
            client: reqwest::Client::new(),
        }
    }

    pub async fn send(
        &self,
        request: &LlmRequest,
        mut on_chunk: Option<LlmChunkCallback>,
    ) -> Result<LlmResponse> {
        match request.endpoint.mode {
            ApiMode::ChatCompletions => self.send_chat(request, &mut on_chunk).await,
            ApiMode::Responses => self.send_responses(request, &mut on_chunk).await,
        }
    }

    async fn send_chat(
        &self,
        request: &LlmRequest,
        on_chunk: &mut Option<LlmChunkCallback>,
    ) -> Result<LlmResponse> {
        let url = format!(
            "{}/chat/completions",
            request.endpoint.api_base.trim_end_matches('/')
        );
        let mut payload = json!({
            "model": request.model,
            "messages": request.messages,
            "stream": request.stream,
        });
        if let Some(obj) = payload.as_object_mut() {
            let temp = adjust_temperature(&request.model, request.temperature);
            obj.insert("temperature".to_string(), Value::from(temp));
            if let Some(limit) = request.max_tokens {
                let key = if use_max_completion_tokens(&request.model, &request.endpoint.provider) {
                    "max_completion_tokens"
                } else {
                    "max_tokens"
                };
                obj.insert(key.to_string(), Value::from(limit));
            }
            if let Some(format) = &request.response_format {
                obj.insert("response_format".to_string(), format.clone());
            }
            if let Some(tools) = &request.tools {
                obj.insert("tools".to_string(), Value::Array(tools.clone()));
            }
            if let Some(choice) = &request.tool_choice {
                obj.insert("tool_choice".to_string(), choice.clone());
            }
        }

        let response = self
            .client
            .post(url)
            .bearer_auth(&request.endpoint.api_key)
            .json(&payload)
            .send()
            .await
            .map_err(|err| GraphError::internal(format!("llm request failed: {err}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_else(|_| "unknown".to_string());
            return Err(GraphError::internal(format!(
                "llm request failed ({status}): {body}"
            )));
        }
        if request.stream {
            let mut text = String::new();
            let mut usage = UsageSummary::default();
            let mut stream = response.bytes_stream();
            let mut buffer = String::new();
            while let Some(chunk) = stream.next().await {
                let chunk =
                    chunk.map_err(|err| GraphError::internal(format!("llm stream error: {err}")))?;
                buffer.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(event) = next_sse_event(&mut buffer) {
                    if event == "[DONE]" {
                        fill_usage_fallback(&request.model, &request.messages, &text, &mut usage);
                        return Ok(LlmResponse {
                            text,
                            usage,
                            raw: None,
                        });
                    }
                    if let Ok(value) = serde_json::from_str::<Value>(&event) {
                        if let Some(delta) = extract_chat_delta(&value) {
                            text.push_str(&delta);
                            if let Some(cb) = on_chunk.as_mut() {
                                cb(delta);
                            }
                        }
                        if let Some(new_usage) = extract_usage(&value) {
                            usage = new_usage;
                        }
                    }
                }
            }
            Ok(LlmResponse {
                text,
                usage,
                raw: None,
            })
        } else {
            let value: Value = response
                .json()
                .await
                .map_err(|err| GraphError::internal(format!("llm response parse failed: {err}")))?;
            let text = extract_chat_message(&value).unwrap_or_default();
            let mut usage = extract_usage(&value).unwrap_or_default();
            fill_usage_fallback(&request.model, &request.messages, &text, &mut usage);
            Ok(LlmResponse {
                text,
                usage,
                raw: Some(value),
            })
        }
    }

    async fn send_responses(
        &self,
        request: &LlmRequest,
        on_chunk: &mut Option<LlmChunkCallback>,
    ) -> Result<LlmResponse> {
        let url = format!(
            "{}/responses",
            request.endpoint.api_base.trim_end_matches('/')
        );
        let mut payload = json!({
            "model": request.model,
            "input": request.messages,
            "stream": request.stream,
        });
        if let Some(obj) = payload.as_object_mut() {
            let temp = adjust_temperature(&request.model, request.temperature);
            obj.insert("temperature".to_string(), Value::from(temp));
            if let Some(limit) = request.max_tokens {
                obj.insert("max_output_tokens".to_string(), Value::from(limit));
            }
            // For Responses API, response_format must be nested under text.format
            if let Some(format) = &request.response_format {
                obj.insert("text".to_string(), json!({ "format": format.clone() }));
            }
            if let Some(tools) = &request.tools {
                obj.insert("tools".to_string(), Value::Array(tools.clone()));
            }
            if let Some(choice) = &request.tool_choice {
                obj.insert("tool_choice".to_string(), choice.clone());
            }
            if let Some(prev) = &request.previous_response_id {
                obj.insert(
                    "previous_response_id".to_string(),
                    Value::String(prev.clone()),
                );
            }
        }

        let response = self
            .client
            .post(url)
            .bearer_auth(&request.endpoint.api_key)
            .json(&payload)
            .send()
            .await
            .map_err(|err| GraphError::internal(format!("llm request failed: {err}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_else(|_| "unknown".to_string());
            return Err(GraphError::internal(format!(
                "llm request failed ({status}): {body}"
            )));
        }
        if request.stream {
            let mut text = String::new();
            let mut usage = UsageSummary::default();
            let mut stream = response.bytes_stream();
            let mut buffer = String::new();
            while let Some(chunk) = stream.next().await {
                let chunk =
                    chunk.map_err(|err| GraphError::internal(format!("llm stream error: {err}")))?;
                buffer.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(event) = next_sse_event(&mut buffer) {
                    if event == "[DONE]" {
                        fill_usage_fallback(&request.model, &request.messages, &text, &mut usage);
                        return Ok(LlmResponse {
                            text,
                            usage,
                            raw: None,
                        });
                    }
                    if let Ok(value) = serde_json::from_str::<Value>(&event) {
                        if let Some(delta) = extract_response_delta(&value) {
                            text.push_str(&delta);
                            if let Some(cb) = on_chunk.as_mut() {
                                cb(delta);
                            }
                        }
                        if let Some(new_usage) = extract_response_usage(&value) {
                            usage = new_usage;
                        }
                    }
                }
            }
            Ok(LlmResponse {
                text,
                usage,
                raw: None,
            })
        } else {
            let value: Value = response
                .json()
                .await
                .map_err(|err| GraphError::internal(format!("llm response parse failed: {err}")))?;
            let text = extract_response_text(&value).unwrap_or_default();
            let mut usage = extract_usage(&value).unwrap_or_default();
            fill_usage_fallback(&request.model, &request.messages, &text, &mut usage);
            Ok(LlmResponse {
                text,
                usage,
                raw: Some(value),
            })
        }
    }
}

pub fn resolve_endpoint(
    model: &str,
    implementation: Option<&Value>,
) -> Result<LlmEndpoint> {
    let mut api_base = implementation
        .and_then(|v| v.get("api_base"))
        .and_then(|v| v.as_str())
        .or_else(|| implementation.and_then(|v| v.get("base_url")).and_then(|v| v.as_str()))
        .or_else(|| implementation.and_then(|v| v.get("api_url")).and_then(|v| v.as_str()))
        .map(|v| v.to_string());

    if api_base.is_none() {
        if let Ok(value) = std::env::var("GRAPH_LLM_API_BASE") {
            api_base = Some(value);
        }
    }

    let mut api_mode = implementation
        .and_then(|v| v.get("api_mode"))
        .and_then(|v| v.as_str())
        .map(|v| v.to_lowercase());

    if api_mode.is_none() {
        if let Ok(value) = std::env::var("GRAPH_LLM_API_MODE") {
            api_mode = Some(value.to_lowercase());
        }
    }

    let mode = match api_mode.as_deref() {
        Some("responses") => ApiMode::Responses,
        Some("chat" | "chat_completions") => ApiMode::ChatCompletions,
        _ => ApiMode::ChatCompletions,
    };

    let provider = if let Some(base) = &api_base {
        if base.contains("groq") {
            "groq".to_string()
        } else if base.contains("googleapis") || base.contains("generativelanguage") {
            "google".to_string()
        } else {
            "openai".to_string()
        }
    } else if is_groq_model(model) {
        "groq".to_string()
    } else {
        "openai".to_string()
    };

    if api_base.is_none() {
        api_base = Some(match provider.as_str() {
            "groq" => std::env::var("GROQ_BASE_URL")
                .unwrap_or_else(|_| "https://api.groq.com/openai/v1".to_string()),
            "google" => std::env::var("GOOGLE_BASE_URL")
                .unwrap_or_else(|_| "https://generativelanguage.googleapis.com/v1".to_string()),
            _ => std::env::var("OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
        });
    }

    let mut api_base = api_base.unwrap_or_else(|| "https://api.openai.com/v1".to_string());
    api_base = normalize_api_base(&api_base);

    let api_key = implementation
        .and_then(|v| v.get("api_key"))
        .and_then(|v| v.as_str())
        .map(|v| v.to_string())
        .or_else(|| std::env::var("GRAPH_LLM_API_KEY").ok())
        .or_else(|| match provider.as_str() {
            "groq" => std::env::var("GROQ_API_KEY").ok(),
            "google" => std::env::var("GOOGLE_API_KEY").ok(),
            _ => std::env::var("OPENAI_API_KEY").ok(),
        })
        .ok_or_else(|| GraphError::internal("missing LLM API key"))?;

    Ok(LlmEndpoint {
        api_base,
        api_key,
        mode,
        provider,
    })
}

fn normalize_api_base(base: &str) -> String {
    let trimmed = base.trim_end_matches('/');
    let trimmed = trimmed.trim_end_matches("/chat/completions").trim_end_matches("/responses");
    trimmed.to_string()
}

fn use_max_completion_tokens(model: &str, provider: &str) -> bool {
    let lower = model.to_lowercase();
    provider == "groq"
        || lower.contains("gpt-5")
        || lower.contains("o1")
        || lower.contains("gpt-oss")
}

fn adjust_temperature(model: &str, requested: f64) -> f64 {
    let lower = model.to_lowercase();
    if (lower.contains("gpt-5")
        || lower.contains("o1-")
        || lower.contains("o3-")
        || lower.contains("o4-"))
        && requested == 0.0
    {
        1.0
    } else {
        requested
    }
}

fn extract_chat_message(value: &Value) -> Option<String> {
    let choices = value.get("choices")?.as_array()?;
    let first = choices.first()?;
    let message = first.get("message")?;
    message.get("content")?.as_str().map(|v| v.to_string())
}

fn extract_chat_delta(value: &Value) -> Option<String> {
    let choices = value.get("choices")?.as_array()?;
    let mut combined = String::new();
    for choice in choices {
        let delta = choice.get("delta")?;
        if let Some(fragment) = delta.get("content").and_then(|v| v.as_str()) {
            combined.push_str(fragment);
        }
    }
    if combined.is_empty() {
        None
    } else {
        Some(combined)
    }
}

fn extract_response_text(value: &Value) -> Option<String> {
    let outputs = value.get("output")?.as_array()?;
    let mut combined = String::new();
    for output in outputs {
        let contents = output.get("content").and_then(|v| v.as_array())?;
        for item in contents {
            if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                combined.push_str(text);
            } else if let Some(text) = item.get("output_text").and_then(|v| v.as_str()) {
                combined.push_str(text);
            }
        }
    }
    if combined.is_empty() {
        None
    } else {
        Some(combined)
    }
}

fn extract_response_delta(value: &Value) -> Option<String> {
    let event_type = value.get("type")?.as_str()?;
    if event_type.ends_with("output_text.delta") {
        return value.get("delta").and_then(|v| v.as_str()).map(|v| v.to_string());
    }
    None
}

fn extract_response_usage(value: &Value) -> Option<UsageSummary> {
    if value.get("type")?.as_str()? == "response.completed" {
        if let Some(resp) = value.get("response") {
            return extract_usage(resp);
        }
    }
    None
}

fn extract_usage(value: &Value) -> Option<UsageSummary> {
    let usage = value.get("usage")?;
    // Extract cached tokens from prompt_tokens_details (OpenAI format)
    // OpenAI returns: usage.prompt_tokens_details.cached_tokens
    let cached_tokens = usage
        .get("prompt_tokens_details")
        .and_then(|details| details.get("cached_tokens"))
        .and_then(|v| v.as_i64());
    let prompt_tokens = usage
        .get("prompt_tokens")
        .and_then(|v| v.as_i64())
        .or_else(|| usage.get("input_tokens").and_then(|v| v.as_i64()));
    let completion_tokens = usage
        .get("completion_tokens")
        .and_then(|v| v.as_i64())
        .or_else(|| usage.get("output_tokens").and_then(|v| v.as_i64()));
    let total_tokens = usage.get("total_tokens").and_then(|v| v.as_i64()).or_else(|| {
        match (prompt_tokens, completion_tokens) {
            (Some(prompt), Some(completion)) => Some(prompt + completion),
            _ => None,
        }
    });
    Some(UsageSummary {
        prompt_tokens,
        completion_tokens,
        total_tokens,
        cached_tokens,
    })
}

fn fill_usage_fallback(
    model: &str,
    messages: &[Value],
    response_text: &str,
    usage: &mut UsageSummary,
) {
    let needs_prompt = usage.prompt_tokens.is_none() || usage.prompt_tokens == Some(0);
    let needs_completion = usage.completion_tokens.is_none() || usage.completion_tokens == Some(0);
    if !needs_prompt && !needs_completion && usage.total_tokens.is_some() {
        return;
    }
    let bpe = match get_bpe_from_model(model) {
        Ok(bpe) => bpe,
        Err(_) => match cl100k_base() {
            Ok(bpe) => bpe,
            Err(_) => return,
        },
    };
    if needs_prompt {
        let prompt_tokens = estimate_message_tokens(&bpe, messages);
        usage.prompt_tokens = Some(prompt_tokens);
    }
    if needs_completion {
        let completion_tokens = estimate_text_tokens(&bpe, response_text);
        usage.completion_tokens = Some(completion_tokens);
    }
    if usage.total_tokens.is_none() {
        let prompt_tokens = usage.prompt_tokens.unwrap_or(0);
        let completion_tokens = usage.completion_tokens.unwrap_or(0);
        usage.total_tokens = Some(prompt_tokens + completion_tokens);
    }
}

fn estimate_message_tokens(bpe: &CoreBPE, messages: &[Value]) -> i64 {
    let mut total = 0i64;
    for msg in messages {
        let text = message_to_text(msg);
        total += estimate_text_tokens(bpe, &text);
    }
    total
}

fn estimate_text_tokens(bpe: &CoreBPE, text: &str) -> i64 {
    bpe.encode_with_special_tokens(text).len() as i64
}

fn message_to_text(msg: &Value) -> String {
    if let Some(obj) = msg.as_object() {
        let role = obj.get("role").and_then(|v| v.as_str()).unwrap_or("");
        let content = obj.get("content").cloned().unwrap_or(Value::Null);
        let content_text = match content {
            Value::String(text) => text,
            Value::Array(items) => items
                .iter()
                .filter_map(|item| item.get("text").and_then(|v| v.as_str()))
                .collect::<Vec<_>>()
                .join(" "),
            Value::Null => String::new(),
            other => other.to_string(),
        };
        if role.is_empty() {
            return content_text;
        }
        return format!("{role}: {content_text}");
    }
    msg.to_string()
}

fn next_sse_event(buffer: &mut String) -> Option<String> {
    let (idx, sep_len) = if let Some(idx) = buffer.find("\r\n\r\n") {
        (idx, 4)
    } else if let Some(idx) = buffer.find("\n\n") {
        (idx, 2)
    } else {
        return None;
    };
    let mut event = buffer[..idx].to_string();
    *buffer = buffer[idx + sep_len..].to_string();
    event = event.replace("\r\n", "\n");
    let mut data_lines = Vec::new();
    for line in event.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.trim_start().to_string());
        }
    }
    if data_lines.is_empty() {
        None
    } else {
        Some(data_lines.join("\n"))
    }
}

pub(crate) fn is_groq_model(model: &str) -> bool {
    let lower = model.to_lowercase();
    let indicators = [
        "gpt-oss", "llama-3", "llama-4", "mixtral", "qwen", "gemma", "kimi",
    ];
    indicators.iter().any(|fragment| lower.contains(fragment))
}

#[async_trait]
impl LlmClientApi for LlmClient {
    async fn send(
        &self,
        request: &LlmRequest,
        on_chunk: Option<LlmChunkCallback>,
    ) -> Result<LlmResponse> {
        LlmClient::send(self, request, on_chunk).await
    }
}
