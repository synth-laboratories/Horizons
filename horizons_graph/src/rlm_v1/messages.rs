use serde_json::Value;
use std::collections::HashSet;

pub fn sanitize_message_sequence(messages: &[Value]) -> Vec<Value> {
    let mut cleaned = Vec::with_capacity(messages.len());
    let mut pending_tool_call_ids: HashSet<String> = HashSet::new();

    for msg in messages {
        let role = msg.get("role").and_then(|v| v.as_str()).unwrap_or("");
        match role {
            "assistant" => {
                pending_tool_call_ids.clear();
                if let Some(tool_calls) = msg.get("tool_calls").and_then(|v| v.as_array()) {
                    for call in tool_calls {
                        if let Some(id) = call.get("id").and_then(|v| v.as_str()) {
                            pending_tool_call_ids.insert(id.to_string());
                        }
                    }
                }
                cleaned.push(msg.clone());
            }
            "tool" => {
                let tool_call_id = msg.get("tool_call_id").and_then(|v| v.as_str());
                if let Some(id) = tool_call_id {
                    if pending_tool_call_ids.contains(id) {
                        cleaned.push(msg.clone());
                    }
                }
            }
            _ => {
                pending_tool_call_ids.clear();
                cleaned.push(msg.clone());
            }
        }
    }

    cleaned
}

pub fn truncate_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let truncate_at = text
        .char_indices()
        .nth(max_chars)
        .map_or(text.len(), |(idx, _)| idx);
    format!("{}... (truncated)", &text[..truncate_at])
}

pub fn estimate_messages_tokens(messages: &[Value]) -> usize {
    let mut chars = 0usize;
    for msg in messages {
        if let Some(content) = msg.get("content") {
            chars += content
                .as_str()
                .map_or_else(|| content.to_string().len(), |s| s.len());
        }
        if let Some(tool_calls) = msg.get("tool_calls") {
            chars += tool_calls.to_string().len();
        }
        if let Some(tool_call_id) = msg.get("tool_call_id").and_then(|v| v.as_str()) {
            chars += tool_call_id.len();
        }
    }
    // Rough heuristic: ~4 chars per token.
    chars / 4
}

pub fn build_system_message(content: String) -> Value {
    serde_json::json!({
        "role": "system",
        "content": content,
    })
}
