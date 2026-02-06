use serde_json::{Map, Value, json};
use std::collections::HashSet;

#[derive(Debug)]
pub struct TemplateError(pub String);

pub fn render_messages(sequence: &Value, inputs: &Value) -> Result<Vec<Value>, TemplateError> {
    let templates = sequence
        .get("template_likes")
        .and_then(|v| v.as_array())
        .ok_or_else(|| TemplateError("sequence.template_likes must be a list".to_string()))?;
    let mut messages = Vec::new();
    for template in templates {
        if let Some(kind) = template.get("type").and_then(|v| v.as_str()) {
            if kind != "prompt_template" {
                continue;
            }
        }
        let role = template
            .get("message_type")
            .and_then(|v| v.as_str())
            .unwrap_or("USER")
            .to_lowercase();
        let content = render_template(template, inputs)?;
        messages.push(json!({
            "role": role,
            "content": content,
        }));
    }

    let images = extract_images(inputs);
    if !images.is_empty() {
        attach_images(&mut messages, images);
    }

    if messages.is_empty() {
        messages.push(json!({
            "role": "user",
            "content": "No templates provided.",
        }));
    }

    Ok(messages)
}

fn render_template(template: &Value, inputs: &Value) -> Result<String, TemplateError> {
    let mut content = template
        .get("structured_template")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let input_fields = template
        .get("input_fields")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let instructions = template
        .get("instructions_fields")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    for (field, data) in instructions {
        let placeholder = format!("<instruction>{field}</instruction>");
        let replacement = data
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        content = content.replace(&placeholder, replacement);
    }

    for (key, field_data) in input_fields {
        let field_name = field_data
            .get("field_name")
            .and_then(|v| v.as_str())
            .unwrap_or(&key);
        let is_required = field_data
            .get("is_required")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let field_type = field_data
            .get("field_type")
            .and_then(|v| v.as_str())
            .unwrap_or("text");
        let value = inputs
            .as_object()
            .and_then(|map| map.get(field_name))
            .cloned();
        let rendered = if let Some(val) = value {
            stringify_value(&val, field_type)
        } else {
            if is_required {
                return Err(TemplateError(format!(
                    "missing required input field '{field_name}'"
                )));
            }
            String::new()
        };
        content = replace_placeholders(&content, field_name, &rendered);
    }

    if let Some(map) = inputs.as_object() {
        for (key, value) in map {
            let rendered = stringify_value(value, "text");
            content = replace_placeholders(&content, key, &rendered);
        }
    }

    Ok(content)
}

fn replace_placeholders(content: &str, field: &str, rendered: &str) -> String {
    let mut updated = content.replace(&format!("<input>{field}</input>"), rendered);
    let variants = [
        format!("{{{{ {field} }}}}"),
        format!("{{{{{field}}}}}"),
        format!("{{{{ {field}}}}}"),
        format!("{{{{{field} }}}}"),
    ];
    for variant in variants {
        updated = updated.replace(&variant, rendered);
    }
    updated
}

fn stringify_value(value: &Value, field_type: &str) -> String {
    if field_type == "image" && is_image_value(value) {
        return "[IMAGE]".to_string();
    }
    if is_image_value(value) {
        return "[IMAGE]".to_string();
    }
    match value {
        Value::String(text) => text.clone(),
        Value::Number(num) => num.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => String::new(),
        other => serde_json::to_string(other).unwrap_or_else(|_| String::new()),
    }
}

fn is_image_value(value: &Value) -> bool {
    match value {
        Value::String(text) => looks_like_image(text),
        Value::Array(items) => items.iter().any(is_image_value),
        Value::Object(map) => {
            map.get("url")
                .and_then(|v| v.as_str())
                .is_some_and(looks_like_image)
                || map
                    .get("data_url")
                    .and_then(|v| v.as_str())
                    .is_some_and(looks_like_image)
        }
        _ => false,
    }
}

fn extract_images(inputs: &Value) -> Vec<String> {
    let mut images = Vec::new();
    let mut seen = HashSet::new();
    let Some(map) = inputs.as_object() else {
        return images;
    };
    for (key, value) in map {
        if key.to_lowercase().contains("image") {
            collect_images(value, &mut images, &mut seen);
            continue;
        }
        collect_images(value, &mut images, &mut seen);
    }
    images
}

fn collect_images(value: &Value, images: &mut Vec<String>, seen: &mut HashSet<String>) {
    match value {
        Value::String(text) => {
            if looks_like_image(text) && seen.insert(text.clone()) {
                images.push(text.clone());
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_images(item, images, seen);
            }
        }
        Value::Object(map) => {
            if map
                .get("type")
                .and_then(|v| v.as_str())
                .is_some_and(|t| t == "image_url")
            {
                if let Some(image_value) = map.get("image_url") {
                    extract_image_from_value(image_value, map, images, seen);
                }
            }

            for key in ["url", "data_url"] {
                if let Some(url) = map.get(key).and_then(|v| v.as_str()) {
                    if looks_like_image(url) && seen.insert(url.to_string()) {
                        images.push(url.to_string());
                    }
                }
            }

            if let Some(image_value) = map.get("image_url") {
                extract_image_from_value(image_value, map, images, seen);
            }

            if let Some(b64) = map
                .get("b64_json")
                .or_else(|| map.get("data"))
                .and_then(|v| v.as_str())
            {
                let mime = map
                    .get("mime_type")
                    .or_else(|| map.get("media_type"))
                    .and_then(|v| v.as_str());
                if let Some(url) = coerce_base64_image(b64, mime) {
                    push_image(images, seen, url);
                }
            }

            for value in map.values() {
                collect_images(value, images, seen);
            }
        }
        _ => {}
    }
}

fn extract_image_from_value(
    value: &Value,
    parent: &Map<String, Value>,
    images: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    match value {
        Value::String(text) => {
            if looks_like_image(text) {
                push_image(images, seen, text.to_string());
            } else if let Some(url) = coerce_base64_image(
                text,
                parent
                    .get("mime_type")
                    .or_else(|| parent.get("media_type"))
                    .and_then(|v| v.as_str()),
            ) {
                push_image(images, seen, url);
            }
        }
        Value::Object(obj) => {
            if let Some(url) = obj.get("url").and_then(|v| v.as_str()) {
                if looks_like_image(url) {
                    push_image(images, seen, url.to_string());
                }
            }
            if let Some(url) = obj.get("image_url").and_then(|v| v.as_str()) {
                if looks_like_image(url) {
                    push_image(images, seen, url.to_string());
                }
            }
            if let Some(b64) = obj
                .get("b64_json")
                .or_else(|| obj.get("data"))
                .and_then(|v| v.as_str())
            {
                let mime = obj
                    .get("mime_type")
                    .or_else(|| obj.get("media_type"))
                    .and_then(|v| v.as_str());
                if let Some(url) = coerce_base64_image(b64, mime) {
                    push_image(images, seen, url);
                }
            }
            for value in obj.values() {
                collect_images(value, images, seen);
            }
        }
        Value::Array(items) => {
            for item in items {
                extract_image_from_value(item, parent, images, seen);
            }
        }
        _ => {}
    }
}

fn coerce_base64_image(value: &str, mime_type: Option<&str>) -> Option<String> {
    if value.starts_with("data:image/") {
        return Some(value.to_string());
    }
    if looks_like_base64(value) {
        let mime = mime_type.unwrap_or("image/png");
        return Some(format!("data:{mime};base64,{value}"));
    }
    None
}

fn push_image(images: &mut Vec<String>, seen: &mut HashSet<String>, url: String) {
    if seen.insert(url.clone()) {
        images.push(url);
    }
}

fn looks_like_base64(value: &str) -> bool {
    if value.len() < 32 {
        return false;
    }
    let mut has_padding = false;
    for ch in value.chars() {
        if ch == '=' {
            has_padding = true;
            continue;
        }
        if !ch.is_ascii_alphanumeric() && ch != '+' && ch != '/' && ch != '\n' && ch != '\r' {
            return false;
        }
    }
    has_padding || value.len() % 4 == 0
}

fn looks_like_image(text: &str) -> bool {
    let lower = text.to_lowercase();
    if lower.starts_with("data:image") {
        return true;
    }
    if lower.starts_with("http://") || lower.starts_with("https://") {
        let exts = [".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg"];
        return exts.iter().any(|ext| lower.contains(ext));
    }
    false
}

fn attach_images(messages: &mut [Value], images: Vec<String>) {
    if messages.is_empty() {
        return;
    }
    let mut target_index = messages.len().saturating_sub(1);
    for (idx, message) in messages.iter().enumerate() {
        if message
            .get("role")
            .and_then(|v| v.as_str())
            .is_some_and(|role| role == "user")
        {
            target_index = idx;
        }
    }
    if let Some(message) = messages.get_mut(target_index) {
        let existing = message.get("content").cloned().unwrap_or(Value::Null);
        let mut parts = match existing {
            Value::Array(items) => items,
            Value::String(text) => {
                if text.is_empty() {
                    Vec::new()
                } else {
                    vec![json!({
                        "type": "text",
                        "text": text,
                    })]
                }
            }
            _ => Vec::new(),
        };
        for image in images {
            parts.push(json!({
                "type": "image_url",
                "image_url": {
                    "url": image,
                },
            }));
        }
        if let Some(obj) = message.as_object_mut() {
            obj.insert("content".to_string(), Value::Array(parts));
        }
    }
}
