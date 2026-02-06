use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedAnswerSchema {
    pub schema: Value,
    pub is_verifier: bool,
}

pub fn resolve_answer_schema(
    raw: Option<Value>,
    is_verifier: bool,
) -> Option<ResolvedAnswerSchema> {
    let resolved = raw.and_then(|value| {
        if let Some(preset_name) = value.as_str() {
            match preset_name {
                "outcome_review" => Some(json!({
                    "type": "object",
                    "properties": {
                        "outcome_review": {
                            "type": "object",
                            "properties": {
                                "criteria": {"type": "object"},
                                "total": {"type": "number"},
                                "summary": {"type": "string"},
                                "reasoning": {"type": "string"}
                            },
                            "required": ["total"]
                        }
                    },
                    "required": ["outcome_review"]
                })),
                "full_verifier" => None,
                _ => None,
            }
        } else if value.is_object() {
            Some(value)
        } else {
            None
        }
    });

    resolved.map(|schema| ResolvedAnswerSchema {
        schema,
        is_verifier,
    })
}

pub fn submit_answer_schema(
    is_verifier: bool,
    answer_schema: &Option<ResolvedAnswerSchema>,
) -> Value {
    if let Some(resolved) = answer_schema {
        let schema = resolved.schema.clone();
        let required = schema
            .get("required")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>().join(", "))
            .unwrap_or_default();
        let description = if required.is_empty() {
            "Submit your final structured answer.".to_string()
        } else {
            format!(
                "Submit your final structured answer. The answer MUST contain: {}.",
                required
            )
        };
        json!({
            "type": "function",
            "function": {
                "name": "submit_answer",
                "description": description,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "answer": schema,
                        "confidence": {"type": "string"},
                        "reasoning_summary": {"type": "string"}
                    },
                    "required": ["answer"]
                }
            }
        })
    } else if is_verifier {
        json!({
            "type": "function",
            "function": {
                "name": "submit_answer",
                "description": "Submit your final structured answer for verifier tasks.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "answer": {
                            "type": "object",
                            "properties": {
                                "event_reviews": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "criteria": {"type": "object"},
                                            "total": {"type": "number"},
                                            "summary": {"type": "string"}
                                        },
                                        "required": ["criteria", "total", "summary"]
                                    }
                                },
                                "outcome_review": {
                                    "type": "object",
                                    "properties": {
                                        "criteria": {"type": "object"},
                                        "total": {"type": "number"},
                                        "summary": {"type": "string"}
                                    },
                                    "required": ["criteria", "total", "summary"]
                                },
                                "event_totals": {
                                    "type": "array",
                                    "items": {"type": "number"}
                                }
                            },
                            "required": ["event_reviews", "outcome_review", "event_totals"]
                        },
                        "confidence": {"type": "string"},
                        "reasoning_summary": {"type": "string"}
                    },
                    "required": ["answer"]
                }
            }
        })
    } else {
        json!({
            "type": "function",
            "function": {
                "name": "submit_answer",
                "description": "Submit your final answer.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "answer": {},
                        "confidence": {"type": "string"},
                        "reasoning_summary": {"type": "string"}
                    },
                    "required": ["answer"]
                }
            }
        })
    }
}

pub fn validate_answer_simple(answer: &Value, schema: &Value) -> Option<String> {
    if let Some(required) = schema.get("required").and_then(|v| v.as_array()) {
        if !answer.is_object() {
            return Some(format!(
                "Expected object, got {}",
                match answer {
                    Value::Null => "null",
                    Value::Bool(_) => "boolean",
                    Value::Number(_) => "number",
                    Value::String(_) => "string",
                    Value::Array(_) => "array",
                    Value::Object(_) => "object",
                }
            ));
        }

        let answer_obj = answer.as_object().unwrap();
        for req in required {
            if let Some(field_name) = req.as_str() {
                if !answer_obj.contains_key(field_name) {
                    return Some(format!("Missing required field: '{}'", field_name));
                }

                if let Some(properties) = schema.get("properties").and_then(|v| v.as_object()) {
                    if let Some(field_schema) = properties.get(field_name) {
                        if let Some(nested_required) =
                            field_schema.get("required").and_then(|v| v.as_array())
                        {
                            let nested_value = answer_obj.get(field_name);
                            if let Some(nested_obj) = nested_value.and_then(|v| v.as_object()) {
                                for nested_req in nested_required {
                                    if let Some(nested_field) = nested_req.as_str() {
                                        if !nested_obj.contains_key(nested_field) {
                                            return Some(format!(
                                                "Missing required field: '{}.{}'",
                                                field_name, nested_field
                                            ));
                                        }
                                    }
                                }
                            } else {
                                return Some(format!("Field '{}' must be an object", field_name));
                            }
                        }
                    }
                }
            }
        }
    }

    None
}
