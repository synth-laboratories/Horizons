use crate::o11y::traits::{OtelSpan, RedactionLevel};
use regex::Regex;

#[derive(Clone)]
pub struct RedactionPolicy {
    level: RedactionLevel,
    extra: Vec<Regex>,
}

impl RedactionPolicy {
    #[tracing::instrument(level = "debug")]
    pub fn new(level: RedactionLevel, extra_patterns: Vec<String>) -> crate::Result<Self> {
        let mut extra = Vec::with_capacity(extra_patterns.len());
        for p in extra_patterns {
            let re = Regex::new(&p).map_err(|e| {
                crate::Error::InvalidInput(format!("invalid redaction regex {p:?}: {e}"))
            })?;
            extra.push(re);
        }
        Ok(Self { level, extra })
    }

    #[tracing::instrument(level = "debug", skip(self, span))]
    pub fn redact_span(&self, span: &mut OtelSpan) {
        // Apply level-based removal first to avoid leaking large blobs.
        self.apply_level(&mut span.attributes);
        for ev in &mut span.events {
            self.apply_level(&mut ev.attributes);
        }

        // Apply PII/sensitive-key and regex redaction.
        redact_map(&self.extra, &mut span.attributes);
        for ev in &mut span.events {
            redact_map(&self.extra, &mut ev.attributes);
        }
    }

    fn apply_level(&self, attrs: &mut std::collections::BTreeMap<String, serde_json::Value>) {
        match self.level {
            RedactionLevel::Full => {}
            RedactionLevel::Metadata => {
                drop_prefix(attrs, "gen_ai.prompt");
                drop_prefix(attrs, "gen_ai.completion");
                drop_prefix(attrs, "gen_ai.request.messages");
                drop_prefix(attrs, "gen_ai.response.messages");
                drop_prefix(attrs, "mcp.tool.args");
                drop_prefix(attrs, "mcp.tool.result");
            }
            RedactionLevel::Off => {
                drop_prefix(attrs, "gen_ai.");
                drop_prefix(attrs, "mcp.tool.args");
                drop_prefix(attrs, "mcp.tool.result");
            }
        }
    }
}

fn drop_prefix(attrs: &mut std::collections::BTreeMap<String, serde_json::Value>, prefix: &str) {
    let keys: Vec<String> = attrs
        .keys()
        .filter(|k| k.starts_with(prefix))
        .cloned()
        .collect();
    for k in keys {
        attrs.remove(&k);
    }
}

fn redact_map(extra: &[Regex], attrs: &mut std::collections::BTreeMap<String, serde_json::Value>) {
    for (k, v) in attrs.iter_mut() {
        if is_sensitive_key(k) || extra.iter().any(|re| re.is_match(k)) {
            *v = serde_json::Value::String("[REDACTED]".to_string());
            continue;
        }
        redact_value(extra, v);
    }
}

fn redact_value(extra: &[Regex], v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Null => {}
        serde_json::Value::Bool(_) => {}
        serde_json::Value::Number(_) => {}
        serde_json::Value::String(s) => {
            if looks_like_secret(s) || extra.iter().any(|re| re.is_match(s)) {
                *s = "[REDACTED]".to_string();
            }
        }
        serde_json::Value::Array(a) => {
            for e in a {
                redact_value(extra, e);
            }
        }
        serde_json::Value::Object(o) => {
            for (k, v) in o.iter_mut() {
                if is_sensitive_key(k) || extra.iter().any(|re| re.is_match(k)) {
                    *v = serde_json::Value::String("[REDACTED]".to_string());
                } else {
                    redact_value(extra, v);
                }
            }
        }
    }
}

fn is_sensitive_key(k: &str) -> bool {
    let k = k.to_ascii_lowercase();
    k.contains("authorization")
        || k.contains("password")
        || k.contains("passwd")
        || k.contains("secret")
        || k.contains("token")
        || k.contains("api_key")
        || k.contains("apikey")
        || k.contains("cookie")
        || k.contains("set-cookie")
        || k.contains("ssn")
        || k.contains("private_key")
}

fn looks_like_secret(s: &str) -> bool {
    let s = s.trim();
    if s.len() < 12 {
        return false;
    }
    // Common high-entropy prefixes.
    let lower = s.to_ascii_lowercase();
    lower.starts_with("bearer ")
        || lower.starts_with("basic ")
        || lower.starts_with("sk-")
        || lower.starts_with("xox")
}
