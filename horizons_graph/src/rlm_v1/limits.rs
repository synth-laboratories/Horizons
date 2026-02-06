use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RlmLimits {
    pub max_iterations: usize,
    pub max_root_calls: i64,
    pub max_subcalls: i64,
    pub max_time_ms: i64,
    pub max_cost_usd: f64,
    pub max_messages_before_compact: usize,
    pub keep_recent_messages: usize,
    pub prompt_token_budget: Option<usize>,
}

impl Default for RlmLimits {
    fn default() -> Self {
        Self {
            max_iterations: 25,
            max_root_calls: 50,
            max_subcalls: 100,
            max_time_ms: 120_000,
            max_cost_usd: f64::INFINITY,
            max_messages_before_compact: 15,
            keep_recent_messages: 6,
            prompt_token_budget: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct LimitStatus {
    pub hit_iterations: bool,
    pub hit_root_calls: bool,
    pub hit_time: bool,
    pub hit_cost: bool,
}

impl LimitStatus {
    pub fn reasons(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if self.hit_iterations {
            out.push("iterations");
        }
        if self.hit_root_calls {
            out.push("root_calls");
        }
        if self.hit_time {
            out.push("time");
        }
        if self.hit_cost {
            out.push("cost");
        }
        out
    }
}
