use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct LmError {
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct ToolError {
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RlmRunError {
    #[error("lm error: {0}")]
    Lm(String),
    #[error("tool error: {0}")]
    Tool(String),
    #[error("execution error: {0}")]
    Execution(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RlmErrorKind {
    NoSubmitAnswer,
    LimitReached,
    GiveUp,
    ExecutionError,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RlmErrorInfo {
    pub kind: RlmErrorKind,
    pub message: String,
    pub details: Option<Value>,
}
