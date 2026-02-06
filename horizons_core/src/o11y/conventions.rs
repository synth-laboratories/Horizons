//! Semantic convention attribute keys used by Horizons spans.

// gen_ai.*
pub const GEN_AI_SYSTEM: &str = "gen_ai.system";
pub const GEN_AI_REQUEST_MODEL: &str = "gen_ai.request.model";
pub const GEN_AI_REQUEST_TEMPERATURE: &str = "gen_ai.request.temperature";
pub const GEN_AI_USAGE_PROMPT_TOKENS: &str = "gen_ai.usage.prompt_tokens";
pub const GEN_AI_USAGE_COMPLETION_TOKENS: &str = "gen_ai.usage.completion_tokens";
pub const GEN_AI_RESPONSE_FINISH_REASON: &str = "gen_ai.response.finish_reason";

// mcp.*
pub const MCP_TOOL_NAME: &str = "mcp.tool.name";
pub const MCP_TOOL_STATUS: &str = "mcp.tool.status";
pub const MCP_TOOL_ARGS: &str = "mcp.tool.args";
pub const MCP_TOOL_RESULT: &str = "mcp.tool.result";

// action.*
pub const ACTION_RISK_LEVEL: &str = "action.risk_level";
pub const ACTION_REVIEW_TYPE: &str = "action.review_type";
pub const ACTION_APPROVED: &str = "action.approved";
