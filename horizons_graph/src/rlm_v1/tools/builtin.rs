use serde_json::{Value, json};

pub fn builtin_tools() -> Vec<Value> {
    vec![
        json!({
            "type": "function",
            "function": {
                "name": "materialize_context",
                "description": "Store an input field for searching. By default stores locally only (fast). Set write_to_sandbox=true if you need to run scripts on the file.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "field_name": {"type": "string"},
                        "filename": {"type": "string"},
                        "write_to_sandbox": {"type": "boolean"}
                    },
                    "required": ["field_name", "filename"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "local_grep",
                "description": "Fast local grep on materialized files. Returns up to 15 matches by default. Use offset for pagination.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "pattern": {"type": "string", "description": "Regex pattern to search for"},
                        "file": {"type": "string"},
                        "ignore_case": {"type": "boolean"},
                        "max_matches": {"type": "integer", "description": "Max matches to return (default 15)"},
                        "offset": {"type": "integer", "description": "Skip first N matches for pagination"},
                        "context_lines": {"type": "integer"}
                    },
                    "required": ["pattern", "file"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "local_search",
                "description": "Fast local substring search. Returns up to 15 matches by default. Use offset for pagination.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Substring to search for"},
                        "file": {"type": "string"},
                        "ignore_case": {"type": "boolean"},
                        "max_matches": {"type": "integer", "description": "Max matches to return (default 15)"},
                        "offset": {"type": "integer", "description": "Skip first N matches for pagination"}
                    },
                    "required": ["query", "file"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "view_lines",
                "description": "View a window of lines from a materialized file (default 50, max 100 lines per call).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "file": {"type": "string"},
                        "start_line": {"type": "integer", "description": "1-indexed line number to start from"},
                        "max_lines": {"type": "integer", "description": "Number of lines to return (default 50, max 100)"}
                    },
                    "required": ["file"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "exec_python",
                "description": "Execute Python code in a sandboxed interpreter. The `context` variable contains the default materialized data, and `files` contains all materialized files (filename -> content). Variables persist across calls within one run. Use print() for output. No filesystem or network access.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "code": {
                            "type": "string",
                            "description": "Python code to execute"
                        },
                        "timeout_ms": {
                            "type": "integer",
                            "description": "Max execution time in ms (default 5000)"
                        }
                    },
                    "required": ["code"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "codex_exec",
                "description": "Execute shell command. Simple grep/cat/head/wc on materialized files run locally. Complex commands or scripts run in sandbox.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "command": {"type": "string"},
                        "timeout_seconds": {"type": "integer"},
                        "force_sandbox": {"type": "boolean"}
                    },
                    "required": ["command"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "query_lm",
                "description": "Ask a sub-question to another LLM.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "prompt": {"type": "string"},
                        "model": {"type": "string"}
                    },
                    "required": ["prompt"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "delegate_lm",
                "description": "Call a sub-LM with role and optional structured output.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "prompt": {"type": "string"},
                        "model": {"type": "string"},
                        "role": {"type": "string"},
                        "system_prompt": {"type": "string"},
                        "response_format": {},
                        "json_schema": {"type": "object"}
                    },
                    "required": ["prompt"]
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "give_up",
                "description": "End early when you cannot finish. Provide a short reason.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "reason": {"type": "string"}
                    },
                    "required": []
                }
            }
        }),
        json!({
            "type": "function",
            "function": {
                "name": "add_evidence",
                "description": "Record evidence supporting your evaluation. Use this to capture specific findings from the trace that support rubric criteria rewards. Evidence persists across conversation summaries.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "description": {
                            "type": "string",
                            "description": "Brief description of what this evidence demonstrates (e.g., 'Tests passed', 'Error handling implemented')"
                        },
                        "evidence_snippet": {
                            "type": "string",
                            "description": "The actual evidence text from the trace (e.g., test output, code snippet, log message)"
                        }
                    },
                    "required": ["description", "evidence_snippet"]
                }
            }
        }),
    ]
}
