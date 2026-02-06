//! MCP gateway implementation.
//!
//! Implements `McpClient` by routing tool calls to configured child MCP servers.
//! Supports stdio (JSON-RPC 2.0 over stdin/stdout) and HTTP (JSON-RPC POST).

use crate::core_agents::mcp::{
    McpClient, McpScopeProvider, McpToolCall, McpToolResult, ensure_scopes,
};
use crate::{Error, Result};
use async_trait::async_trait;
use chrono::Utc;
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpServerConfig {
    pub name: String,
    pub transport: McpTransport,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum McpTransport {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: HashMap<String, String>,
    },
    Http {
        url: String,
        #[serde(default)]
        headers: HashMap<String, String>,
    },
}

struct StdioServer {
    child: Child,
    stdin: ChildStdin,
    stdout: tokio::io::Lines<BufReader<ChildStdout>>,
}

impl StdioServer {
    async fn spawn(command: &str, args: &[String], env: &HashMap<String, String>) -> Result<Self> {
        let mut cmd = Command::new(command);
        cmd.args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());
        for (k, v) in env {
            cmd.env(k, v);
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| Error::backend("spawn mcp stdio server", e))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::BackendMessage("mcp stdio missing stdin".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::BackendMessage("mcp stdio missing stdout".to_string()))?;
        let stdout = BufReader::new(stdout).lines();
        Ok(Self {
            child,
            stdin,
            stdout,
        })
    }

    async fn jsonrpc_call(
        &mut self,
        id: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });

        let line = serde_json::to_string(&req).map_err(|e| Error::backend("jsonrpc encode", e))?;
        self.stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| Error::backend("jsonrpc write", e))?;
        self.stdin
            .write_all(b"\n")
            .await
            .map_err(|e| Error::backend("jsonrpc write newline", e))?;
        self.stdin
            .flush()
            .await
            .map_err(|e| Error::backend("jsonrpc flush", e))?;

        // Read lines until we find a response matching the ID.
        // This serializes requests per server via the outer mutex.
        while let Some(line) = self
            .stdout
            .next_line()
            .await
            .map_err(|e| Error::backend("jsonrpc read", e))?
        {
            let v: serde_json::Value =
                serde_json::from_str(&line).map_err(|e| Error::backend("jsonrpc decode", e))?;
            if v.get("id").and_then(|x| x.as_str()) != Some(id) {
                continue;
            }
            if let Some(err) = v.get("error") {
                return Err(Error::BackendMessage(format!("mcp jsonrpc error: {err}")));
            }
            if let Some(result) = v.get("result") {
                return Ok(result.clone());
            }
            return Err(Error::BackendMessage(
                "mcp jsonrpc response missing result".to_string(),
            ));
        }

        Err(Error::BackendMessage(
            "mcp stdio server closed stdout".to_string(),
        ))
    }

    async fn shutdown(&mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

enum McpServerHandle {
    Stdio {
        server: Box<Mutex<StdioServer>>,
    },
    Http {
        url: String,
        headers: HashMap<String, String>,
        client: HttpClient,
    },
}

impl McpServerHandle {
    async fn shutdown(&self) {
        if let Self::Stdio { server, .. } = self {
            let mut s = server.lock().await;
            s.shutdown().await;
        }
    }

    async fn tools_list(&self, request_id: &str) -> Result<Vec<String>> {
        let result = match self {
            Self::Stdio { server, .. } => {
                let mut s = server.lock().await;
                s.jsonrpc_call(request_id, "tools/list", serde_json::json!({}))
                    .await?
            }
            Self::Http {
                url,
                headers,
                client,
                ..
            } => {
                let req = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "tools/list",
                    "params": {},
                });
                let mut r = client.post(format!("{}/call", url.trim_end_matches('/')));
                for (k, v) in headers {
                    r = r.header(k, v);
                }
                let resp = r.json(&req).send().await.map_err(Error::backend_reqwest)?;
                let v: serde_json::Value = resp.json().await.map_err(Error::backend_reqwest)?;
                if let Some(err) = v.get("error") {
                    return Err(Error::BackendMessage(format!(
                        "mcp http jsonrpc error: {err}"
                    )));
                }
                v.get("result").cloned().ok_or_else(|| {
                    Error::BackendMessage("mcp http response missing result".to_string())
                })?
            }
        };

        // Accept common shapes:
        // 1) {"tools":[{"name":"x"}, ...]}
        // 2) [{"name":"x"}, ...]
        let mut out = Vec::new();
        if let Some(arr) = result.as_array() {
            for t in arr {
                if let Some(name) = t.get("name").and_then(|v| v.as_str()) {
                    out.push(name.to_string());
                }
            }
        } else if let Some(arr) = result.get("tools").and_then(|v| v.as_array()) {
            for t in arr {
                if let Some(name) = t.get("name").and_then(|v| v.as_str()) {
                    out.push(name.to_string());
                }
            }
        }
        Ok(out)
    }

    async fn tool_call(
        &self,
        request_id: &str,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let params = serde_json::json!({
            "name": tool_name,
            "arguments": arguments,
        });

        match self {
            Self::Stdio { server, .. } => {
                let mut s = server.lock().await;
                s.jsonrpc_call(request_id, "tools/call", params).await
            }
            Self::Http {
                url,
                headers,
                client,
                ..
            } => {
                let req = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "tools/call",
                    "params": params,
                });
                let mut r = client.post(format!("{}/call", url.trim_end_matches('/')));
                for (k, v) in headers {
                    r = r.header(k, v);
                }
                let resp = r.json(&req).send().await.map_err(Error::backend_reqwest)?;
                let v: serde_json::Value = resp.json().await.map_err(Error::backend_reqwest)?;
                if let Some(err) = v.get("error") {
                    return Err(Error::BackendMessage(format!(
                        "mcp http jsonrpc error: {err}"
                    )));
                }
                v.get("result").cloned().ok_or_else(|| {
                    Error::BackendMessage("mcp http response missing result".to_string())
                })
            }
        }
    }
}

pub struct McpGateway {
    servers: RwLock<HashMap<String, McpServerHandle>>,
    scope_provider: Arc<dyn McpScopeProvider>,
}

impl McpGateway {
    #[tracing::instrument(level = "debug", skip(scope_provider))]
    pub fn new(scope_provider: Arc<dyn McpScopeProvider>) -> Self {
        Self {
            servers: RwLock::new(HashMap::new()),
            scope_provider,
        }
    }

    fn split_tool_name(
        &self,
        tool_name: &str,
        server_names: &[String],
    ) -> Result<(String, String)> {
        if let Some((server, tool)) = tool_name.split_once('.') {
            if server.trim().is_empty() || tool.trim().is_empty() {
                return Err(Error::InvalidInput("invalid tool name".to_string()));
            }
            return Ok((server.to_string(), tool.to_string()));
        }

        if server_names.len() == 1 {
            return Ok((server_names[0].clone(), tool_name.to_string()));
        }

        Err(Error::InvalidInput(
            "tool name must be prefixed with '<server>.<tool>' when multiple MCP servers are configured"
                .to_string(),
        ))
    }

    #[tracing::instrument(level = "info", skip(self, configs))]
    pub async fn reconfigure(&self, configs: Vec<McpServerConfig>) -> Result<()> {
        let mut next: HashMap<String, McpServerHandle> = HashMap::new();

        for cfg in configs {
            if cfg.name.trim().is_empty() {
                return Err(Error::InvalidInput("mcp server name is empty".to_string()));
            }

            let handle = match &cfg.transport {
                McpTransport::Stdio { command, args, env } => {
                    if command.trim().is_empty() {
                        return Err(Error::InvalidInput(
                            "mcp stdio command is empty".to_string(),
                        ));
                    }
                    let server = StdioServer::spawn(command, args, env).await?;
                    McpServerHandle::Stdio {
                        server: Box::new(Mutex::new(server)),
                    }
                }
                McpTransport::Http { url, headers } => {
                    if url.trim().is_empty() {
                        return Err(Error::InvalidInput("mcp http url is empty".to_string()));
                    }
                    McpServerHandle::Http {
                        url: url.clone(),
                        headers: headers.clone(),
                        client: HttpClient::new(),
                    }
                }
            };

            next.insert(cfg.name, handle);
        }

        // Swap and shutdown removed/old handles.
        let mut servers = self.servers.write().await;
        let old = std::mem::replace(&mut *servers, next);
        drop(servers);

        for (_, h) in old {
            h.shutdown().await;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn list_tools(&self) -> Result<Vec<String>> {
        let servers = self.servers.read().await;
        let mut out = Vec::new();
        for (name, h) in servers.iter() {
            let request_id = format!("tools_list:{}:{}", name, Uuid::new_v4());
            let tools = h.tools_list(&request_id).await.unwrap_or_default();
            for t in tools {
                out.push(format!("{name}.{t}"));
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }
}

#[async_trait]
impl McpClient for McpGateway {
    #[tracing::instrument(level = "info", skip(self, call))]
    async fn call_tool(&self, call: McpToolCall) -> Result<McpToolResult> {
        // Scope check before routing.
        ensure_scopes(
            self.scope_provider.as_ref(),
            &call.identity,
            &call.requested_scopes,
        )
        .await?;

        let servers = self.servers.read().await;
        if servers.is_empty() {
            return Err(Error::InvalidInput(
                "mcp gateway not configured".to_string(),
            ));
        }
        let server_names: Vec<String> = servers.keys().cloned().collect();
        let (server, tool) = self.split_tool_name(&call.tool_name, &server_names)?;

        let handle = servers
            .get(&server)
            .ok_or_else(|| Error::NotFound(format!("mcp server not found: {server}")))?;

        let output = handle
            .tool_call(&call.request_id, &tool, call.arguments.clone())
            .await?;

        Ok(McpToolResult {
            request_id: call.request_id,
            ok: true,
            output,
            finished_at: Utc::now(),
        })
    }

    fn name(&self) -> &'static str {
        "mcp_gateway"
    }
}
