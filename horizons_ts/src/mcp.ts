import { HorizonsClient } from "./client";

export interface McpServerConfig {
  name: string;
  transport: unknown;
}

export interface McpConfigResponse {
  ok: boolean;
  server_count: number;
}

export interface McpToolResult {
  output: unknown;
  ok: boolean;
  error?: string | null;
  metadata?: Record<string, unknown>;
}

export class McpAPI {
  constructor(private client: HorizonsClient) {}

  async configure(servers: McpServerConfig[]): Promise<McpConfigResponse> {
    return await this.client.request<McpConfigResponse>("/api/v1/mcp/config", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ servers }),
    });
  }

  async listTools(): Promise<string[]> {
    const resp = await this.client.request<{ tools: string[] }>("/api/v1/mcp/tools", { method: "GET" });
    return resp.tools ?? [];
  }

  async call(input: {
    tool_name: string;
    arguments?: unknown;
    request_id?: string;
    requested_at?: string;
    metadata?: Record<string, string>;
  }): Promise<McpToolResult> {
    // Near-term hardening: identity and scope selection are derived server-side from
    // the authenticated request, not caller-supplied payload fields.
    const body = {
      tool_name: input.tool_name,
      ...(input.arguments !== undefined ? { arguments: input.arguments } : {}),
      ...(input.request_id ? { request_id: input.request_id } : {}),
      ...(input.requested_at ? { requested_at: input.requested_at } : {}),
      ...(input.metadata ? { metadata: input.metadata } : {}),
    };
    return await this.client.request<McpToolResult>("/api/v1/mcp/call", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }
}
