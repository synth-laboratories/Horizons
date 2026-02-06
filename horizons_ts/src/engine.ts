/**
 * SDK module for Horizons Engine / Sandbox endpoints.
 *
 * Covers: POST /api/v1/engine/run, POST /api/v1/engine/start,
 * GET /api/v1/engine/:handle_id/events (SSE),
 * POST /api/v1/engine/:handle_id/release,
 * GET /api/v1/engine/:handle_id/health.
 */
import { HorizonsClient, HorizonsError } from "./client";

// -- Request / Response types ------------------------------------------------

export type AgentKind = "codex" | "claude" | "opencode";
export type PermissionMode = "bypass" | "plan";

export interface RunEngineRequest {
  agent: AgentKind;
  instruction: string;
  model?: string;
  permission_mode?: PermissionMode;
  image?: string;
  env_vars?: Record<string, string>;
  timeout_seconds?: number;
}

export interface RunEngineResponse {
  handle_id: string;
  completed: boolean;
  duration_seconds: number;
  event_count: number;
  final_output?: string | null;
  error?: string | null;
}

export interface StartEngineResponse {
  handle_id: string;
  session_id: string;
  sandbox_agent_url: string;
}

export interface EngineHealthResponse {
  healthy: boolean;
}

export interface ReleaseResponse {
  released: boolean;
  handle_id: string;
}

// -- API class ---------------------------------------------------------------

export class EngineAPI {
  constructor(private client: HorizonsClient) {}

  /**
   * Run a coding agent to completion in a sandbox (one-shot).
   */
  async run(req: RunEngineRequest): Promise<RunEngineResponse> {
    const body: Record<string, unknown> = {
      agent: req.agent,
      instruction: req.instruction,
      permission_mode: req.permission_mode ?? "bypass",
      timeout_seconds: req.timeout_seconds ?? 1800,
    };
    if (req.model !== undefined) body.model = req.model;
    if (req.image !== undefined) body.image = req.image;
    if (req.env_vars !== undefined) body.env_vars = req.env_vars;

    return await this.client.request<RunEngineResponse>("/api/v1/engine/run", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  /**
   * Start a coding agent without waiting — returns handle for SSE streaming.
   */
  async start(req: RunEngineRequest): Promise<StartEngineResponse> {
    const body: Record<string, unknown> = {
      agent: req.agent,
      instruction: req.instruction,
      permission_mode: req.permission_mode ?? "bypass",
      timeout_seconds: req.timeout_seconds ?? 1800,
    };
    if (req.model !== undefined) body.model = req.model;
    if (req.image !== undefined) body.image = req.image;
    if (req.env_vars !== undefined) body.env_vars = req.env_vars;

    return await this.client.request<StartEngineResponse>("/api/v1/engine/start", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  /**
   * Stream events from a running sandbox agent session via SSE.
   *
   * Yields parsed event objects. Stops on "done" or "error" events.
   */
  async *events(handleId: string): AsyncGenerator<Record<string, unknown>> {
    const headers = this.client["headers"]({
      accept: "text/event-stream",
    }) as Record<string, string>;

    const resp = await fetch(`${this.client.baseUrl}/api/v1/engine/${handleId}/events`, {
      method: "GET",
      headers,
    });

    if (!resp.ok || !resp.body) {
      throw new HorizonsError(`engine events failed: ${resp.statusText}`, resp.status);
    }

    const reader = resp.body.getReader();
    let buffer = "";
    let currentEvent = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += new TextDecoder().decode(value);
      let sepIndex: number;

      while ((sepIndex = buffer.indexOf("\n\n")) >= 0) {
        const chunk = buffer.slice(0, sepIndex);
        buffer = buffer.slice(sepIndex + 2);

        const lines = chunk.split("\n");
        let eventType = "";
        let dataStr = "";

        for (const line of lines) {
          if (line.startsWith("event:")) {
            eventType = line.slice(6).trim();
          } else if (line.startsWith("data:")) {
            dataStr = line.slice(5).trim();
          }
        }

        if (eventType === "done") return;
        if (eventType === "error") {
          throw new HorizonsError(`sandbox event error: ${dataStr}`);
        }

        if (dataStr) {
          try {
            yield JSON.parse(dataStr);
          } catch {
            // Non-JSON data — skip gracefully.
          }
        }
      }
    }
  }

  /**
   * Release (tear down) a sandbox by its handle.
   */
  async release(handleId: string): Promise<ReleaseResponse> {
    return await this.client.request<ReleaseResponse>(`/api/v1/engine/${handleId}/release`, {
      method: "POST",
    });
  }

  /**
   * Check health of the sandbox-agent in a running container.
   */
  async health(handleId: string): Promise<EngineHealthResponse> {
    return await this.client.request<EngineHealthResponse>(`/api/v1/engine/${handleId}/health`, {
      method: "GET",
    });
  }
}
