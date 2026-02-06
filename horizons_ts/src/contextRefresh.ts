import { HorizonsClient } from "./client";
import { UUID } from "./types";

export class ContextRefreshAPI {
  constructor(private client: HorizonsClient) {}

  async trigger(source_id: string, trigger?: Record<string, unknown>): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = { source_id };
    if (trigger) body.trigger = trigger;
    return await this.client.request("/api/v1/context-refresh/run", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  async status(params: { source_id?: string; limit?: number; offset?: number } = {}): Promise<Record<string, unknown>> {
    const search = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null) search.append(k, String(v));
    });
    const path = `/api/v1/context-refresh/status${search.toString() ? `?${search.toString()}` : ""}`;
    return await this.client.request(path, { method: "GET" });
  }

  async registerSource(input: {
    project_id: UUID;
    source_id: string;
    connector_id: string;
    scope: string;
    enabled?: boolean;
    schedule_expr?: string;
    event_triggers?: unknown[];
    settings?: Record<string, unknown>;
  }): Promise<Record<string, unknown>> {
    return await this.client.request("/api/v1/connectors", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(input),
    });
  }

  async listSources(): Promise<Record<string, unknown>[]> {
    const resp = await this.client.request<{ sources: Record<string, unknown>[] }>("/api/v1/connectors", { method: "GET" });
    return resp.sources ?? [];
  }
}
