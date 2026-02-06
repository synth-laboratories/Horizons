import { HorizonsClient } from "./client";
import { UUID } from "./types";

export type MemoryType = "observation" | "summary" | "action";

export interface MemoryItem {
  id?: string;
  scope: Record<string, string>;
  item_type: MemoryType;
  content: unknown;
  index_text?: string;
  importance_0_to_1?: number;
  created_at: string;
}

export interface Summary {
  agent_id: string;
  horizon: string;
  content: unknown;
  at: string;
}

export class MemoryAPI {
  constructor(private client: HorizonsClient) {}

  async retrieve(agent_id: string, q: string, limit = 20): Promise<MemoryItem[]> {
    const qs = new URLSearchParams({ agent_id, q, limit: String(limit) });
    return await this.client.request<MemoryItem[]>(`/api/v1/memory?${qs.toString()}`, { method: "GET" });
  }

  async append(input: {
    agent_id: string;
    item_type: MemoryType;
    content: unknown;
    index_text?: string;
    importance_0_to_1?: number;
    created_at?: string;
  }): Promise<string> {
    const resp = await this.client.request<{ id: string }>("/api/v1/memory", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(input),
    });
    return resp.id;
  }

  async summarize(agent_id: string, horizon: string): Promise<Summary> {
    const resp = await this.client.request<Summary>("/api/v1/memory/summarize", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ agent_id, horizon }),
    });
    return resp;
  }
}
