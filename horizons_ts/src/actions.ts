import { HorizonsClient } from "./client";
import { ActionProposal, RiskLevel, UUID } from "./types";

export class ActionsAPI {
  constructor(private client: HorizonsClient) {}

  async propose(input: {
    agent_id: string;
    action_type: string;
    payload: unknown;
    risk_level: RiskLevel;
    dedupe_key: string;
    context: unknown;
    ttl_seconds?: number;
    project_id?: string;
  }): Promise<UUID> {
    const resp = await this.client.request<{ action_id: UUID }>("/api/v1/actions/propose", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(input),
    });
    return resp.action_id;
  }

  async approve(actionId: UUID, reason: string, project_id?: string): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = { reason };
    if (project_id) body.project_id = project_id;
    return await this.client.request(`/api/v1/actions/${actionId}/approve`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  async deny(actionId: UUID, reason: string, project_id?: string): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = { reason };
    if (project_id) body.project_id = project_id;
    return await this.client.request(`/api/v1/actions/${actionId}/deny`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  async pending(limit = 100, offset = 0, project_id?: string): Promise<ActionProposal[]> {
    const params = new URLSearchParams({ limit: String(limit), offset: String(offset) });
    if (project_id) params.append("project_id", project_id);
    const path = `/api/v1/actions/pending?${params.toString()}`;
    return await this.client.request<ActionProposal[]>(path, { method: "GET" });
  }
}
