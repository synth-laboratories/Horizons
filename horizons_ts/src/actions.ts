import { HorizonsClient } from "./client";
import { ActionProposal, RiskLevel, UUID } from "./types";

/** Action lifecycle API (propose, approve, deny, list pending). */
export class ActionsAPI {
  constructor(private client: HorizonsClient) {}

  /** Propose an action for approval and return the action ID. */
  async propose(input: {
    agent_id: string;
    action_type: string;
    payload: unknown;
    risk_level: RiskLevel;
    dedupe_key?: string;
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

  /** Approve a pending action proposal. */
  async approve(actionId: UUID, reason: string, project_id?: string): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = { reason };
    if (project_id) body.project_id = project_id;
    return await this.client.request(`/api/v1/actions/${actionId}/approve`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  /** Deny a pending action proposal. */
  async deny(actionId: UUID, reason: string, project_id?: string): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = { reason };
    if (project_id) body.project_id = project_id;
    return await this.client.request(`/api/v1/actions/${actionId}/deny`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  /** List pending action proposals for the project/org scope. */
  async pending(limit = 100, offset = 0, project_id?: string): Promise<ActionProposal[]> {
    const params = new URLSearchParams({ limit: String(limit), offset: String(offset) });
    if (project_id) params.append("project_id", project_id);
    const path = `/api/v1/actions/pending?${params.toString()}`;
    return await this.client.request<ActionProposal[]>(path, { method: "GET" });
  }
}
