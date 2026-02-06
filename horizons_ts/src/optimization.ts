import { HorizonsClient } from "./client";
import { OptimizationRunRow, UUID } from "./types";

export class OptimizationAPI {
  constructor(private client: HorizonsClient) {}

  async run(input: {
    cfg: Record<string, unknown>;
    initial_policy: Record<string, unknown>;
    dataset: Record<string, unknown>;
    project_id?: UUID;
  }): Promise<OptimizationRunRow> {
    const resp = await this.client.request<OptimizationRunRow>("/api/v1/optimization/run", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(input),
    });
    return resp;
  }

  async status(params: { project_id?: UUID; limit?: number; offset?: number } = {}): Promise<OptimizationRunRow[]> {
    const search = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null) search.append(k, String(v));
    });
    const path = `/api/v1/optimization/status${search.toString() ? `?${search.toString()}` : ""}`;
    return await this.client.request<OptimizationRunRow[]>(path, { method: "GET" });
  }

  async reports(params: { project_id?: UUID; run_id?: UUID; limit?: number; offset?: number } = {}): Promise<Record<string, unknown>> {
    const search = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null) search.append(k, String(v));
    });
    const path = `/api/v1/optimization/reports${search.toString() ? `?${search.toString()}` : ""}`;
    return await this.client.request<Record<string, unknown>>(path, { method: "GET" });
  }
}
