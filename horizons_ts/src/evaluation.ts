import { HorizonsClient } from "./client";
import { EvalReportRow, UUID } from "./types";

export class EvaluationAPI {
  constructor(private client: HorizonsClient) {}

  async run(casePayload: Record<string, unknown>, project_id?: UUID): Promise<EvalReportRow> {
    const body: Record<string, unknown> = { case: casePayload };
    if (project_id) body.project_id = project_id;
    return await this.client.request<EvalReportRow>("/api/v1/eval/run", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  async reports(params: { project_id?: UUID; report_id?: UUID; limit?: number; offset?: number } = {}): Promise<Record<string, unknown>> {
    const search = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null) search.append(k, String(v));
    });
    const path = `/api/v1/eval/reports${search.toString() ? `?${search.toString()}` : ""}`;
    return await this.client.request<Record<string, unknown>>(path, { method: "GET" });
  }
}
