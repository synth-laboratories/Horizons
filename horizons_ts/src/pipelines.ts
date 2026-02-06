import { HorizonsClient } from "./client";
import { PipelineRun } from "./types";

export class PipelinesAPI {
  constructor(private client: HorizonsClient) {}

  async run(input: { spec: Record<string, unknown>; inputs?: unknown }): Promise<PipelineRun> {
    return await this.client.request<PipelineRun>("/api/v1/pipelines/run", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ spec: input.spec, inputs: input.inputs ?? {} }),
    });
  }

  async getRun(runId: string): Promise<PipelineRun> {
    return await this.client.request<PipelineRun>(`/api/v1/pipelines/runs/${runId}`, {
      method: "GET",
    });
  }

  async approve(runId: string, stepId: string): Promise<Record<string, unknown>> {
    return await this.client.request(`/api/v1/pipelines/runs/${runId}/approve/${stepId}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });
  }

  async cancel(runId: string): Promise<Record<string, unknown>> {
    return await this.client.request(`/api/v1/pipelines/runs/${runId}/cancel`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });
  }
}

