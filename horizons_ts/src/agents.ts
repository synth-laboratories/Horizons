import { HorizonsClient, HorizonsError } from "./client";
import { AgentRunResult } from "./types";

export class AgentsAPI {
  constructor(private client: HorizonsClient) {}

  async run(agent_id: string, inputs?: unknown, project_id?: string): Promise<AgentRunResult> {
    const body: Record<string, unknown> = { agent_id };
    if (inputs !== undefined) body.inputs = inputs;
    if (project_id) body.project_id = project_id;
    const resp = await this.client.request<{ result: AgentRunResult }>("/api/v1/agents/run", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    return resp.result;
  }

  async *chatStream(agent_id: string, inputs?: unknown, project_id?: string): AsyncGenerator<Record<string, unknown>> {
    const body: Record<string, unknown> = { agent_id };
    if (inputs !== undefined) body.inputs = inputs;
    if (project_id) body.project_id = project_id;

    const resp = await fetch(`${this.client.baseUrl}/api/v1/agents/chat`, {
      method: "POST",
      headers: { ...this.client["headers"]({ "content-type": "application/json", accept: "text/event-stream" }) },
      body: JSON.stringify(body),
    });
    if (!resp.ok || !resp.body) {
      throw new HorizonsError(`chat stream failed: ${resp.statusText}`, resp.status);
    }
    const reader = resp.body.getReader();
    let buffer = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += new TextDecoder().decode(value);
      let sepIndex;
      while ((sepIndex = buffer.indexOf("\n\n")) >= 0) {
        const chunk = buffer.slice(0, sepIndex);
        buffer = buffer.slice(sepIndex + 2);
        const dataLine = chunk.split("\n").find((line) => line.startsWith("data:"));
        if (!dataLine) continue;
        const jsonStr = dataLine.replace("data:", "").trim();
        if (!jsonStr) continue;
        try {
          yield JSON.parse(jsonStr);
        } catch (err) {
          throw new HorizonsError(`invalid SSE payload: ${jsonStr}`);
        }
      }
    }
  }

  async listRegistered(): Promise<string[]> {
    return await this.client.request<string[]>("/api/v1/agents", { method: "GET" });
  }
}
