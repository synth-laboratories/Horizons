import { HorizonsClient } from "./client";
import { Event, EventDirection, EventStatus } from "./types";

export class EventsAPI {
  constructor(private client: HorizonsClient) {}

  async publish(input: {
    topic: string;
    source: string;
    payload: unknown;
    dedupe_key: string;
    direction?: EventDirection;
    project_id?: string;
    metadata?: Record<string, unknown>;
    timestamp?: string;
  }): Promise<string> {
    const body = { direction: "outbound", ...input };
    const resp = await this.client.request<{ event_id: string }>("/api/v1/events/publish", {
      method: "POST",
      body: JSON.stringify(body),
      headers: { "content-type": "application/json" },
    });
    return resp.event_id;
  }

  async list(params: {
    project_id?: string;
    topic?: string;
    direction?: EventDirection;
    since?: string;
    until?: string;
    status?: EventStatus;
    limit?: number;
  } = {}): Promise<Event[]> {
    const search = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null) search.append(k, String(v));
    });
    const path = `/api/v1/events${search.toString() ? `?${search.toString()}` : ""}`;
    return await this.client.request<Event[]>(path, { method: "GET" });
  }
}
