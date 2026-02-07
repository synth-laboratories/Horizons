import { HorizonsClient } from "./client";
import { Event, EventDirection, EventStatus, Subscription, SubscriptionConfig, SubscriptionHandler } from "./types";

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

  async subscribe(input: {
    topic_pattern: string;
    direction: EventDirection;
    handler: SubscriptionHandler;
    config?: SubscriptionConfig;
    filter?: Record<string, unknown> | null;
  }): Promise<string> {
    const resp = await this.client.request<{ subscription_id: string }>("/api/v1/subscriptions", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(input),
    });
    return resp.subscription_id;
  }

  async listSubscriptions(): Promise<Subscription[]> {
    return await this.client.request<Subscription[]>("/api/v1/subscriptions", { method: "GET" });
  }

  async unsubscribe(subscription_id: string): Promise<void> {
    await this.client.request(`/api/v1/subscriptions/${subscription_id}`, { method: "DELETE" });
  }
}
