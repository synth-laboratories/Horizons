import { describe, it, expect, beforeAll, afterAll, afterEach } from "vitest";
import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";
import { HorizonsClient } from "./client";
import { OnboardAPI } from "./onboard";
import { EventsAPI } from "./events";
import { AgentsAPI } from "./agents";

const BASE = "http://localhost:8000";
const ORG_ID = "00000000-0000-0000-0000-000000000001";
const PROJECT_ID = "00000000-0000-0000-0000-000000000002";
const RUN_ID = "00000000-0000-0000-0000-000000000003";

const server = setupServer();

beforeAll(() => server.listen());
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("OnboardAPI", () => {
  it("creates and lists projects", async () => {
    let capturedHeaders: Headers | undefined;

    server.use(
      http.post(`${BASE}/api/v1/projects`, ({ request }) => {
        capturedHeaders = request.headers;
        return HttpResponse.json({
          handle: {
            org_id: ORG_ID,
            project_id: PROJECT_ID,
            connection_url: "libsql://test",
            auth_token: null,
          },
        });
      }),
      http.get(`${BASE}/api/v1/projects`, () => {
        return HttpResponse.json([
          {
            org_id: ORG_ID,
            project_id: PROJECT_ID,
            connection_url: "libsql://test",
            auth_token: null,
          },
        ]);
      }),
    );

    const client = new HorizonsClient(BASE, ORG_ID);
    const onboard = new OnboardAPI(client);

    const handle = await onboard.createProject(PROJECT_ID);
    expect(handle.project_id).toBe(PROJECT_ID);

    const projects = await onboard.listProjects();
    expect(projects[0].org_id).toBe(ORG_ID);

    expect(capturedHeaders?.get("x-org-id")).toBe(ORG_ID);
  });
});

describe("EventsAPI", () => {
  it("publishes and lists events", async () => {
    server.use(
      http.post(`${BASE}/api/v1/events/publish`, () => {
        return HttpResponse.json({ event_id: "evt_1" });
      }),
      http.get(`${BASE}/api/v1/events`, () => {
        return HttpResponse.json([
          {
            event_id: "evt_1",
            org_id: ORG_ID,
            project_id: PROJECT_ID,
            topic: "demo",
            source: "test",
            direction: "outbound",
            payload: { hello: "world" },
            dedupe_key: "k",
            metadata: {},
            status: "pending",
            created_at: "2024-01-01T00:00:00Z",
          },
        ]);
      }),
    );

    const client = new HorizonsClient(BASE, ORG_ID, { projectId: PROJECT_ID });
    const events = new EventsAPI(client);

    const evtId = await events.publish({
      topic: "demo",
      source: "test",
      payload: {},
      dedupe_key: "k",
    });
    expect(evtId).toBe("evt_1");

    const list = await events.list();
    expect(list[0].topic).toBe("demo");
  });
});

describe("AgentsAPI", () => {
  it("runs an agent with correct headers", async () => {
    let capturedHeaders: Headers | undefined;

    server.use(
      http.post(`${BASE}/api/v1/agents/run`, ({ request }) => {
        capturedHeaders = request.headers;
        return HttpResponse.json({
          result: {
            run_id: RUN_ID,
            org_id: ORG_ID,
            project_id: PROJECT_ID,
            agent_id: "demo",
            started_at: "2024-01-01T00:00:00Z",
            finished_at: "2024-01-01T00:00:01Z",
            proposed_action_ids: [],
          },
        });
      }),
    );

    const client = new HorizonsClient(BASE, ORG_ID, {
      projectId: PROJECT_ID,
      agentId: "agent:demo",
    });
    const agents = new AgentsAPI(client);

    const res = await agents.run("demo");
    expect(res.agent_id).toBe("demo");
    expect(capturedHeaders?.get("x-agent-id")).toBe("agent:demo");
  });
});
