# Horizons TypeScript SDK

TypeScript SDK for the Horizons REST API.

Use this SDK to run agents, propose/approve actions, publish events, and
integrate Horizons into Node.js services and web backends.

## Install

```bash
npm install @horizons-ai/sdk
```

## Quickstart

```ts
import {
  HorizonsClient,
  OnboardAPI,
  EventsAPI,
  AgentsAPI,
  ActionsAPI,
} from "@horizons-ai/sdk";

async function main(): Promise<void> {
  const orgId = "00000000-0000-0000-0000-000000000000";
  const client = new HorizonsClient("http://localhost:8000", orgId, {
    agentId: "agent:demo",
    // apiKey: process.env.HORIZONS_API_KEY,
  });

  const onboard = new OnboardAPI(client);
  const events = new EventsAPI(client);
  const agents = new AgentsAPI(client);
  const actions = new ActionsAPI(client);

  // Create project
  const handle = await onboard.createProject();
  const projectId = handle.project_id;

  // Publish event
  await events.publish({
    topic: "demo.hello",
    source: "horizons_ts_readme",
    payload: { hello: "world" },
    dedupe_key: "horizons-ts-readme-demo-1",
  });

  // Run agent
  const run = await agents.run("dev.noop", { prompt: "hello" }, projectId);
  console.log("run_id:", run.run_id);

  // Propose + approve action (approval gate flow)
  const actionId = await actions.propose({
    agent_id: "agent:demo",
    action_type: "demo.notify",
    payload: { message: "hello from ts sdk" },
    risk_level: "low",
    context: { source: "readme" },
    dedupe_key: "horizons-ts-readme-action-1",
    project_id: projectId,
  });
  await actions.approve(actionId, "readme demo", projectId);
  const pending = await actions.pending(100, 0, projectId);
  console.log("pending_actions:", pending.length);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

## API modules

- `OnboardAPI` - projects and tenant setup
- `EventsAPI` - publish/query event bus
- `AgentsAPI` - run agents and stream SSE chat
- `ActionsAPI` - propose/approve/deny actions
- `ContextRefreshAPI` - connectors and refresh runs
- `EngineAPI` - sandbox execution APIs
- `PipelinesAPI` - multi-step pipeline runs
- `McpAPI` - MCP server config and tool calls

## Build

```bash
npm run build
```

