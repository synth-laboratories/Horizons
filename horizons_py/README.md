# Horizons Python SDK

Async Python SDK for the Horizons REST API.

Use this SDK to run agents, propose/approve actions, publish events, and query
platform state from Python services and workers.

## Install

```bash
pip install horizons
```

## Quickstart

```python
import asyncio
import uuid
from horizons import HorizonsClient
from horizons import models


async def main() -> None:
    org_id = uuid.UUID("00000000-0000-0000-0000-000000000000")

    async with HorizonsClient(
        "http://localhost:8000",
        org_id,
        agent_id="agent:demo",
        # api_key="hzn_<uuid>.<secret>",
    ) as client:
        # Health
        health = await client.health()
        print("health:", health)

        # Create project
        handle = await client.onboard.create_project()
        project_id = handle.project_id
        print("project_id:", project_id)

        # Publish event
        await client.events.publish(
            topic="demo.hello",
            source="horizons_py_readme",
            payload={"hello": "world"},
            dedupe_key="horizons-py-readme-demo-1",
        )

        # Run agent
        run = await client.agents.run(
            agent_id="dev.noop",
            project_id=project_id,
            inputs={"prompt": "hello"},
        )
        print("run_id:", run.run_id)

        # Propose + approve action (approval gate flow)
        action_id = await client.actions.propose(
            agent_id="agent:demo",
            action_type="demo.notify",
            payload={"message": "hello from python sdk"},
            risk_level=models.RiskLevel.low,
            dedupe_key="horizons-py-readme-action-1",
            context={"source": "readme"},
            project_id=project_id,
        )
        await client.actions.approve(action_id, reason="readme demo", project_id=project_id)
        pending = await client.actions.pending(project_id=project_id)
        print("pending_actions:", len(pending))


if __name__ == "__main__":
    asyncio.run(main())
```

## API modules

- `client.onboard` - projects and tenant setup
- `client.events` - publish/query event bus
- `client.agents` - run agents and stream chat SSE
- `client.actions` - propose/approve/deny action lifecycle
- `client.context_refresh` - connectors and refresh runs
- `client.engine` - sandbox execution APIs
- `client.pipelines` - multi-step pipeline runs
- `client.mcp` - MCP server config and tool calls

## Notes

- `base_url` should point to your self-hosted `horizons_server`.
- For production mutating calls, use bearer auth (`api_key=...`).
- In local development, `x-org-id` header scoping is enabled by default.
