import uuid

import pytest
import respx
import httpx

from horizons.client import HorizonsClient
from horizons import models


ORG_ID = uuid.uuid4()
PROJECT_ID = uuid.uuid4()


@pytest.mark.asyncio
async def test_create_and_list_projects():
    with respx.mock(base_url="http://localhost:8000") as mock:
        mock.post("/api/v1/projects").respond(200, json={"handle": {
            "org_id": str(ORG_ID),
            "project_id": str(PROJECT_ID),
            "connection_url": "libsql://test",
            "auth_token": None,
        }})
        mock.get("/api/v1/projects").respond(200, json=[{
            "org_id": str(ORG_ID),
            "project_id": str(PROJECT_ID),
            "connection_url": "libsql://test",
            "auth_token": None,
        }])

        async with HorizonsClient("http://localhost:8000", ORG_ID) as client:
            handle = await client.onboard.create_project(PROJECT_ID)
            assert handle.project_id == PROJECT_ID
            projects = await client.onboard.list_projects()
            assert projects[0].org_id == ORG_ID

        request = mock.calls[0].request
        assert request.headers["x-org-id"] == str(ORG_ID)


@pytest.mark.asyncio
async def test_publish_event_and_list():
    with respx.mock(base_url="http://localhost:8000") as mock:
        mock.post("/api/v1/events/publish").respond(200, json={"event_id": "evt_1"})
        mock.get("/api/v1/events").respond(200, json=[{
            "id": "evt_1",
            "org_id": str(ORG_ID),
            "project_id": str(PROJECT_ID),
            "timestamp": "2024-01-01T00:00:00Z",
            "received_at": "2024-01-01T00:00:00Z",
            "topic": "demo",
            "source": "test",
            "direction": "outbound",
            "payload": {"hello": "world"},
            "dedupe_key": "k",
            "metadata": {},
            "status": "pending",
            "retry_count": 0,
            "last_attempt_at": None
        }])

        async with HorizonsClient("http://localhost:8000", ORG_ID, project_id=PROJECT_ID) as client:
            evt_id = await client.events.publish(topic="demo", source="test", payload={}, dedupe_key="k")
            assert evt_id == "evt_1"
            events = await client.events.list()
            assert events[0].topic == "demo"


@pytest.mark.asyncio
async def test_run_agent():
    with respx.mock(base_url="http://localhost:8000") as mock:
        mock.post("/api/v1/agents/run").respond(200, json={
            "result": {
                "run_id": str(uuid.uuid4()),
                "org_id": str(ORG_ID),
                "project_id": str(PROJECT_ID),
                "agent_id": "demo",
                "started_at": "2024-01-01T00:00:00Z",
                "finished_at": "2024-01-01T00:00:01Z",
                "proposed_action_ids": []
            }
        })

        async with HorizonsClient("http://localhost:8000", ORG_ID, project_id=PROJECT_ID, agent_id="agent:demo") as client:
            res = await client.agents.run(agent_id="demo")
            assert res.agent_id == "demo"
            # headers include identity
            call = mock.calls[0].request
            assert call.headers["x-agent-id"] == "agent:demo"
