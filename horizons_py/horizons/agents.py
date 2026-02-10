from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Optional
from uuid import UUID

from .client import HorizonsClient
from . import models


class AgentsAPI:
    """Agent execution API methods."""

    def __init__(self, client: HorizonsClient) -> None:
        self._client = client

    async def run(
        self, *, agent_id: str, inputs: Optional[Dict[str, Any]] = None, project_id: Optional[UUID] = None
    ) -> models.AgentRunResult:
        """Run an agent once and return the typed run result."""
        body: Dict[str, Any] = {"agent_id": agent_id}
        if inputs is not None:
            body["inputs"] = inputs
        if project_id:
            body["project_id"] = str(project_id)
        resp = await self._client._request("POST", "/api/v1/agents/run", json=body)
        data = await self._client.json(resp)
        return models.AgentRunResult.model_validate(data["result"])

    async def chat_stream(
        self,
        *,
        agent_id: str,
        inputs: Optional[Dict[str, Any]] = None,
        project_id: Optional[UUID] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream agent chat events from `/api/v1/agents/chat` (SSE)."""
        body: Dict[str, Any] = {"agent_id": agent_id}
        if inputs is not None:
            body["inputs"] = inputs
        if project_id:
            body["project_id"] = str(project_id)
        async for event in self._client.sse("/api/v1/agents/chat", json=body):
            yield event

    async def list_registered(self) -> List[str]:
        """List registered agent identifiers for the current tenant scope."""
        resp = await self._client._request("GET", "/api/v1/agents")
        data = await self._client.json(resp)
        return list(data)
