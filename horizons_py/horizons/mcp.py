from __future__ import annotations

from typing import Any, Dict, List, Optional

from .client import HorizonsClient


class McpAPI:
    def __init__(self, client: HorizonsClient) -> None:
        self._client = client

    async def configure(self, *, servers: List[Dict[str, Any]]) -> Dict[str, Any]:
        resp = await self._client._request("POST", "/api/v1/mcp/config", json={"servers": servers})
        return await self._client.json(resp)

    async def list_tools(self) -> List[str]:
        resp = await self._client._request("GET", "/api/v1/mcp/tools")
        data = await self._client.json(resp)
        tools = data.get("tools") if isinstance(data, dict) else None
        return list(tools or [])

    async def call(
        self,
        *,
        tool_name: str,
        arguments: Optional[Any] = None,
        request_id: Optional[str] = None,
        requested_at: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        # Near-term hardening:
        # - identity is derived from the authenticated request (headers/bearer token)
        # - scope selection is derived server-side from grants/policy
        body: Dict[str, Any] = {"tool_name": tool_name}
        if arguments is not None:
            body["arguments"] = arguments
        if request_id is not None:
            body["request_id"] = request_id
        if requested_at is not None:
            body["requested_at"] = requested_at
        if metadata is not None:
            body["metadata"] = metadata
        resp = await self._client._request("POST", "/api/v1/mcp/call", json=body)
        return await self._client.json(resp)
