from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import UUID

from .client import HorizonsClient


class ContextRefreshAPI:
    def __init__(self, client: HorizonsClient) -> None:
        self._client = client

    async def trigger(self, *, source_id: str, trigger: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        body = {"source_id": source_id}
        if trigger:
            body["trigger"] = trigger
        resp = await self._client._request("POST", "/api/v1/context-refresh/run", json=body)
        return await self._client.json(resp)

    async def status(
        self, *, source_id: Optional[str] = None, limit: int = 25, offset: int = 0
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if source_id:
            params["source_id"] = source_id
        resp = await self._client._request("GET", "/api/v1/context-refresh/status", params=params)
        return await self._client.json(resp)

    async def register_source(
        self,
        *,
        project_id: UUID,
        source_id: str,
        connector_id: str,
        scope: str,
        enabled: bool = True,
        schedule_expr: Optional[str] = None,
        event_triggers: Optional[list] = None,
        settings: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "project_id": str(project_id),
            "source_id": source_id,
            "connector_id": connector_id,
            "scope": scope,
            "enabled": enabled,
        }
        if schedule_expr:
            body["schedule_expr"] = schedule_expr
        if event_triggers is not None:
            body["event_triggers"] = event_triggers
        if settings is not None:
            body["settings"] = settings
        resp = await self._client._request("POST", "/api/v1/connectors", json=body)
        return await self._client.json(resp)

    async def list_sources(self) -> List[Dict[str, Any]]:
        resp = await self._client._request("GET", "/api/v1/connectors")
        data = await self._client.json(resp)
        return list(data.get("sources", []))
