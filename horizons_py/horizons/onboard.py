from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import UUID

from .client import HorizonsClient
from . import models


class OnboardAPI:
    def __init__(self, client: HorizonsClient) -> None:
        self._client = client

    async def create_project(self, project_id: Optional[UUID] = None) -> models.ProjectDbHandle:
        payload: Dict[str, Any] = {}
        if project_id:
            payload["project_id"] = str(project_id)
        resp = await self._client._request("POST", "/api/v1/projects", json=payload)
        data = await self._client.json(resp)
        return models.ProjectDbHandle.model_validate(data["handle"])

    async def list_projects(self, *, limit: int = 100, offset: int = 0) -> List[models.ProjectDbHandle]:
        resp = await self._client._request(
            "GET", "/api/v1/projects", params={"limit": limit, "offset": offset}
        )
        data = await self._client.json(resp)
        return [models.ProjectDbHandle.model_validate(item) for item in data]

    async def query(self, project_id: UUID, sql: str, params: Optional[list] = None) -> Dict[str, Any]:
        resp = await self._client._request(
            "POST",
            f"/api/v1/projects/{project_id}/query",
            json={"sql": sql, "params": params or []},
        )
        return await self._client.json(resp)

    async def execute(self, project_id: UUID, sql: str, params: Optional[list] = None) -> Dict[str, Any]:
        resp = await self._client._request(
            "POST",
            f"/api/v1/projects/{project_id}/execute",
            json={"sql": sql, "params": params or []},
        )
        return await self._client.json(resp)
