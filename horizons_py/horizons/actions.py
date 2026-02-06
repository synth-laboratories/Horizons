from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import UUID

from .client import HorizonsClient
from . import models


class ActionsAPI:
    def __init__(self, client: HorizonsClient) -> None:
        self._client = client

    async def propose(
        self,
        *,
        agent_id: str,
        action_type: str,
        payload: Any,
        risk_level: models.RiskLevel,
        dedupe_key: Optional[str] = None,
        context: Any,
        ttl_seconds: Optional[int] = None,
        project_id: Optional[UUID] = None,
    ) -> UUID:
        body: Dict[str, Any] = {
            "agent_id": agent_id,
            "action_type": action_type,
            "payload": payload,
            "risk_level": risk_level.value,
            "context": context,
        }
        if dedupe_key is not None:
            body["dedupe_key"] = dedupe_key
        if ttl_seconds is not None:
            body["ttl_seconds"] = ttl_seconds
        if project_id:
            body["project_id"] = str(project_id)
        resp = await self._client._request("POST", "/api/v1/actions/propose", json=body)
        data = await self._client.json(resp)
        return UUID(data["action_id"])

    async def approve(
        self, action_id: UUID, *, reason: str, project_id: Optional[UUID] = None
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {"reason": reason}
        if project_id:
            body["project_id"] = str(project_id)
        resp = await self._client._request(
            "POST", f"/api/v1/actions/{action_id}/approve", json=body
        )
        return await self._client.json(resp)

    async def deny(
        self, action_id: UUID, *, reason: str, project_id: Optional[UUID] = None
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {"reason": reason}
        if project_id:
            body["project_id"] = str(project_id)
        resp = await self._client._request(
            "POST", f"/api/v1/actions/{action_id}/deny", json=body
        )
        return await self._client.json(resp)

    async def pending(
        self, *, project_id: Optional[UUID] = None, limit: int = 100, offset: int = 0
    ) -> List[models.ActionProposal]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if project_id:
            params["project_id"] = str(project_id)
        resp = await self._client._request("GET", "/api/v1/actions/pending", params=params)
        data = await self._client.json(resp)
        return [models.ActionProposal.model_validate(item) for item in data]
