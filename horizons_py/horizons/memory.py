from __future__ import annotations

from typing import Any, Dict, List, Optional

from .client import HorizonsClient
from . import models


class MemoryAPI:
    def __init__(self, client: HorizonsClient) -> None:
        self._client = client

    async def retrieve(self, *, agent_id: str, q: str, limit: int = 20) -> List[models.MemoryItem]:
        params = {"agent_id": agent_id, "q": q, "limit": limit}
        resp = await self._client._request("GET", "/api/v1/memory", params=params)
        data = await self._client.json(resp)
        return [models.MemoryItem.model_validate(item) for item in data]

    async def append(
        self,
        *,
        agent_id: str,
        item_type: models.MemoryType,
        content: Any,
        index_text: Optional[str] = None,
        importance_0_to_1: Optional[float] = None,
        created_at: Optional[str] = None,
    ) -> str:
        body: Dict[str, Any] = {
            "agent_id": agent_id,
            "item_type": item_type.value,
            "content": content,
        }
        if index_text:
            body["index_text"] = index_text
        if importance_0_to_1 is not None:
            body["importance_0_to_1"] = importance_0_to_1
        if created_at:
            body["created_at"] = created_at
        resp = await self._client._request("POST", "/api/v1/memory", json=body)
        data = await self._client.json(resp)
        return data["id"]

    async def summarize(self, *, agent_id: str, horizon: str) -> models.Summary:
        body = {"agent_id": agent_id, "horizon": horizon}
        resp = await self._client._request("POST", "/api/v1/memory/summarize", json=body)
        data = await self._client.json(resp)
        return models.Summary.model_validate(data)
