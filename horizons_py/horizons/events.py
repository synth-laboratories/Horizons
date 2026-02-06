from __future__ import annotations

from typing import Any, Dict, List, Optional

from .client import HorizonsClient
from . import models


class EventsAPI:
    def __init__(self, client: HorizonsClient) -> None:
        self._client = client

    async def list(
        self,
        *,
        project_id: Optional[str] = None,
        topic: Optional[str] = None,
        direction: Optional[models.EventDirection] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        status: Optional[models.EventStatus] = None,
        limit: int = 100,
    ) -> List[models.Event]:
        params: Dict[str, Any] = {"limit": limit}
        if project_id:
            params["project_id"] = project_id
        if topic:
            params["topic"] = topic
        if direction:
            params["direction"] = direction.value
        if since:
            params["since"] = since
        if until:
            params["until"] = until
        if status:
            params["status"] = status.value
        resp = await self._client._request("GET", "/api/v1/events", params=params)
        data = await self._client.json(resp)
        return [models.Event.model_validate(evt) for evt in data]

    async def publish(
        self,
        *,
        topic: str,
        source: str,
        payload: Any,
        dedupe_key: str,
        direction: models.EventDirection = models.EventDirection.outbound,
        project_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[str] = None,
    ) -> str:
        body: Dict[str, Any] = {
            "topic": topic,
            "source": source,
            "payload": payload,
            "dedupe_key": dedupe_key,
            "direction": direction.value,
        }
        if project_id:
            body["project_id"] = project_id
        if metadata is not None:
            body["metadata"] = metadata
        if timestamp:
            body["timestamp"] = timestamp
        resp = await self._client._request("POST", "/api/v1/events/publish", json=body)
        data = await self._client.json(resp)
        return data.get("event_id")

    async def subscribe(
        self,
        *,
        topic_pattern: str,
        direction: models.EventDirection,
        handler: models.SubscriptionHandler,
        config: Optional[models.SubscriptionConfig] = None,
        filter: Optional[Dict[str, Any]] = None,
    ) -> str:
        body: Dict[str, Any] = {
            "topic_pattern": topic_pattern,
            "direction": direction.value,
            "handler": handler.model_dump(mode="json"),
        }
        if config:
            body["config"] = config.model_dump(mode="json")
        if filter is not None:
            body["filter"] = filter
        resp = await self._client._request("POST", "/api/v1/subscriptions", json=body)
        data = await self._client.json(resp)
        return data.get("subscription_id")
