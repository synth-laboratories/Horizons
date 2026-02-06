from __future__ import annotations

from typing import Any, Dict, Optional

from .client import HorizonsClient
from . import models


class PipelinesAPI:
    def __init__(self, client: HorizonsClient) -> None:
        self._client = client

    async def run(self, *, spec: Dict[str, Any], inputs: Optional[Any] = None) -> models.PipelineRun:
        body: Dict[str, Any] = {"spec": spec, "inputs": inputs or {}}
        resp = await self._client._request("POST", "/api/v1/pipelines/run", json=body)
        data = await self._client.json(resp)
        return models.PipelineRun.model_validate(data)

    async def get_run(self, run_id: str) -> models.PipelineRun:
        resp = await self._client._request("GET", f"/api/v1/pipelines/runs/{run_id}")
        data = await self._client.json(resp)
        return models.PipelineRun.model_validate(data)

    async def approve(self, run_id: str, step_id: str) -> Dict[str, Any]:
        resp = await self._client._request(
            "POST", f"/api/v1/pipelines/runs/{run_id}/approve/{step_id}", json={}
        )
        return await self._client.json(resp)

    async def cancel(self, run_id: str) -> Dict[str, Any]:
        resp = await self._client._request("POST", f"/api/v1/pipelines/runs/{run_id}/cancel", json={})
        return await self._client.json(resp)

