from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import UUID

from .client import HorizonsClient
from . import models


class OptimizationAPI:
    def __init__(self, client: HorizonsClient) -> None:
        self._client = client

    async def run(
        self,
        *,
        cfg: Dict[str, Any],
        initial_policy: Dict[str, Any],
        dataset: Dict[str, Any],
        project_id: Optional[UUID] = None,
    ) -> models.OptimizationRunRow:
        body: Dict[str, Any] = {"cfg": cfg, "initial_policy": initial_policy, "dataset": dataset}
        if project_id:
            body["project_id"] = str(project_id)
        resp = await self._client._request("POST", "/api/v1/optimization/run", json=body)
        data = await self._client.json(resp)
        return models.OptimizationRunRow.model_validate(data)

    async def status(
        self, *, project_id: Optional[UUID] = None, limit: int = 50, offset: int = 0
    ) -> List[models.OptimizationRunRow]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if project_id:
            params["project_id"] = str(project_id)
        resp = await self._client._request("GET", "/api/v1/optimization/status", params=params)
        data = await self._client.json(resp)
        return [models.OptimizationRunRow.model_validate(item) for item in data]

    async def reports(
        self, *, project_id: Optional[UUID] = None, run_id: Optional[UUID] = None, limit: int = 50, offset: int = 0
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if project_id:
            params["project_id"] = str(project_id)
        if run_id:
            params["run_id"] = str(run_id)
        resp = await self._client._request("GET", "/api/v1/optimization/reports", params=params)
        return await self._client.json(resp)
