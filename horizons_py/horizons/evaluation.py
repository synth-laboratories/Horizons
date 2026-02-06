from __future__ import annotations

from typing import Any, Dict, Optional
from uuid import UUID

from .client import HorizonsClient
from . import models


class EvaluationAPI:
    def __init__(self, client: HorizonsClient) -> None:
        self._client = client

    async def run(self, *, case: Dict[str, Any], project_id: Optional[UUID] = None) -> models.EvalReportRow:
        body: Dict[str, Any] = {"case": case}
        if project_id:
            body["project_id"] = str(project_id)
        resp = await self._client._request("POST", "/api/v1/eval/run", json=body)
        data = await self._client.json(resp)
        return models.EvalReportRow.model_validate(data)

    async def reports(
        self,
        *,
        project_id: Optional[UUID] = None,
        report_id: Optional[UUID] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if project_id:
            params["project_id"] = str(project_id)
        if report_id:
            params["report_id"] = str(report_id)
        resp = await self._client._request("GET", "/api/v1/eval/reports", params=params)
        return await self._client.json(resp)
