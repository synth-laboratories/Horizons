from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Dict, Optional
from uuid import UUID

import httpx

from . import exceptions

DEFAULT_TIMEOUT = 30.0


class HorizonsClient:
    """Async HTTP client for Horizons REST API."""

    def __init__(
        self,
        base_url: str,
        org_id: UUID,
        *,
        project_id: Optional[UUID] = None,
        user_id: Optional[UUID] = None,
        user_email: Optional[str] = None,
        agent_id: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.org_id = org_id
        self.project_id = project_id
        self.user_id = user_id
        self.user_email = user_email
        self.agent_id = agent_id
        self.api_key = api_key
        self._client = client or httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def __aenter__(self) -> "HorizonsClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers: Dict[str, str] = {"x-org-id": str(self.org_id)}
        if self.project_id:
            headers["x-project-id"] = str(self.project_id)
        if self.user_id:
            headers["x-user-id"] = str(self.user_id)
            if self.user_email:
                headers["x-user-email"] = self.user_email
        if self.agent_id:
            headers["x-agent-id"] = self.agent_id
        if self.api_key:
            headers["authorization"] = f"Bearer {self.api_key}"
        if extra:
            headers.update(extra)
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        resp = await self._client.request(
            method,
            path,
            params=params,
            json=json,
            headers=self._headers(headers),
            timeout=DEFAULT_TIMEOUT,
            follow_redirects=False,
        )
        return resp

    @staticmethod
    def _map_error(resp: httpx.Response) -> exceptions.HorizonsError:
        if resp.status_code == 404:
            return exceptions.NotFoundError(resp.text)
        if resp.status_code in (401, 403):
            return exceptions.AuthError(resp.text)
        if 400 <= resp.status_code < 500:
            return exceptions.ValidationError(resp.text)
        return exceptions.ServerError(resp.text)

    async def json(self, resp: httpx.Response) -> Any:
        if 200 <= resp.status_code < 300:
            if resp.headers.get("content-length") == "0":
                return None
            return resp.json()
        raise self._map_error(resp)

    async def sse(self, path: str, *, json: Any) -> AsyncIterator[Dict[str, Any]]:
        async with self._client.stream(
            "POST",
            path,
            json=json,
            headers=self._headers({"accept": "text/event-stream"}),
        ) as resp:
            if resp.status_code >= 400:
                raise self._map_error(resp)
            async for line in resp.aiter_lines():
                if not line:
                    continue
                if line.startswith(":"):
                    continue
                if line.startswith("data:"):
                    data_str = line[len("data:") :].strip()
                    try:
                        yield httpx.Response(200, text=data_str).json()
                    except Exception as exc:  # pragma: no cover - defensive
                        raise exceptions.StreamError(f"invalid SSE data: {data_str}") from exc
                # ignore other SSE fields for simplicity

    # Convenience module accessors (lazy imports to avoid cycles)
    @property
    def onboard(self):
        from .onboard import OnboardAPI

        return OnboardAPI(self)

    @property
    def events(self):
        from .events import EventsAPI

        return EventsAPI(self)

    @property
    def agents(self):
        from .agents import AgentsAPI

        return AgentsAPI(self)

    @property
    def actions(self):
        from .actions import ActionsAPI

        return ActionsAPI(self)

    @property
    def memory(self):
        from .memory import MemoryAPI

        return MemoryAPI(self)

    @property
    def context_refresh(self):
        from .context_refresh import ContextRefreshAPI

        return ContextRefreshAPI(self)

    @property
    def optimization(self):
        from .optimization import OptimizationAPI

        return OptimizationAPI(self)

    @property
    def evaluation(self):
        from .evaluation import EvaluationAPI

        return EvaluationAPI(self)

    @property
    def engine(self):
        from .engine import EngineAPI

        return EngineAPI(self)

    async def health(self) -> Any:
        resp = await self._request("GET", "/api/v1/health")
        return await self.json(resp)

