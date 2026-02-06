"""SDK module for Horizons Engine / Sandbox endpoints.

Covers: POST /api/v1/engine/run, POST /api/v1/engine/start,
GET /api/v1/engine/:handle_id/events (SSE),
POST /api/v1/engine/:handle_id/release,
GET /api/v1/engine/:handle_id/health.
"""
from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Optional

from .client import HorizonsClient
from . import exceptions


class EngineAPI:
    """Client for the sandbox engine lifecycle endpoints."""

    def __init__(self, client: HorizonsClient) -> None:
        self._client = client

    # -----------------------------------------------------------------
    # POST /api/v1/engine/run  (one-shot)
    # -----------------------------------------------------------------

    async def run(
        self,
        *,
        agent: str,
        instruction: str,
        model: Optional[str] = None,
        permission_mode: str = "bypass",
        image: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
        timeout_seconds: int = 1800,
    ) -> Dict[str, Any]:
        """Run a coding agent to completion in a sandbox and return the result.

        Args:
            agent: Which agent to run — ``"codex"``, ``"claude"``, or ``"opencode"``.
            instruction: The instruction to send to the agent.
            model: LLM model override (e.g. ``"gpt-5-nano"``).
            permission_mode: ``"bypass"`` (default) or ``"plan"``.
            image: Docker image override.
            env_vars: Environment variables to inject (typically API keys).
            timeout_seconds: Maximum seconds the session may run.

        Returns:
            Dict with ``handle_id``, ``completed``, ``duration_seconds``,
            ``event_count``, ``final_output``, ``error``.
        """
        body: Dict[str, Any] = {
            "agent": agent,
            "instruction": instruction,
            "permission_mode": permission_mode,
            "timeout_seconds": timeout_seconds,
        }
        if model is not None:
            body["model"] = model
        if image is not None:
            body["image"] = image
        if env_vars:
            body["env_vars"] = env_vars

        resp = await self._client._request("POST", "/api/v1/engine/run", json=body)
        return await self._client.json(resp)

    # -----------------------------------------------------------------
    # POST /api/v1/engine/start  (async — returns handle for SSE)
    # -----------------------------------------------------------------

    async def start(
        self,
        *,
        agent: str,
        instruction: str,
        model: Optional[str] = None,
        permission_mode: str = "bypass",
        image: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
        timeout_seconds: int = 1800,
    ) -> Dict[str, Any]:
        """Start a coding agent without waiting for completion.

        Returns a handle that can be used with :meth:`events` (SSE)
        and :meth:`release`.

        Returns:
            Dict with ``handle_id``, ``session_id``, ``sandbox_agent_url``.
        """
        body: Dict[str, Any] = {
            "agent": agent,
            "instruction": instruction,
            "permission_mode": permission_mode,
            "timeout_seconds": timeout_seconds,
        }
        if model is not None:
            body["model"] = model
        if image is not None:
            body["image"] = image
        if env_vars:
            body["env_vars"] = env_vars

        resp = await self._client._request("POST", "/api/v1/engine/start", json=body)
        return await self._client.json(resp)

    # -----------------------------------------------------------------
    # GET /api/v1/engine/:handle_id/events  (SSE stream)
    # -----------------------------------------------------------------

    async def events(self, handle_id: str) -> AsyncIterator[Dict[str, Any]]:
        """Stream events from a running sandbox agent session via SSE.

        Yields dicts for each ``event`` SSE message. Stops when a ``done``
        or ``error`` event is received, or when the connection closes.
        """
        import httpx

        async with self._client._client.stream(
            "GET",
            f"/api/v1/engine/{handle_id}/events",
            headers=self._client._headers({"accept": "text/event-stream"}),
        ) as resp:
            if resp.status_code >= 400:
                body = ""
                async for chunk in resp.aiter_text():
                    body += chunk
                raise self._client._map_error(
                    httpx.Response(resp.status_code, text=body)
                )

            event_type: Optional[str] = None
            async for line in resp.aiter_lines():
                if not line:
                    event_type = None
                    continue
                if line.startswith(":"):
                    continue
                if line.startswith("event:"):
                    event_type = line[len("event:"):].strip()
                    continue
                if line.startswith("data:"):
                    data_str = line[len("data:"):].strip()

                    if event_type == "done":
                        return
                    if event_type == "error":
                        raise exceptions.StreamError(
                            f"sandbox event error: {data_str}"
                        )

                    if data_str:
                        try:
                            import json
                            yield json.loads(data_str)
                        except Exception:
                            # Non-JSON data line — skip gracefully.
                            pass

    # -----------------------------------------------------------------
    # POST /api/v1/engine/:handle_id/release
    # -----------------------------------------------------------------

    async def release(self, handle_id: str) -> Dict[str, Any]:
        """Release (tear down) a sandbox by its handle.

        Returns:
            Dict with ``released`` (bool) and ``handle_id``.
        """
        resp = await self._client._request(
            "POST", f"/api/v1/engine/{handle_id}/release"
        )
        return await self._client.json(resp)

    # -----------------------------------------------------------------
    # GET /api/v1/engine/:handle_id/health
    # -----------------------------------------------------------------

    async def health(self, handle_id: str) -> Dict[str, Any]:
        """Check health of the sandbox-agent in a running container.

        Returns:
            Dict with ``healthy`` (bool).
        """
        resp = await self._client._request(
            "GET", f"/api/v1/engine/{handle_id}/health"
        )
        return await self._client.json(resp)
