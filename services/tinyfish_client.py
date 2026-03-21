"""TinyFish API client."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib import error, request

from services.settings import settings


class TinyFishError(RuntimeError):
    """Raised when TinyFish returns an error or unexpected payload."""


@dataclass
class TinyFishRun:
    run_id: str
    status: str
    result: Any = None
    error: Any = None
    raw: dict[str, Any] | None = None
    elapsed_seconds: float | None = None
    delayed: bool = False
    last_heartbeat_at: datetime | None = None
    last_progress_at: datetime | None = None


TinyFishRunUpdateCallback = Callable[[TinyFishRun], Awaitable[None] | None]


class TinyFishClient:
    """Minimal TinyFish HTTP client using the documented async run flow."""

    def __init__(self) -> None:
        self.base_url = settings.tinyfish_base_url.rstrip("/")
        self.api_key = settings.tinyfish_api_key
        self.browser_profile = settings.tinyfish_browser_profile

    async def run_json(
        self,
        url: str,
        goal: str,
        on_update: TinyFishRunUpdateCallback | None = None,
    ) -> TinyFishRun:
        run_id = await self.start_run(url, goal)
        return await self.wait_for_run(run_id, on_update=on_update)

    async def start_run(self, url: str, goal: str) -> str:
        if not self.api_key:
            raise TinyFishError("TINYFISH_API_KEY is not configured.")
        payload: dict[str, Any] = {
            "url": url,
            "goal": goal,
            "browser_profile": self.browser_profile,
            "api_integration": "tinydetective",
        }
        if settings.tinyfish_proxy_enabled:
            payload["proxy_config"] = {
                "enabled": True,
                "country_code": settings.tinyfish_proxy_country_code,
            }
        response = await asyncio.to_thread(
            self._request_json,
            "POST",
            f"{self.base_url}/v1/automation/run-async",
            payload,
        )
        run_id = response.get("run_id")
        if not run_id:
            raise TinyFishError(f"TinyFish did not return a run_id: {response}")
        return str(run_id)

    async def wait_for_run(
        self,
        run_id: str,
        on_update: TinyFishRunUpdateCallback | None = None,
    ) -> TinyFishRun:
        started_mono = time.monotonic()
        heartbeat_at = datetime.now(timezone.utc)
        progress_at = heartbeat_at
        last_fingerprint: str | None = None

        while True:
            run = await self.get_run(run_id)
            heartbeat_at = datetime.now(timezone.utc)
            fingerprint = self._fingerprint(run)
            if fingerprint != last_fingerprint:
                last_fingerprint = fingerprint
                progress_at = heartbeat_at

            run.elapsed_seconds = time.monotonic() - started_mono
            run.delayed = run.elapsed_seconds >= settings.tinyfish_run_soft_timeout_seconds
            run.last_heartbeat_at = heartbeat_at
            run.last_progress_at = progress_at

            if on_update is not None:
                maybe_awaitable = on_update(run)
                if asyncio.iscoroutine(maybe_awaitable):
                    await maybe_awaitable

            status = run.status.upper()
            if status == "COMPLETED":
                return run
            if status in {"FAILED", "CANCELLED"}:
                raise TinyFishError(f"TinyFish run {run_id} ended with status {status}: {run.error}")
            await asyncio.sleep(settings.tinyfish_poll_interval_seconds)

    async def get_run(self, run_id: str) -> TinyFishRun:
        response = await asyncio.to_thread(
            self._request_json,
            "POST",
            f"{self.base_url}/v1/runs/batch",
            {"run_ids": [run_id]},
        )
        runs = response.get("data") or []
        if not runs:
            raise TinyFishError(f"TinyFish run {run_id} was not found in batch lookup: {response}")
        run_data = runs[0]
        return TinyFishRun(
            run_id=str(run_data.get("run_id") or run_id),
            status=str(run_data.get("status") or "UNKNOWN"),
            result=self._extract_result_payload(run_data),
            error=run_data.get("error"),
            raw=run_data,
        )

    def _request_json(self, method: str, url: str, payload: dict[str, Any] | None) -> dict[str, Any]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        req = request.Request(
            url=url,
            data=body,
            method=method,
            headers={
                "Content-Type": "application/json",
                "X-API-Key": self.api_key,
            },
        )
        try:
            timeout = None if settings.tinyfish_http_timeout_seconds <= 0 else settings.tinyfish_http_timeout_seconds
            with request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise TinyFishError(f"TinyFish HTTP {exc.code}: {detail}") from exc
        except error.URLError as exc:
            raise TinyFishError(f"Failed to reach TinyFish: {exc.reason}") from exc

    @staticmethod
    def _extract_result_payload(response: dict[str, Any]) -> Any:
        for key in ("resultJson", "result", "data"):
            if key in response and response[key] is not None:
                value = response[key]
                if isinstance(value, str):
                    try:
                        return json.loads(value)
                    except json.JSONDecodeError:
                        return value
                return value
        return None

    @staticmethod
    def _fingerprint(run: TinyFishRun) -> str:
        return json.dumps(
            {
                "status": run.status,
                "result": run.result,
                "error": run.error,
                "raw": run.raw,
            },
            sort_keys=True,
            default=str,
        )
