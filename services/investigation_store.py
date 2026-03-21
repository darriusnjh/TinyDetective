"""In-memory investigation persistence."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from uuid import uuid4

from models.schemas import (
    ActivityLogEntry,
    InvestigationCreateRequest,
    InvestigationResponse,
    InvestigationStatus,
    utc_now,
)


class InvestigationStore:
    """Store investigation state in memory for the MVP."""

    def __init__(self) -> None:
        self._items: dict[str, InvestigationResponse] = {}
        self._requests: dict[str, InvestigationCreateRequest] = {}
        self._lock = asyncio.Lock()

    async def create(self, payload: InvestigationCreateRequest) -> InvestigationResponse:
        async with self._lock:
            investigation_id = str(uuid4())
            self._requests[investigation_id] = payload
            item = InvestigationResponse(
                investigation_id=investigation_id,
                status=InvestigationStatus.queued,
            )
            self._items[investigation_id] = item
            return deepcopy(item)

    async def get(self, investigation_id: str) -> InvestigationResponse | None:
        async with self._lock:
            item = self._items.get(investigation_id)
            return deepcopy(item) if item else None

    async def get_request(self, investigation_id: str) -> InvestigationCreateRequest:
        async with self._lock:
            return deepcopy(self._requests[investigation_id])

    async def save(self, item: InvestigationResponse) -> None:
        async with self._lock:
            existing = self._items.get(item.investigation_id)
            if existing is not None and len(existing.activity_log) > len(item.activity_log):
                item.activity_log = deepcopy(existing.activity_log)
            item.updated_at = utc_now()
            self._items[item.investigation_id] = deepcopy(item)

    async def append_activity(self, investigation_id: str, entry: ActivityLogEntry) -> None:
        async with self._lock:
            item = self._items.get(investigation_id)
            if item is None:
                return
            item.activity_log.append(entry)
            item.updated_at = utc_now()
            self._items[investigation_id] = deepcopy(item)
