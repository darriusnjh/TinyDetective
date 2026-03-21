"""SQLite-backed investigation persistence."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from uuid import uuid4

from models.schemas import (
    ActivityLogEntry,
    InvestigationCreateRequest,
    InvestigationListItem,
    InvestigationResponse,
    InvestigationStatus,
    utc_now,
)
from services.settings import settings

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class InvestigationStore:
    """Persist investigation state in SQLite."""

    def __init__(self, database_path: str | Path | None = None) -> None:
        raw_database_path = str(database_path or settings.investigation_store_path)
        if raw_database_path == ":memory:":
            self._database_path: Path | None = None
            self._database_target = raw_database_path
        else:
            resolved_path = Path(raw_database_path).expanduser()
            if not resolved_path.is_absolute():
                resolved_path = PROJECT_ROOT / resolved_path
            self._database_path = resolved_path
            self._database_target = str(resolved_path)
        self._lock = asyncio.Lock()
        self._initialize_database()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._database_target)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize_database(self) -> None:
        if self._database_path is not None:
            self._database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS investigations (
                    investigation_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_investigations_updated_at
                ON investigations(updated_at DESC)
                """
            )

    def _create_sync(self, payload: InvestigationCreateRequest) -> InvestigationResponse:
        investigation_id = str(uuid4())
        item = InvestigationResponse(
            investigation_id=investigation_id,
            status=InvestigationStatus.queued,
        )
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO investigations (
                    investigation_id,
                    status,
                    request_json,
                    response_json,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    investigation_id,
                    item.status.value,
                    payload.model_dump_json(),
                    item.model_dump_json(),
                    item.created_at.isoformat(),
                    item.updated_at.isoformat(),
                ),
            )
        return item.model_copy(deep=True)

    def _get_sync(self, investigation_id: str) -> InvestigationResponse | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT response_json FROM investigations WHERE investigation_id = ?",
                (investigation_id,),
            ).fetchone()
        if row is None:
            return None
        return InvestigationResponse.model_validate_json(row["response_json"])

    def _get_request_sync(self, investigation_id: str) -> InvestigationCreateRequest:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT request_json FROM investigations WHERE investigation_id = ?",
                (investigation_id,),
            ).fetchone()
        if row is None:
            raise KeyError(investigation_id)
        return InvestigationCreateRequest.model_validate_json(row["request_json"])

    def _save_sync(self, item: InvestigationResponse) -> None:
        existing = self._get_sync(item.investigation_id)
        if existing is not None and len(existing.activity_log) > len(item.activity_log):
            item.activity_log = existing.activity_log
        with self._connect() as connection:
            updated_at = item.updated_at.isoformat()
            cursor = connection.execute(
                """
                UPDATE investigations
                SET status = ?, response_json = ?, updated_at = ?
                WHERE investigation_id = ?
                """,
                (
                    item.status.value,
                    item.model_dump_json(),
                    updated_at,
                    item.investigation_id,
                ),
            )
        if cursor.rowcount == 0:
            raise KeyError(item.investigation_id)

    def _append_activity_sync(self, investigation_id: str, entry: ActivityLogEntry) -> None:
        item = self._get_sync(investigation_id)
        if item is None:
            return
        item.activity_log.append(entry)
        item.updated_at = utc_now()
        self._save_sync(item)

    def _list_active_sync(self) -> list[InvestigationResponse]:
        active_statuses = (
            InvestigationStatus.queued.value,
            InvestigationStatus.running.value,
            InvestigationStatus.delayed.value,
        )
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT response_json
                FROM investigations
                WHERE status IN (?, ?, ?)
                ORDER BY created_at ASC
                """,
                active_statuses,
            ).fetchall()
        return [InvestigationResponse.model_validate_json(row["response_json"]) for row in rows]

    def _list_recent_sync(self, limit: int) -> list[InvestigationListItem]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT investigation_id, status, request_json, response_json, created_at, updated_at
                FROM investigations
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        items: list[InvestigationListItem] = []
        for row in rows:
            request_payload = InvestigationCreateRequest.model_validate_json(row["request_json"])
            response_payload = InvestigationResponse.model_validate_json(row["response_json"])
            source_urls = [str(source_url) for source_url in request_payload.source_urls]
            primary_report = response_payload.reports[0] if response_payload.reports else None
            primary_source_product = primary_report.extracted_source_product if primary_report else None
            primary_source_title = None
            if primary_source_product is not None:
                primary_source_title = (
                    primary_source_product.product_name
                    or primary_source_product.model
                    or primary_source_product.brand
                )
            items.append(
                InvestigationListItem(
                    investigation_id=row["investigation_id"],
                    status=InvestigationStatus(row["status"]),
                    primary_source_url=source_urls[0] if source_urls else None,
                    primary_source_title=primary_source_title,
                    source_count=len(source_urls),
                    error=response_payload.error,
                    created_at=response_payload.created_at,
                    updated_at=response_payload.updated_at,
                )
            )
        return items

    async def create(self, payload: InvestigationCreateRequest) -> InvestigationResponse:
        async with self._lock:
            return await asyncio.to_thread(self._create_sync, payload)

    async def get(self, investigation_id: str) -> InvestigationResponse | None:
        async with self._lock:
            return await asyncio.to_thread(self._get_sync, investigation_id)

    async def get_request(self, investigation_id: str) -> InvestigationCreateRequest:
        async with self._lock:
            return await asyncio.to_thread(self._get_request_sync, investigation_id)

    async def save(self, item: InvestigationResponse) -> None:
        async with self._lock:
            item.updated_at = utc_now()
            await asyncio.to_thread(self._save_sync, item)

    async def list_active(self) -> list[InvestigationResponse]:
        async with self._lock:
            return await asyncio.to_thread(self._list_active_sync)

    async def list_recent(self, limit: int = 12) -> list[InvestigationListItem]:
        async with self._lock:
            return await asyncio.to_thread(self._list_recent_sync, limit)

    async def append_activity(self, investigation_id: str, entry: ActivityLogEntry) -> None:
        async with self._lock:
            await asyncio.to_thread(self._append_activity_sync, investigation_id, entry)
