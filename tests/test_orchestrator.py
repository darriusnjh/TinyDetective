"""Non-network orchestrator-adjacent tests."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from models.schemas import InvestigationCreateRequest, InvestigationStatus, SourceProduct, TaskStatus
from services.investigation_orchestrator import InvestigationOrchestrator
from services.investigation_store import InvestigationStore
from services.tinyfish_client import TinyFishRun


def test_investigation_request_defaults_comparison_sites() -> None:
    request = InvestigationCreateRequest(
        source_urls=["https://brand.example/products/alpha-case"],
    )
    assert request.comparison_sites == []


class BlockingSourceAgent:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def run(self, source_url: str, on_update=None):
        self.started.set()
        await self.release.wait()
        return SourceProduct(source_url=source_url, brand="Brand"), {"runtime": "stub"}


class EmptyDiscoveryAgent:
    async def run(
        self,
        source_product: SourceProduct,
        comparison_sites: list[str],
        top_n: int = 3,
        on_update=None,
    ):
        return [], []


class SummaryAgent:
    async def run(self, source_product: SourceProduct | None, top_matches: list[object], error: str | None = None):
        return error or "Finished summary"


class UpdatingSourceAgent:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def run(self, source_url: str, on_update=None):
        if on_update is not None:
            await on_update(
                TinyFishRun(
                    run_id="run-source-123",
                    status="RUNNING",
                    elapsed_seconds=12.5,
                    last_heartbeat_at=datetime(2026, 3, 21, 10, 0, 5, tzinfo=timezone.utc),
                    last_progress_at=datetime(2026, 3, 21, 10, 0, 3, tzinfo=timezone.utc),
                )
            )
        self.started.set()
        await self.release.wait()
        return SourceProduct(source_url=source_url, brand="Brand"), {"tinyfish_run_id": "run-source-123"}


def test_orchestrator_persists_inflight_task_progress(tmp_path) -> None:
    async def run() -> None:
        store = InvestigationStore(tmp_path / "orchestrator-progress.sqlite3")
        source_agent = BlockingSourceAgent()
        orchestrator = InvestigationOrchestrator(
            store=store,
            source_agent=source_agent,
            discovery_agent=EmptyDiscoveryAgent(),
            summary_agent=SummaryAgent(),
        )
        created = await store.create(
            InvestigationCreateRequest(
                source_urls=["https://brand.example/products/alpha-case"],
                comparison_sites=["https://shopee.sg/"],
            )
        )

        investigation_task = asyncio.create_task(orchestrator.run_investigation(created.investigation_id))
        await asyncio.wait_for(source_agent.started.wait(), timeout=1.0)

        in_progress = await store.get(created.investigation_id)
        assert in_progress is not None
        assert in_progress.status == InvestigationStatus.running
        assert len(in_progress.reports) == 1
        assert in_progress.reports[0].summary == "Extracting official product details."
        assert len(in_progress.reports[0].raw_agent_outputs) == 1
        assert in_progress.reports[0].raw_agent_outputs[0].agent_name == "source_extraction"
        assert in_progress.reports[0].raw_agent_outputs[0].status == TaskStatus.running

        source_agent.release.set()
        await asyncio.wait_for(investigation_task, timeout=1.0)

    asyncio.run(run())


def test_orchestrator_persists_provider_heartbeat_updates(tmp_path) -> None:
    async def run() -> None:
        store = InvestigationStore(tmp_path / "orchestrator-heartbeat.sqlite3")
        source_agent = UpdatingSourceAgent()
        orchestrator = InvestigationOrchestrator(
            store=store,
            source_agent=source_agent,
            discovery_agent=EmptyDiscoveryAgent(),
            summary_agent=SummaryAgent(),
        )
        created = await store.create(
            InvestigationCreateRequest(
                source_urls=["https://brand.example/products/alpha-case"],
                comparison_sites=["https://shopee.sg/"],
            )
        )

        investigation_task = asyncio.create_task(orchestrator.run_investigation(created.investigation_id))
        await asyncio.wait_for(source_agent.started.wait(), timeout=1.0)

        in_progress = await store.get(created.investigation_id)
        assert in_progress is not None
        source_task = in_progress.reports[0].raw_agent_outputs[0]
        assert source_task.provider_run_id == "run-source-123"
        assert source_task.provider_status == "RUNNING"
        assert source_task.last_heartbeat_at == datetime(2026, 3, 21, 10, 0, 5, tzinfo=timezone.utc)
        assert source_task.last_progress_at == datetime(2026, 3, 21, 10, 0, 3, tzinfo=timezone.utc)
        assert source_task.output_payload["runtime"]["tinyfish_run_id"] == "run-source-123"

        source_agent.release.set()
        await asyncio.wait_for(investigation_task, timeout=1.0)

    asyncio.run(run())


def test_investigation_store_persists_across_instances(tmp_path) -> None:
    async def run() -> None:
        database_path = tmp_path / "investigations.sqlite3"
        store = InvestigationStore(database_path)
        created = await store.create(
            InvestigationCreateRequest(
                source_urls=["https://brand.example/products/alpha-case"],
                comparison_sites=["https://shopee.sg/"],
            )
        )

        created.status = InvestigationStatus.completed
        await store.save(created)

        reloaded_store = InvestigationStore(database_path)
        saved_request = await reloaded_store.get_request(created.investigation_id)
        saved_investigation = await reloaded_store.get(created.investigation_id)

        assert saved_request.source_urls == ["https://brand.example/products/alpha-case"]
        assert saved_request.comparison_sites == ["https://shopee.sg/"]
        assert saved_investigation is not None
        assert saved_investigation.investigation_id == created.investigation_id
        assert saved_investigation.status == InvestigationStatus.completed

    asyncio.run(run())
