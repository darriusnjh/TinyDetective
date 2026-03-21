"""Non-network orchestrator-adjacent tests."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from models.schemas import (
    AgentTaskState,
    InvestigationCreateRequest,
    InvestigationReport,
    InvestigationStatus,
    SourceProduct,
    TaskStatus,
)
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
        top_n: int = 5,
        on_update=None,
    ):
        return [], []

    async def run_for_site(
        self,
        source_product: SourceProduct,
        comparison_site: str,
        top_n: int = 5,
        on_update=None,
    ):
        return [], {}


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


class ResumeOnlySourceAgent:
    def __init__(self) -> None:
        self.run_calls = 0
        self.resume_calls = 0

    async def run(self, source_url: str, on_update=None):
        self.run_calls += 1
        raise AssertionError("resume path should not start a new TinyFish run")

    async def resume(
        self,
        source_url: str,
        run_id: str,
        on_update=None,
        started_at=None,
        last_progress_at=None,
    ):
        self.resume_calls += 1
        if on_update is not None:
            await on_update(
                TinyFishRun(
                    run_id=run_id,
                    status="RUNNING",
                    elapsed_seconds=18.0,
                    last_heartbeat_at=datetime(2026, 3, 21, 10, 0, 9, tzinfo=timezone.utc),
                    last_progress_at=datetime(2026, 3, 21, 10, 0, 7, tzinfo=timezone.utc),
                )
            )
        return SourceProduct(source_url=source_url, brand="Brand"), {
            "tinyfish_run_id": run_id,
            "tinyfish_status": "COMPLETED",
        }


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


def test_investigation_store_lists_active_runs(tmp_path) -> None:
    async def run() -> None:
        store = InvestigationStore(tmp_path / "active.sqlite3")
        active = await store.create(
            InvestigationCreateRequest(
                source_urls=["https://brand.example/products/active-case"],
                comparison_sites=["https://shopee.sg/"],
            )
        )
        completed = await store.create(
            InvestigationCreateRequest(
                source_urls=["https://brand.example/products/completed-case"],
                comparison_sites=["https://shopee.sg/"],
            )
        )
        completed.status = InvestigationStatus.completed
        await store.save(completed)

        active_runs = await store.list_active()
        active_ids = {item.investigation_id for item in active_runs}

        assert active.investigation_id in active_ids
        assert completed.investigation_id not in active_ids

    asyncio.run(run())


def test_investigation_store_lists_recent_runs_newest_first(tmp_path) -> None:
    async def run() -> None:
        store = InvestigationStore(tmp_path / "recent.sqlite3")
        first = await store.create(
            InvestigationCreateRequest(
                source_urls=["https://brand.example/products/first-case"],
                comparison_sites=["https://shopee.sg/"],
            )
        )
        second = await store.create(
            InvestigationCreateRequest(
                source_urls=["https://brand.example/products/second-case"],
                comparison_sites=["https://shopee.sg/"],
            )
        )

        recent_runs = await store.list_recent(limit=10)

        assert [item.investigation_id for item in recent_runs[:2]] == [
            second.investigation_id,
            first.investigation_id,
        ]
        assert recent_runs[0].primary_source_url == "https://brand.example/products/second-case"

    asyncio.run(run())


def test_orchestrator_resumes_saved_source_run_after_restart(tmp_path) -> None:
    async def run() -> None:
        database_path = tmp_path / "resume.sqlite3"
        store = InvestigationStore(database_path)
        created = await store.create(
            InvestigationCreateRequest(
                source_urls=["https://brand.example/products/alpha-case"],
                comparison_sites=["https://shopee.sg/"],
            )
        )

        investigation = await store.get(created.investigation_id)
        assert investigation is not None
        investigation.status = InvestigationStatus.running
        investigation.reports = [
            InvestigationReport(
                source_url="https://brand.example/products/alpha-case",
                summary="Extracting official product details.",
                raw_agent_outputs=[
                    AgentTaskState(
                        agent_name="source_extraction",
                        status=TaskStatus.running,
                        input_payload={"source_url": "https://brand.example/products/alpha-case"},
                        output_payload={"runtime": {"tinyfish_run_id": "run-source-123"}},
                        provider_run_id="run-source-123",
                        provider_status="RUNNING",
                        started_at=datetime(2026, 3, 21, 10, 0, 0, tzinfo=timezone.utc),
                        last_heartbeat_at=datetime(2026, 3, 21, 10, 0, 5, tzinfo=timezone.utc),
                        last_progress_at=datetime(2026, 3, 21, 10, 0, 3, tzinfo=timezone.utc),
                    )
                ],
            )
        ]
        await store.save(investigation)

        source_agent = ResumeOnlySourceAgent()
        orchestrator = InvestigationOrchestrator(
            store=InvestigationStore(database_path),
            source_agent=source_agent,
            discovery_agent=EmptyDiscoveryAgent(),
            summary_agent=SummaryAgent(),
        )

        await orchestrator.run_investigation(created.investigation_id)

        reloaded = await store.get(created.investigation_id)
        assert reloaded is not None
        assert source_agent.resume_calls == 1
        assert source_agent.run_calls == 0
        assert reloaded.status == InvestigationStatus.completed
        assert reloaded.reports[0].extracted_source_product is not None
        source_task = reloaded.reports[0].raw_agent_outputs[0]
        assert source_task.status == TaskStatus.completed
        assert source_task.provider_run_id == "run-source-123"

    asyncio.run(run())
