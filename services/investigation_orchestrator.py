"""Investigation orchestrator for the counterfeit research pipeline."""

from __future__ import annotations

import inspect

from agents.candidate_discovery_agent import CandidateDiscoveryAgent
from agents.evidence_agent import EvidenceAgent
from agents.product_comparison_agent import ProductComparisonAgent
from agents.ranking_agent import RankingAgent
from agents.research_summary_agent import ResearchSummaryAgent
from agents.source_extraction_agent import SourceExtractionAgent
from models.schemas import (
    AgentTaskState,
    ComparisonResult,
    InvestigationReport,
    InvestigationResponse,
    InvestigationStatus,
    SourceProduct,
    TaskStatus,
    utc_now,
)
from services.investigation_store import InvestigationStore
from services.settings import settings
from services.tinyfish_client import TinyFishRun
from services.tinyfish_runtime import TinyFishRuntime


class InvestigationOrchestrator:
    """Coordinate the multi-agent counterfeit research workflow."""

    def __init__(
        self,
        store: InvestigationStore,
        runtime: TinyFishRuntime | None = None,
        source_agent: SourceExtractionAgent | None = None,
        discovery_agent: CandidateDiscoveryAgent | None = None,
        comparison_agent: ProductComparisonAgent | None = None,
        evidence_agent: EvidenceAgent | None = None,
        ranking_agent: RankingAgent | None = None,
        summary_agent: ResearchSummaryAgent | None = None,
    ) -> None:
        self.store = store
        self.runtime = runtime or TinyFishRuntime()
        self.source_agent = source_agent or SourceExtractionAgent()
        self.discovery_agent = discovery_agent or CandidateDiscoveryAgent()
        self.comparison_agent = comparison_agent or ProductComparisonAgent()
        self.evidence_agent = evidence_agent or EvidenceAgent()
        self.ranking_agent = ranking_agent or RankingAgent()
        self.summary_agent = summary_agent or ResearchSummaryAgent()

    @staticmethod
    def _pending_report(source_url: str) -> InvestigationReport:
        return InvestigationReport(
            source_url=source_url,
            summary="Queued for investigation.",
        )

    async def _save_report_progress(
        self,
        investigation: InvestigationResponse,
        report_index: int,
        report: InvestigationReport,
    ) -> None:
        investigation.reports[report_index] = report
        await self.store.save(investigation)

    @staticmethod
    async def _run_with_optional_update(
        fn: object,
        *args: object,
        on_update: object | None = None,
        **kwargs: object,
    ) -> object:
        if on_update is not None and "on_update" in inspect.signature(fn).parameters:
            return await fn(*args, on_update=on_update, **kwargs)
        return await fn(*args, **kwargs)

    @staticmethod
    def _runtime_payload(run: TinyFishRun) -> dict[str, object]:
        return {
            "tinyfish_run_id": run.run_id,
            "tinyfish_status": run.status,
            "tinyfish_result": run.result,
            "tinyfish_elapsed_seconds": run.elapsed_seconds,
            "tinyfish_delayed": run.delayed,
            "tinyfish_last_heartbeat_at": run.last_heartbeat_at.isoformat() if run.last_heartbeat_at else None,
            "tinyfish_last_progress_at": run.last_progress_at.isoformat() if run.last_progress_at else None,
        }

    async def _apply_task_update(
        self,
        investigation: InvestigationResponse,
        report_index: int,
        report: InvestigationReport,
        task_log: list[AgentTaskState],
        task: AgentTaskState,
        run: TinyFishRun,
        running_summary: str,
        delayed_summary: str | None = None,
    ) -> None:
        task.provider_run_id = run.run_id
        task.provider_status = run.status
        task.last_heartbeat_at = run.last_heartbeat_at
        task.last_progress_at = run.last_progress_at
        task.status = TaskStatus.delayed if run.delayed else TaskStatus.running
        task.output_payload = {"runtime": self._runtime_payload(run)}

        investigation.status = (
            InvestigationStatus.delayed if run.delayed else InvestigationStatus.running
        )
        report.summary = delayed_summary if run.delayed and delayed_summary else running_summary
        report.raw_agent_outputs = task_log
        report.error = None
        await self._save_report_progress(investigation, report_index, report)

    async def run_investigation(self, investigation_id: str) -> None:
        request = await self.store.get_request(investigation_id)
        investigation = await self.store.get(investigation_id)
        if investigation is None:
            return
        investigation.status = InvestigationStatus.running
        investigation.updated_at = utc_now()
        investigation.error = None
        investigation.reports = [
            self._pending_report(str(source_url))
            for source_url in request.source_urls
        ]
        await self.store.save(investigation)

        try:
            comparison_sites = [
                str(site) for site in request.comparison_sites
            ] or settings.ecommerce_store_urls
            if not comparison_sites:
                raise ValueError(
                    "No comparison sites were provided in the request or ECOMMERCE_STORE_URLS."
                )
            for report_index, source_url in enumerate(request.source_urls):
                report = await self._run_for_source(
                    investigation,
                    report_index,
                    str(source_url),
                    comparison_sites,
                    request.max_candidates_per_site,
                )
                investigation.reports[report_index] = report
            investigation.status = InvestigationStatus.completed
        except Exception as exc:  # pragma: no cover
            investigation.status = InvestigationStatus.failed
            investigation.error = str(exc)
        await self.store.save(investigation)

    async def _run_for_source(
        self,
        investigation: InvestigationResponse,
        report_index: int,
        source_url: str,
        comparison_sites: list[str],
        max_candidates_per_site: int,
    ) -> InvestigationReport:
        report = investigation.reports[report_index]
        task_log: list[AgentTaskState] = []
        source_product: SourceProduct | None = None
        try:
            search_summary = (
                f"Searching {len(comparison_sites)} marketplace target"
                f"{'' if len(comparison_sites) == 1 else 's'}."
            )
            source_task = AgentTaskState(
                agent_name="source_extraction",
                status=TaskStatus.running,
                input_payload={"source_url": source_url},
                started_at=utc_now(),
            )
            task_log.append(source_task)
            report.summary = "Extracting official product details."
            report.raw_agent_outputs = task_log
            report.error = None
            investigation.status = InvestigationStatus.running
            await self._save_report_progress(investigation, report_index, report)
            source_product, source_raw_output = await self.runtime.run_agent(
                lambda: self._run_with_optional_update(
                    self.source_agent.run,
                    source_url,
                    on_update=lambda run: self._apply_task_update(
                        investigation,
                        report_index,
                        report,
                        task_log,
                        source_task,
                        run,
                        "Extracting official product details.",
                        "Extracting official product details. TinyFish is still working on the source page.",
                    ),
                )
            )
            source_task.status = TaskStatus.completed
            source_task.provider_status = source_raw_output.get("tinyfish_status")
            source_task.provider_run_id = source_raw_output.get("tinyfish_run_id")
            source_task.output_payload = {
                "source_product": source_product.model_dump(),
                "runtime": source_raw_output,
            }
            source_task.completed_at = utc_now()
            report.extracted_source_product = source_product
            report.summary = search_summary
            investigation.status = InvestigationStatus.running
            await self._save_report_progress(investigation, report_index, report)

            discovery_task = AgentTaskState(
                agent_name="candidate_discovery",
                status=TaskStatus.running,
                input_payload={"comparison_sites": comparison_sites, "top_n": max_candidates_per_site},
                started_at=utc_now(),
            )
            task_log.append(discovery_task)
            report.raw_agent_outputs = task_log
            investigation.status = InvestigationStatus.running
            await self._save_report_progress(investigation, report_index, report)
            candidates, discovery_raw_outputs = await self.runtime.run_agent(
                lambda: self._run_with_optional_update(
                    self.discovery_agent.run,
                    source_product,
                    comparison_sites,
                    top_n=max_candidates_per_site,
                    on_update=lambda run: self._apply_task_update(
                        investigation,
                        report_index,
                        report,
                        task_log,
                        discovery_task,
                        run,
                        search_summary,
                        "Searching marketplace targets. TinyFish is still actively working through the search.",
                    ),
                )
            )
            discovery_task.status = TaskStatus.completed
            last_discovery_runtime = discovery_raw_outputs[-1] if discovery_raw_outputs else {}
            discovery_task.provider_status = last_discovery_runtime.get("tinyfish_status")
            discovery_task.provider_run_id = last_discovery_runtime.get("tinyfish_run_id")
            discovery_task.output_payload = {
                "candidate_count": len(candidates),
                "candidates": [candidate.model_dump() for candidate in candidates],
                "runtime": discovery_raw_outputs,
            }
            discovery_task.completed_at = utc_now()
            report.summary = (
                f"Comparing {len(candidates)} candidate listing"
                f"{'' if len(candidates) == 1 else 's'}."
                if candidates
                else "No candidate listings found. Moving to ranking and summary."
            )
            investigation.status = InvestigationStatus.running
            await self._save_report_progress(investigation, report_index, report)

            comparisons: list[ComparisonResult] = []
            for candidate_index, candidate in enumerate(candidates, start=1):
                comparison_summary = (
                    f"Comparing candidate {candidate_index} of {len(candidates)}."
                )
                comparison_task = AgentTaskState(
                    agent_name="product_comparison",
                    status=TaskStatus.running,
                    input_payload={"product_url": str(candidate.product_url)},
                    started_at=utc_now(),
                )
                task_log.append(comparison_task)
                report.raw_agent_outputs = task_log
                report.summary = comparison_summary
                investigation.status = InvestigationStatus.running
                await self._save_report_progress(investigation, report_index, report)
                comparison, comparison_raw_output = await self.runtime.run_agent(
                    lambda candidate=candidate: self._run_with_optional_update(
                        self.comparison_agent.run,
                        source_product,
                        candidate,
                        on_update=lambda run: self._apply_task_update(
                            investigation,
                            report_index,
                            report,
                            task_log,
                            comparison_task,
                            run,
                            comparison_summary,
                            f"Comparing candidate {candidate_index} of {len(candidates)}. TinyFish is still inspecting the listing.",
                        ),
                    )
                )
                comparison_task.status = TaskStatus.completed
                comparison_task.provider_status = comparison_raw_output.get("tinyfish_status")
                comparison_task.provider_run_id = comparison_raw_output.get("tinyfish_run_id")
                comparison_task.output_payload = {
                    "comparison": comparison.model_dump(),
                    "runtime": comparison_raw_output,
                }
                comparison_task.completed_at = utc_now()
                investigation.status = InvestigationStatus.running
                await self._save_report_progress(investigation, report_index, report)

                evidence_task = AgentTaskState(
                    agent_name="evidence",
                    status=TaskStatus.running,
                    input_payload={"product_url": str(candidate.product_url)},
                    started_at=utc_now(),
                )
                task_log.append(evidence_task)
                report.raw_agent_outputs = task_log
                report.summary = (
                    f"Collecting evidence for candidate {candidate_index} of {len(candidates)}."
                )
                await self._save_report_progress(investigation, report_index, report)
                evidence = await self.runtime.run_agent(
                    lambda comparison=comparison: self.evidence_agent.run(source_product, comparison)
                )
                comparison.evidence = evidence
                evidence_task.status = TaskStatus.completed
                evidence_task.output_payload = {"evidence": [item.model_dump() for item in evidence]}
                evidence_task.completed_at = utc_now()
                comparisons.append(comparison)
                await self._save_report_progress(investigation, report_index, report)

            ranking_task = AgentTaskState(
                agent_name="ranking",
                status=TaskStatus.running,
                input_payload={"comparison_count": len(comparisons)},
                started_at=utc_now(),
            )
            task_log.append(ranking_task)
            report.raw_agent_outputs = task_log
            report.summary = "Ranking suspicious listings."
            investigation.status = InvestigationStatus.running
            await self._save_report_progress(investigation, report_index, report)
            top_matches = await self.runtime.run_agent(lambda: self.ranking_agent.run(comparisons))
            ranking_task.status = TaskStatus.completed
            ranking_task.output_payload = {"ranked_product_urls": [str(item.product_url) for item in top_matches]}
            ranking_task.completed_at = utc_now()
            report.top_matches = top_matches
            report.summary = "Writing the final investigation summary."
            investigation.status = InvestigationStatus.running
            await self._save_report_progress(investigation, report_index, report)

            summary_task = AgentTaskState(
                agent_name="research_summary",
                status=TaskStatus.running,
                input_payload={"top_match_count": len(top_matches)},
                started_at=utc_now(),
            )
            task_log.append(summary_task)
            report.raw_agent_outputs = task_log
            await self._save_report_progress(investigation, report_index, report)
            summary = await self.runtime.run_agent(lambda: self.summary_agent.run(source_product, top_matches))
            summary_task.status = TaskStatus.completed
            summary_task.output_payload = {"summary": summary}
            summary_task.completed_at = utc_now()
            report.summary = summary
            report.raw_agent_outputs = task_log
            investigation.status = InvestigationStatus.running
            await self._save_report_progress(investigation, report_index, report)
            return report
        except Exception as exc:
            active_task = next(
                (
                    task
                    for task in reversed(task_log)
                    if task.status in {TaskStatus.running, TaskStatus.delayed}
                ),
                None,
            )
            if active_task is not None:
                active_task.status = TaskStatus.failed
                active_task.error = str(exc)
                active_task.completed_at = utc_now()
            else:
                task_log.append(
                    AgentTaskState(
                        agent_name="research_summary",
                        status=TaskStatus.failed,
                        input_payload={"source_url": source_url},
                        error=str(exc),
                        started_at=utc_now(),
                        completed_at=utc_now(),
                    )
                )
            summary = await self.summary_agent.run(source_product, [], error=str(exc))
            report.extracted_source_product = source_product
            report.top_matches = []
            report.summary = summary
            report.raw_agent_outputs = task_log
            report.error = str(exc)
            await self._save_report_progress(investigation, report_index, report)
            return report
