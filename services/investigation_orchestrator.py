"""Investigation orchestrator for the counterfeit research pipeline."""

from __future__ import annotations

import asyncio
import logging

from agents.candidate_discovery_agent import CandidateDiscoveryAgent
from agents.evidence_agent import EvidenceAgent
from agents.product_comparison_agent import ProductComparisonAgent
from agents.ranking_agent import RankingAgent
from agents.research_summary_agent import ResearchSummaryAgent
from agents.source_extraction_agent import SourceExtractionAgent
from models.schemas import ActivityLogEntry
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


logger = logging.getLogger("tinydetective")


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
        investigation_id: str,
        report_index: int,
        report: InvestigationReport,
        status: InvestigationStatus | None = None,
        error: str | None = None,
    ) -> None:
        investigation = await self.store.get(investigation_id)
        if investigation is None:
            return
        if report_index >= len(investigation.reports):
            missing = report_index + 1 - len(investigation.reports)
            investigation.reports.extend(
                [self._pending_report(report.source_url) for _ in range(missing)]
            )
        investigation.reports[report_index] = report
        if status is not None:
            investigation.status = status
        if error is not None:
            investigation.error = error
        await self.store.save(investigation)

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
        investigation_id: str,
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

        report.summary = delayed_summary if run.delayed and delayed_summary else running_summary
        report.raw_agent_outputs = task_log
        report.error = None
        await self._save_report_progress(
            investigation_id,
            report_index,
            report,
            status=InvestigationStatus.delayed if run.delayed else InvestigationStatus.running,
            error=None,
        )

    async def run_investigation(self, investigation_id: str) -> None:
        request = await self.store.get_request(investigation_id)
        investigation = await self.store.get(investigation_id)
        if investigation is None:
            return
        await self._log_activity(
            investigation_id,
            agent_name="orchestrator",
            message="Investigation started.",
            metadata={
                "source_url_count": len(request.source_urls),
                "comparison_site_count": len(request.comparison_sites or settings.ecommerce_store_urls),
            },
        )
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
            await self._log_activity(
                investigation_id,
                agent_name="orchestrator",
                message="Launching source investigations in parallel.",
                metadata={"comparison_sites": comparison_sites},
            )
            reports = await asyncio.gather(
                *[
                    self._run_for_source(
                        investigation_id,
                        report_index,
                        str(source_url),
                        comparison_sites,
                        request.max_candidates_per_site,
                    )
                    for report_index, source_url in enumerate(request.source_urls)
                ]
            )
            latest = await self.store.get(investigation_id)
            if latest is None:
                return
            latest.reports = reports
            latest.status = InvestigationStatus.completed
            await self._log_activity(
                investigation_id,
                agent_name="orchestrator",
                message="Investigation completed.",
                metadata={"report_count": len(reports)},
            )
            await self.store.save(latest)
        except Exception as exc:  # pragma: no cover
            latest = await self.store.get(investigation_id)
            if latest is not None:
                latest.status = InvestigationStatus.failed
                latest.error = str(exc)
                await self.store.save(latest)
            await self._log_activity(
                investigation_id,
                agent_name="orchestrator",
                message=f"Investigation failed: {exc}",
                level="error",
            )

    async def _run_for_source(
        self,
        investigation_id: str,
        report_index: int,
        source_url: str,
        comparison_sites: list[str],
        max_candidates_per_site: int,
    ) -> InvestigationReport:
        report = self._pending_report(source_url)
        task_log: list[AgentTaskState] = []
        source_product: SourceProduct | None = None
        try:
            await self._log_activity(
                investigation_id,
                agent_name="source_extraction",
                source_url=source_url,
                message="Starting source extraction.",
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
            await self._save_report_progress(
                investigation_id,
                report_index,
                report,
                status=InvestigationStatus.running,
                error=None,
            )
            source_product, source_raw_output = await self.runtime.run_agent(
                lambda: self.source_agent.run(
                    source_url,
                    on_update=lambda run: self._apply_task_update(
                        investigation_id,
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
            report.summary = (
                f"Searching {len(comparison_sites)} marketplace target"
                f"{'' if len(comparison_sites) == 1 else 's'}."
            )
            report.raw_agent_outputs = task_log
            await self._save_report_progress(
                investigation_id,
                report_index,
                report,
                status=InvestigationStatus.running,
                error=None,
            )
            await self._log_activity(
                investigation_id,
                agent_name="source_extraction",
                source_url=source_url,
                message="Completed source extraction.",
                metadata={
                    "brand": source_product.brand,
                    "product_name": source_product.product_name,
                },
            )

            await self._log_activity(
                investigation_id,
                agent_name="candidate_discovery",
                source_url=source_url,
                message="Starting candidate discovery across sites and queries.",
                metadata={
                    "comparison_sites": comparison_sites,
                    "top_n": max_candidates_per_site,
                },
            )
            discovery_task = AgentTaskState(
                agent_name="candidate_discovery",
                status=TaskStatus.running,
                input_payload={"comparison_sites": comparison_sites, "top_n": max_candidates_per_site},
                started_at=utc_now(),
            )
            task_log.append(discovery_task)
            report.raw_agent_outputs = task_log
            await self._save_report_progress(
                investigation_id,
                report_index,
                report,
                status=InvestigationStatus.running,
                error=None,
            )
            candidates, discovery_raw_outputs = await self.runtime.run_agent(
                lambda: self.discovery_agent.run(
                    source_product,
                    comparison_sites,
                    top_n=max_candidates_per_site,
                    on_update=lambda run: self._apply_task_update(
                        investigation_id,
                        report_index,
                        report,
                        task_log,
                        discovery_task,
                        run,
                        (
                            f"Searching {len(comparison_sites)} marketplace target"
                            f"{'' if len(comparison_sites) == 1 else 's'}."
                        ),
                        "Searching marketplace targets. TinyFish is still actively working through the queries.",
                    ),
                )
            )
            discovery_task.status = TaskStatus.completed
            if discovery_raw_outputs:
                discovery_task.provider_status = discovery_raw_outputs[-1].get("tinyfish_status")
                discovery_task.provider_run_id = discovery_raw_outputs[-1].get("tinyfish_run_id")
            discovery_task.output_payload = {
                "candidate_count": len(candidates),
                "candidates": [candidate.model_dump() for candidate in candidates],
                "runtime": discovery_raw_outputs,
            }
            discovery_task.completed_at = utc_now()
            for output in discovery_raw_outputs:
                await self._log_activity(
                    investigation_id,
                    agent_name="candidate_discovery",
                    source_url=source_url,
                    message="Completed discovery query.",
                    metadata={
                        "comparison_site": output.get("comparison_site"),
                        "search_query": output.get("search_query"),
                        "tinyfish_run_id": output.get("tinyfish_run_id"),
                        "tinyfish_status": output.get("tinyfish_status"),
                    },
                )
            await self._log_activity(
                investigation_id,
                agent_name="candidate_discovery",
                source_url=source_url,
                message="Candidate discovery completed.",
                metadata={"candidate_count": len(candidates)},
            )

            comparison_phase_task = AgentTaskState(
                agent_name="product_comparison",
                status=TaskStatus.running,
                input_payload={"candidate_count": len(candidates)},
                started_at=utc_now(),
            )
            task_log.append(comparison_phase_task)
            report.summary = (
                f"Comparing {len(candidates)} candidate listing"
                f"{'' if len(candidates) == 1 else 's'}."
                if candidates
                else "No candidate listings found. Moving to ranking and summary."
            )
            report.raw_agent_outputs = task_log
            await self._save_report_progress(
                investigation_id,
                report_index,
                report,
                status=InvestigationStatus.running,
                error=None,
            )

            comparison_results = await asyncio.gather(
                *[
                    self._compare_candidate(investigation_id, source_product, candidate)
                    for candidate in candidates
                ]
            )
            comparisons = [comparison for comparison, _ in comparison_results]
            for _, candidate_task_log in comparison_results:
                task_log.extend(candidate_task_log)
            comparison_phase_task.status = TaskStatus.completed
            comparison_phase_task.completed_at = utc_now()
            comparison_phase_task.output_payload = {
                "comparison_count": len(comparisons),
            }
            filtered_comparisons = [
                comparison for comparison in comparisons if not comparison.is_official_store
            ]
            excluded_official_store_count = len(comparisons) - len(filtered_comparisons)
            report.raw_agent_outputs = task_log
            await self._save_report_progress(
                investigation_id,
                report_index,
                report,
                status=InvestigationStatus.running,
                error=None,
            )

            ranking_task = AgentTaskState(
                agent_name="ranking",
                status=TaskStatus.running,
                input_payload={
                    "comparison_count": len(comparisons),
                    "excluded_official_store_count": excluded_official_store_count,
                },
                started_at=utc_now(),
            )
            task_log.append(ranking_task)
            report.summary = "Ranking candidate results."
            report.raw_agent_outputs = task_log
            await self._save_report_progress(
                investigation_id,
                report_index,
                report,
                status=InvestigationStatus.running,
                error=None,
            )
            await self._log_activity(
                investigation_id,
                agent_name="ranking",
                source_url=source_url,
                message="Ranking candidate results.",
                metadata={
                    "comparison_count": len(comparisons),
                    "excluded_official_store_count": excluded_official_store_count,
                },
            )
            top_matches = await self.runtime.run_agent(
                lambda: self.ranking_agent.run(filtered_comparisons)
            )
            ranking_task.status = TaskStatus.completed
            ranking_task.output_payload = {
                "ranked_product_urls": [str(item.product_url) for item in top_matches],
                "excluded_official_store_urls": [
                    str(item.product_url) for item in comparisons if item.is_official_store
                ],
            }
            ranking_task.completed_at = utc_now()
            await self._log_activity(
                investigation_id,
                agent_name="ranking",
                source_url=source_url,
                message="Ranking completed.",
                metadata={"top_match_count": len(top_matches)},
            )
            report.top_matches = top_matches
            report.excluded_official_store_count = excluded_official_store_count
            report.summary = "Generating investigation summary."
            report.raw_agent_outputs = task_log
            await self._save_report_progress(
                investigation_id,
                report_index,
                report,
                status=InvestigationStatus.running,
                error=None,
            )

            summary_task = AgentTaskState(
                agent_name="research_summary",
                status=TaskStatus.running,
                input_payload={"top_match_count": len(top_matches)},
                started_at=utc_now(),
            )
            task_log.append(summary_task)
            await self._log_activity(
                investigation_id,
                agent_name="research_summary",
                source_url=source_url,
                message="Generating investigation summary.",
            )
            summary = await self.runtime.run_agent(
                lambda: self.summary_agent.run(
                    source_product,
                    top_matches,
                    excluded_official_store_count=excluded_official_store_count,
                )
            )
            summary_task.status = TaskStatus.completed
            summary_task.output_payload = {"summary": summary}
            summary_task.completed_at = utc_now()
            await self._log_activity(
                investigation_id,
                agent_name="research_summary",
                source_url=source_url,
                message="Summary generated.",
                metadata={"summary": summary},
            )

            return InvestigationReport(
                source_url=source_url,
                extracted_source_product=source_product,
                top_matches=top_matches,
                excluded_official_store_count=excluded_official_store_count,
                summary=summary,
                raw_agent_outputs=task_log,
            )
        except Exception as exc:
            await self._log_activity(
                investigation_id,
                agent_name="research_summary",
                source_url=source_url,
                message=f"Source investigation failed: {exc}",
                level="error",
            )
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
            await self._save_report_progress(
                investigation_id,
                report_index,
                report,
                status=InvestigationStatus.failed,
                error=str(exc),
            )
            return report

    async def _compare_candidate(
        self,
        investigation_id: str,
        source_product: SourceProduct,
        candidate,
    ) -> tuple[ComparisonResult, list[AgentTaskState]]:
        candidate_task_log: list[AgentTaskState] = []
        await self._log_activity(
            investigation_id,
            agent_name="product_comparison",
            source_url=str(source_product.source_url),
            message="Starting candidate comparison.",
            metadata={
                "product_url": str(candidate.product_url),
                "discovery_queries": candidate.discovery_queries,
            },
        )
        comparison_task = AgentTaskState(
            agent_name="product_comparison",
            status=TaskStatus.running,
            input_payload={"product_url": str(candidate.product_url)},
            started_at=utc_now(),
        )
        candidate_task_log.append(comparison_task)
        comparison, comparison_raw_output = await self.runtime.run_agent(
            lambda: self.comparison_agent.run(source_product, candidate)
        )
        comparison_task.status = TaskStatus.completed
        comparison_task.output_payload = {
            "comparison": comparison.model_dump(),
            "runtime": comparison_raw_output,
        }
        comparison_task.completed_at = utc_now()
        await self._log_activity(
            investigation_id,
            agent_name="product_comparison",
            source_url=str(source_product.source_url),
            message="Candidate comparison completed.",
            metadata={
                "product_url": str(candidate.product_url),
                "match_score": comparison.match_score,
                "counterfeit_risk_score": comparison.counterfeit_risk_score,
                "is_official_store": comparison.is_official_store,
            },
        )

        evidence_task = AgentTaskState(
            agent_name="evidence",
            status=TaskStatus.running,
            input_payload={"product_url": str(candidate.product_url)},
            started_at=utc_now(),
        )
        candidate_task_log.append(evidence_task)
        evidence = await self.runtime.run_agent(
            lambda: self.evidence_agent.run(source_product, comparison)
        )
        comparison.evidence = evidence
        evidence_task.status = TaskStatus.completed
        evidence_task.output_payload = {"evidence": [item.model_dump() for item in evidence]}
        evidence_task.completed_at = utc_now()
        await self._log_activity(
            investigation_id,
            agent_name="evidence",
            source_url=str(source_product.source_url),
            message="Evidence generation completed.",
            metadata={
                "product_url": str(candidate.product_url),
                "evidence_count": len(evidence),
            },
        )
        return comparison, candidate_task_log

    async def _log_activity(
        self,
        investigation_id: str,
        agent_name: str,
        message: str,
        level: str = "info",
        source_url: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        entry = ActivityLogEntry(
            agent_name=agent_name,
            level=level,
            message=message,
            source_url=source_url,
            metadata=dict(metadata or {}),
        )
        await self.store.append_activity(investigation_id, entry)
        log_line = (
            f"investigation_id={investigation_id} "
            f"agent={agent_name} "
            f"source_url={source_url or '-'} "
            f"message={message} "
            f"metadata={entry.metadata}"
        )
        getattr(logger, level if hasattr(logger, level) else "info")(log_line)
