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
    InvestigationStatus,
    SourceProduct,
    TaskStatus,
    utc_now,
)
from services.investigation_store import InvestigationStore
from services.settings import settings
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
        await self.store.save(investigation)

        reports: list[InvestigationReport] = []
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
                        str(source_url),
                        comparison_sites,
                        request.max_candidates_per_site,
                    )
                    for source_url in request.source_urls
                ]
            )
            investigation.reports = reports
            investigation.status = InvestigationStatus.completed
            await self._log_activity(
                investigation_id,
                agent_name="orchestrator",
                message="Investigation completed.",
                metadata={"report_count": len(reports)},
            )
        except Exception as exc:  # pragma: no cover
            investigation.status = InvestigationStatus.failed
            investigation.error = str(exc)
            await self._log_activity(
                investigation_id,
                agent_name="orchestrator",
                message=f"Investigation failed: {exc}",
                level="error",
            )
        await self.store.save(investigation)

    async def _run_for_source(
        self,
        investigation_id: str,
        source_url: str,
        comparison_sites: list[str],
        max_candidates_per_site: int,
    ) -> InvestigationReport:
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
            source_product, source_raw_output = await self.runtime.run_agent(
                lambda: self.source_agent.run(source_url)
            )
            source_task.status = TaskStatus.completed
            source_task.output_payload = {
                "source_product": source_product.model_dump(),
                "runtime": source_raw_output,
            }
            source_task.completed_at = utc_now()
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
            candidates, discovery_raw_outputs = await self.runtime.run_agent(
                lambda: self.discovery_agent.run(source_product, comparison_sites, top_n=max_candidates_per_site)
            )
            discovery_task.status = TaskStatus.completed
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

            comparison_results = await asyncio.gather(
                *[
                    self._compare_candidate(investigation_id, source_product, candidate)
                    for candidate in candidates
                ]
            )
            comparisons = [comparison for comparison, _ in comparison_results]
            for _, candidate_task_log in comparison_results:
                task_log.extend(candidate_task_log)
            filtered_comparisons = [
                comparison for comparison in comparisons if not comparison.is_official_store
            ]
            excluded_official_store_count = len(comparisons) - len(filtered_comparisons)

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
            return InvestigationReport(
                source_url=source_url,
                extracted_source_product=source_product,
                top_matches=[],
                summary=summary,
                raw_agent_outputs=task_log,
                error=str(exc),
            )

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
