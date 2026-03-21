"""Investigation orchestrator for the counterfeit research pipeline."""

from __future__ import annotations

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
    InvestigationStatus,
    SourceProduct,
    TaskStatus,
    utc_now,
)
from services.investigation_store import InvestigationStore
from services.settings import settings
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

    async def run_investigation(self, investigation_id: str) -> None:
        request = await self.store.get_request(investigation_id)
        investigation = await self.store.get(investigation_id)
        if investigation is None:
            return
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
            for source_url in request.source_urls:
                report = await self._run_for_source(
                    str(source_url),
                    comparison_sites,
                    request.max_candidates_per_site,
                )
                reports.append(report)
            investigation.reports = reports
            investigation.status = InvestigationStatus.completed
        except Exception as exc:  # pragma: no cover
            investigation.status = InvestigationStatus.failed
            investigation.error = str(exc)
        await self.store.save(investigation)

    async def _run_for_source(
        self,
        source_url: str,
        comparison_sites: list[str],
        max_candidates_per_site: int,
    ) -> InvestigationReport:
        task_log: list[AgentTaskState] = []
        source_product: SourceProduct | None = None
        try:
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

            comparisons: list[ComparisonResult] = []
            for candidate in candidates:
                comparison_task = AgentTaskState(
                    agent_name="product_comparison",
                    status=TaskStatus.running,
                    input_payload={"product_url": str(candidate.product_url)},
                    started_at=utc_now(),
                )
                task_log.append(comparison_task)
                comparison, comparison_raw_output = await self.runtime.run_agent(
                    lambda candidate=candidate: self.comparison_agent.run(source_product, candidate)
                )
                comparison_task.status = TaskStatus.completed
                comparison_task.output_payload = {
                    "comparison": comparison.model_dump(),
                    "runtime": comparison_raw_output,
                }
                comparison_task.completed_at = utc_now()

                evidence_task = AgentTaskState(
                    agent_name="evidence",
                    status=TaskStatus.running,
                    input_payload={"product_url": str(candidate.product_url)},
                    started_at=utc_now(),
                )
                task_log.append(evidence_task)
                evidence = await self.runtime.run_agent(
                    lambda comparison=comparison: self.evidence_agent.run(source_product, comparison)
                )
                comparison.evidence = evidence
                evidence_task.status = TaskStatus.completed
                evidence_task.output_payload = {"evidence": [item.model_dump() for item in evidence]}
                evidence_task.completed_at = utc_now()
                comparisons.append(comparison)

            ranking_task = AgentTaskState(
                agent_name="ranking",
                status=TaskStatus.running,
                input_payload={"comparison_count": len(comparisons)},
                started_at=utc_now(),
            )
            task_log.append(ranking_task)
            top_matches = await self.runtime.run_agent(lambda: self.ranking_agent.run(comparisons))
            ranking_task.status = TaskStatus.completed
            ranking_task.output_payload = {"ranked_product_urls": [str(item.product_url) for item in top_matches]}
            ranking_task.completed_at = utc_now()

            summary_task = AgentTaskState(
                agent_name="research_summary",
                status=TaskStatus.running,
                input_payload={"top_match_count": len(top_matches)},
                started_at=utc_now(),
            )
            task_log.append(summary_task)
            summary = await self.runtime.run_agent(lambda: self.summary_agent.run(source_product, top_matches))
            summary_task.status = TaskStatus.completed
            summary_task.output_payload = {"summary": summary}
            summary_task.completed_at = utc_now()

            return InvestigationReport(
                source_url=source_url,
                extracted_source_product=source_product,
                top_matches=top_matches,
                summary=summary,
                raw_agent_outputs=task_log,
            )
        except Exception as exc:
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
