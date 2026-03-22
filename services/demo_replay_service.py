"""In-memory demo replay services for previously completed investigations and seller cases."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from typing import Any, Awaitable, Callable
from uuid import uuid4

from models.case_schemas import (
    OfficialProductMatch,
    SellerCaseCreateRequest,
    SellerCaseEvidenceItem,
    SellerCaseResponse,
    SellerCaseStatus,
    SellerListing,
    SellerListingTriageAssessment,
    SellerProfile,
    ActionRequestDraft,
)
from models.schemas import (
    ActivityLogEntry,
    AgentTaskState,
    CandidateProduct,
    CandidateTriageAssessment,
    ComparisonReasoningEnrichment,
    ComparisonResult,
    EvidenceItem,
    InvestigationCreateRequest,
    InvestigationReport,
    InvestigationResponse,
    InvestigationStatus,
    SourceProduct,
    TaskStatus,
    utc_now,
)
from services.investigation_store import InvestigationStore
from services.settings import settings


class DemoReplayService:
    """Replay completed investigations from SQLite without running the live pipeline."""

    def __init__(
        self,
        store: InvestigationStore,
        *,
        step_delay_seconds: float | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self.store = store
        configured_delay = (
            settings.demo_replay_step_delay_seconds
            if step_delay_seconds is None
            else step_delay_seconds
        )
        # Keep real demo runs visibly staged even if the configured delay is set too low.
        self.step_delay_seconds = max(configured_delay, 1.4) if step_delay_seconds is None else configured_delay
        self._sleep = sleep or asyncio.sleep
        self._lock = asyncio.Lock()
        self._replays: dict[str, InvestigationResponse] = {}
        self._jobs: dict[str, asyncio.Task[None]] = {}

    @staticmethod
    def _pending_report(source_url: str) -> InvestigationReport:
        return InvestigationReport(
            source_url=source_url,
            summary="Queued for demo replay.",
        )

    @staticmethod
    def _clone_source_product(report: InvestigationReport) -> SourceProduct | None:
        if report.extracted_source_product is not None:
            return report.extracted_source_product.model_copy(deep=True)
        for task in report.raw_agent_outputs:
            if task.agent_name != "source_extraction":
                continue
            payload = task.output_payload.get("source_product")
            if payload is not None:
                return SourceProduct.model_validate(payload)
        return None

    @staticmethod
    def _completed_tasks(report: InvestigationReport, agent_name: str) -> list[AgentTaskState]:
        return [
            task.model_copy(deep=True)
            for task in report.raw_agent_outputs
            if task.agent_name == agent_name and task.status == TaskStatus.completed
        ]

    @staticmethod
    def _load_comparison(task: AgentTaskState) -> ComparisonResult:
        return ComparisonResult.model_validate(task.output_payload["comparison"])

    @classmethod
    def _comparison_snapshots(cls, report: InvestigationReport) -> list[ComparisonResult]:
        comparison_tasks = cls._completed_tasks(report, "product_comparison")
        if comparison_tasks:
            return [cls._load_comparison(task) for task in comparison_tasks]
        return [comparison.model_copy(deep=True) for comparison in report.top_matches]

    @staticmethod
    def _mock_evidence(comparison: ComparisonResult) -> list[EvidenceItem]:
        if comparison.evidence:
            return [item.model_copy(deep=True) for item in comparison.evidence]

        candidate = comparison.candidate_product
        evidence: list[EvidenceItem] = []

        if comparison.counterfeit_risk_score >= 0.65:
            evidence.append(
                EvidenceItem(
                    type="suspicious_signal",
                    field="counterfeit_risk_score",
                    source_value=round(comparison.counterfeit_risk_score, 2),
                    candidate_value=round(comparison.counterfeit_risk_score, 2),
                    confidence=0.74,
                    note="Saved replay inferred elevated counterfeit risk from the final ranked result.",
                )
            )

        if candidate.title:
            evidence.append(
                EvidenceItem(
                    type="listing_attribute",
                    field="title",
                    source_value=None,
                    candidate_value=candidate.title,
                    confidence=0.69,
                    note="Saved replay preserved the candidate title as a comparison anchor.",
                )
            )

        if comparison.suspicious_signals:
            evidence.append(
                EvidenceItem(
                    type="suspicious_signal",
                    field="signals",
                    source_value=", ".join(comparison.suspicious_signals[:3]),
                    candidate_value=", ".join(comparison.suspicious_signals[:3]),
                    confidence=0.71,
                    note="Saved replay reconstructed suspicious indicators from the final comparison outcome.",
                )
            )

        return evidence

    @staticmethod
    def _mock_enrichment(comparison: ComparisonResult) -> ComparisonReasoningEnrichment:
        source_url = str(comparison.source_url)
        product_url = str(comparison.product_url)
        notes = list(comparison.reasoning_notes[:3])
        if not notes:
            notes = [
                "Demo replay synthesized reasoning from the saved ranked comparison.",
                "Structured listing attributes were unavailable in the original intermediate trace.",
            ]

        return ComparisonReasoningEnrichment(
            source_url=source_url,
            product_url=product_url,
            enriched_reason=comparison.reason or "Demo replay reconstructed the reasoning narrative from saved output.",
            reasoning_notes=notes,
            additional_suspicious_signals=list(comparison.suspicious_signals[:2]),
            risk_adjustment=0.0,
            match_adjustment=0.0,
        )

    @staticmethod
    def _load_evidence(task: AgentTaskState) -> list[EvidenceItem]:
        return [
            EvidenceItem.model_validate(item)
            for item in task.output_payload.get("evidence", [])
        ]

    @staticmethod
    def _load_enrichment(task: AgentTaskState) -> ComparisonReasoningEnrichment:
        return ComparisonReasoningEnrichment.model_validate(task.output_payload["enrichment"])

    @staticmethod
    def _task_query(task: AgentTaskState, candidate: CandidateProduct | None = None) -> str:
        if candidate is not None and candidate.discovery_queries:
            return candidate.discovery_queries[0]
        if candidate is not None and candidate.title:
            return candidate.title
        if candidate is not None and candidate.model:
            return candidate.model
        query = task.output_payload.get("search_query") or task.input_payload.get("search_query")
        if query:
            return str(query)
        return "saved marketplace query"

    @staticmethod
    def _task_marketplace(task: AgentTaskState, candidate: CandidateProduct | None = None) -> str:
        marketplace = (
            task.output_payload.get("comparison_site")
            or task.input_payload.get("comparison_site")
        )
        if marketplace:
            return str(marketplace)
        if candidate is not None:
            return candidate.marketplace
        return ""

    @classmethod
    def _derive_discovery_tasks(cls, report: InvestigationReport) -> list[AgentTaskState]:
        discovery_tasks = cls._completed_tasks(report, "candidate_discovery")
        comparisons = cls._comparison_snapshots(report)
        should_synthesize = len(comparisons) > 1 and len(discovery_tasks) <= 1
        if discovery_tasks and not should_synthesize:
            return discovery_tasks

        if not comparisons:
            return discovery_tasks

        base_task = discovery_tasks[0] if discovery_tasks else None
        synthetic_tasks: list[AgentTaskState] = []
        for index, comparison in enumerate(comparisons, start=1):
            candidate = comparison.candidate_product.model_copy(deep=True)
            template_task = AgentTaskState(
                agent_name="candidate_discovery",
                input_payload={"comparison_site": candidate.marketplace},
                output_payload={},
            )
            search_query = cls._task_query(base_task or template_task, candidate)
            comparison_site = cls._task_marketplace(base_task or template_task, candidate)
            synthetic_tasks.append(
                AgentTaskState(
                    agent_name="candidate_discovery",
                    status=TaskStatus.completed,
                    input_payload={
                        "comparison_site": comparison_site,
                        "search_query": search_query,
                    },
                    output_payload={
                        "comparison_site": comparison_site,
                        "search_query": search_query,
                        "candidate_count": 1,
                        "candidates": [candidate.model_dump()],
                        "runtime": {
                            "demo_replay": True,
                            "tinyfish_elapsed_seconds": round(1.4 + (index * 0.6), 1),
                        },
                    },
                )
            )
        return synthetic_tasks

    @classmethod
    def _derive_triage_tasks(
        cls,
        report: InvestigationReport,
        discovery_tasks: list[AgentTaskState],
    ) -> list[AgentTaskState]:
        triage_tasks = cls._completed_tasks(report, "candidate_triage")
        if triage_tasks:
            return triage_tasks

        comparisons = {
            str(comparison.product_url): comparison
            for comparison in cls._comparison_snapshots(report)
        }
        candidates: dict[str, CandidateProduct] = {}
        for task in discovery_tasks:
            for candidate_payload in task.output_payload.get("candidates", []):
                candidate = CandidateProduct.model_validate(candidate_payload)
                candidates[str(candidate.product_url)] = candidate

        derived_tasks: list[AgentTaskState] = []
        for product_url, candidate in candidates.items():
            comparison = comparisons.get(product_url)
            shortlisted = comparison is not None
            derived_tasks.append(
                AgentTaskState(
                    agent_name="candidate_triage",
                    status=TaskStatus.completed,
                    input_payload={"product_url": product_url},
                    output_payload={
                        "triage": CandidateTriageAssessment(
                            source_url=str(report.source_url),
                            product_url=product_url,
                            investigation_priority_score=(
                                comparison.triage_priority_score
                                if comparison is not None
                                else 0.15
                            ),
                            suspicion_score=(
                                comparison.triage_suspicion_score
                                if comparison is not None
                                else 0.05
                            ),
                            should_shortlist=shortlisted,
                            rationale=(
                                comparison.reason
                                if comparison is not None
                                else "Saved replay kept this listing out of the deep-comparison shortlist."
                            ),
                            suspicious_signals=(
                                list(comparison.suspicious_signals[:4])
                                if comparison is not None
                                else []
                            ),
                        ).model_dump()
                    },
                )
            )
        return derived_tasks

    @classmethod
    def _derive_comparison_tasks(cls, report: InvestigationReport) -> list[AgentTaskState]:
        comparison_tasks = cls._completed_tasks(report, "product_comparison")
        if comparison_tasks:
            return comparison_tasks

        derived_tasks: list[AgentTaskState] = []
        for comparison in report.top_matches:
            derived_tasks.append(
                AgentTaskState(
                    agent_name="product_comparison",
                    status=TaskStatus.completed,
                    input_payload={"product_url": str(comparison.product_url)},
                    output_payload={"comparison": comparison.model_dump()},
                )
            )
        return derived_tasks

    @classmethod
    def _derive_evidence_tasks(
        cls,
        report: InvestigationReport,
        comparison_tasks: list[AgentTaskState],
    ) -> list[AgentTaskState]:
        evidence_tasks = cls._completed_tasks(report, "evidence")
        if evidence_tasks:
            return evidence_tasks

        derived_tasks: list[AgentTaskState] = []
        for comparison_task in comparison_tasks:
            comparison = cls._load_comparison(comparison_task)
            evidence_items = cls._mock_evidence(comparison)
            if not evidence_items:
                continue
            derived_tasks.append(
                AgentTaskState(
                    agent_name="evidence",
                    status=TaskStatus.completed,
                    input_payload={"product_url": str(comparison.product_url)},
                    output_payload={
                        "evidence": [item.model_dump() for item in evidence_items]
                    },
                )
            )
        return derived_tasks

    @classmethod
    def _derive_enrichment_tasks(cls, report: InvestigationReport) -> list[AgentTaskState]:
        enrichment_tasks = cls._completed_tasks(report, "reasoning_enrichment")
        if enrichment_tasks:
            return enrichment_tasks

        derived_tasks: list[AgentTaskState] = []
        for comparison in cls._comparison_snapshots(report):
            derived_tasks.append(
                AgentTaskState(
                    agent_name="reasoning_enrichment",
                    status=TaskStatus.completed,
                    input_payload={"product_url": str(comparison.product_url)},
                    output_payload={
                        "enrichment": cls._mock_enrichment(comparison).model_dump()
                    },
                )
            )
        return derived_tasks

    @classmethod
    def _derive_ranking_task(cls, report: InvestigationReport) -> AgentTaskState:
        ranking_tasks = cls._completed_tasks(report, "ranking")
        if ranking_tasks:
            return ranking_tasks[-1]
        return AgentTaskState(
            agent_name="ranking",
            status=TaskStatus.completed,
            input_payload={
                "comparison_count": len(report.top_matches),
                "excluded_official_store_count": report.excluded_official_store_count,
            },
            output_payload={
                "ranked_product_urls": [str(item.product_url) for item in report.top_matches],
            },
        )

    @classmethod
    def _derive_summary_task(cls, report: InvestigationReport) -> AgentTaskState:
        summary_tasks = cls._completed_tasks(report, "research_summary")
        if summary_tasks:
            return summary_tasks[-1]
        return AgentTaskState(
            agent_name="research_summary",
            status=TaskStatus.completed,
            input_payload={"top_match_count": len(report.top_matches)},
            output_payload={"summary": report.summary},
        )

    @staticmethod
    def _search_summary(discovery_task_count: int) -> str:
        return (
            f"Replaying {discovery_task_count} saved marketplace quer"
            f"{'y' if discovery_task_count == 1 else 'ies'}."
        )

    @staticmethod
    def _triage_summary(candidate_count: int) -> str:
        return (
            f"Triaging {candidate_count} discovered candidate listing"
            f"{'' if candidate_count == 1 else 's'} with OpenAI."
        )

    @staticmethod
    def _comparison_summary(total_candidates: int) -> str:
        return (
            f"Running saved candidate analysis across {total_candidates} shortlisted candidate"
            f"{'' if total_candidates == 1 else 's'}."
        )

    @staticmethod
    def _evidence_summary(total_candidates: int) -> str:
        return (
            f"Collecting evidence across {total_candidates} shortlisted candidate"
            f"{'' if total_candidates == 1 else 's'}."
        )

    @staticmethod
    def _reasoning_summary(total_candidates: int) -> str:
        return (
            f"Refining reasoning across {total_candidates} shortlisted candidate"
            f"{'' if total_candidates == 1 else 's'} with OpenAI."
        )

    @staticmethod
    def _new_running_task(template: AgentTaskState) -> AgentTaskState:
        return AgentTaskState(
            agent_name=template.agent_name,
            status=TaskStatus.running,
            input_payload=deepcopy(template.input_payload),
            started_at=utc_now(),
        )

    @staticmethod
    def _complete_task(task: AgentTaskState, template: AgentTaskState) -> None:
        task.status = TaskStatus.completed
        task.error = None
        task.output_payload = deepcopy(template.output_payload)
        task.provider_run_id = None
        task.provider_status = None
        task.last_heartbeat_at = None
        task.last_progress_at = None
        task.completed_at = utc_now()

    async def _pause(self, multiplier: float = 1.0) -> None:
        delay = max(self.step_delay_seconds * multiplier, 0.0)
        if delay > 0:
            await self._sleep(delay)

    async def _pause_stage(self) -> None:
        await self._pause(1.0)

    async def _pause_work_unit(self, multiplier: float = 0.35) -> None:
        await self._pause(multiplier)

    async def _mutate(
        self,
        investigation_id: str,
        mutation: Callable[[InvestigationResponse], None],
    ) -> None:
        async with self._lock:
            replay = self._replays.get(investigation_id)
            if replay is None:
                return
            mutation(replay)
            replay.updated_at = utc_now()

    async def _append_activity(
        self,
        investigation_id: str,
        *,
        agent_name: str,
        message: str,
        source_url: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        def mutation(replay: InvestigationResponse) -> None:
            replay.activity_log.append(
                ActivityLogEntry(
                    agent_name=agent_name,
                    message=message,
                    source_url=source_url,
                    metadata=dict(metadata or {}),
                )
            )

        await self._mutate(investigation_id, mutation)

    async def create_replay(self, payload: InvestigationCreateRequest) -> InvestigationResponse:
        source_urls = [str(source_url) for source_url in payload.source_urls]
        template = await self.store.find_latest_completed_by_source_urls(source_urls)
        if template is None:
            raise LookupError(
                "Demo mode could not find a completed saved investigation matching the submitted source URL set."
            )

        replay = InvestigationResponse(
            investigation_id=str(uuid4()),
            status=InvestigationStatus.queued,
            reports=[self._pending_report(source_url) for source_url in source_urls],
        )

        async with self._lock:
            self._replays[replay.investigation_id] = replay
            self._jobs[replay.investigation_id] = asyncio.create_task(
                self._run_replay(replay.investigation_id, template)
            )

        return replay.model_copy(deep=True)

    async def get(self, investigation_id: str) -> InvestigationResponse | None:
        async with self._lock:
            replay = self._replays.get(investigation_id)
            return replay.model_copy(deep=True) if replay is not None else None

    async def wait(self, investigation_id: str) -> None:
        async with self._lock:
            job = self._jobs.get(investigation_id)
        if job is not None:
            await job

    async def _run_replay(
        self,
        investigation_id: str,
        template: InvestigationResponse,
    ) -> None:
        try:
            await self._pause_work_unit(0.2)
            await self._mutate(
                investigation_id,
                lambda replay: setattr(replay, "status", InvestigationStatus.running),
            )
            for report_index, template_report in enumerate(template.reports):
                await self._replay_report(investigation_id, report_index, template_report)

            def complete(replay: InvestigationResponse) -> None:
                replay.status = InvestigationStatus.completed
                replay.error = None

            await self._mutate(investigation_id, complete)
            await self._append_activity(
                investigation_id,
                agent_name="demo_replay",
                message="Completed saved investigation replay.",
            )
        except Exception as exc:  # pragma: no cover
            def fail(replay: InvestigationResponse) -> None:
                replay.status = InvestigationStatus.failed
                replay.error = str(exc)

            await self._mutate(investigation_id, fail)
        finally:
            async with self._lock:
                self._jobs.pop(investigation_id, None)

    async def _replay_report(
        self,
        investigation_id: str,
        report_index: int,
        template_report: InvestigationReport,
    ) -> None:
        report = self._pending_report(str(template_report.source_url))
        source_product = self._clone_source_product(template_report)
        discovery_templates = self._derive_discovery_tasks(template_report)
        triage_templates = self._derive_triage_tasks(template_report, discovery_templates)
        comparison_templates = self._derive_comparison_tasks(template_report)
        evidence_templates = self._derive_evidence_tasks(template_report, comparison_templates)
        enrichment_templates = self._derive_enrichment_tasks(template_report)
        ranking_template = self._derive_ranking_task(template_report)
        summary_template = self._derive_summary_task(template_report)

        await self._append_activity(
            investigation_id,
            agent_name="source_extraction",
            message="Replaying source extraction for the saved official product page.",
            source_url=str(template_report.source_url),
        )

        source_template = AgentTaskState(
            agent_name="source_extraction",
            status=TaskStatus.completed,
            input_payload={"source_url": str(template_report.source_url)},
            output_payload={
                "source_product": source_product.model_dump() if source_product is not None else None,
                "runtime": {"demo_replay": True},
            },
        )
        source_task = self._new_running_task(source_template)
        report.summary = "Extracting official product details."
        report.raw_agent_outputs.append(source_task)

        await self._mutate(
            investigation_id,
            lambda replay: (
                setattr(replay, "status", InvestigationStatus.running),
                replay.reports.__setitem__(report_index, report),
            ),
        )
        await self._pause_work_unit(0.45)

        self._complete_task(source_task, source_template)
        report.extracted_source_product = source_product
        report.summary = self._search_summary(max(len(discovery_templates), 1))
        await self._mutate(
            investigation_id,
            lambda replay: replay.reports.__setitem__(report_index, report),
        )

        if template_report.error and source_product is None:
            report.error = template_report.error
            report.summary = template_report.summary
            await self._mutate(
                investigation_id,
                lambda replay: replay.reports.__setitem__(report_index, report),
            )
            return

        if discovery_templates:
            await self._append_activity(
                investigation_id,
                agent_name="candidate_discovery",
                message=f"Replaying {len(discovery_templates)} saved marketplace discovery step(s).",
                source_url=str(template_report.source_url),
            )
            for template_task in discovery_templates:
                discovery_task = self._new_running_task(template_task)
                report.raw_agent_outputs.append(discovery_task)
                report.summary = self._search_summary(len(discovery_templates))
                await self._mutate(
                    investigation_id,
                    lambda replay: replay.reports.__setitem__(report_index, report),
                )
                await self._pause_work_unit(0.55)
                self._complete_task(discovery_task, template_task)
                await self._mutate(
                    investigation_id,
                    lambda replay: replay.reports.__setitem__(report_index, report),
                )
            await self._pause_stage()

        if triage_templates:
            await self._append_activity(
                investigation_id,
                agent_name="candidate_triage",
                message="Replaying candidate intake and shortlist triage.",
                source_url=str(template_report.source_url),
            )
            for index, template_task in enumerate(triage_templates, start=1):
                triage_task = self._new_running_task(template_task)
                report.raw_agent_outputs.append(triage_task)
                report.summary = (
                    f"Candidate intake {index} of {len(triage_templates)}. Reviewing discovered listings."
                )
                await self._mutate(
                    investigation_id,
                    lambda replay: replay.reports.__setitem__(report_index, report),
                )
                await self._pause_work_unit(0.65)
                self._complete_task(triage_task, template_task)
                await self._mutate(
                    investigation_id,
                    lambda replay: replay.reports.__setitem__(report_index, report),
                )
            await self._pause_stage()

        if comparison_templates:
            await self._append_activity(
                investigation_id,
                agent_name="product_comparison",
                message=f"Replaying saved candidate analysis across {len(comparison_templates)} listing(s).",
                source_url=str(template_report.source_url),
            )
            report.summary = self._comparison_summary(len(comparison_templates))
            for template_task in comparison_templates:
                comparison_task = self._new_running_task(template_task)
                report.raw_agent_outputs.append(comparison_task)
                await self._mutate(
                    investigation_id,
                    lambda replay: replay.reports.__setitem__(report_index, report),
                )
                await self._pause_work_unit(0.7)
                self._complete_task(comparison_task, template_task)
                await self._mutate(
                    investigation_id,
                    lambda replay: replay.reports.__setitem__(report_index, report),
                )
            await self._pause_stage()

        if evidence_templates:
            report.summary = self._evidence_summary(len(evidence_templates))
            running_tasks = [self._new_running_task(template) for template in evidence_templates]
            report.raw_agent_outputs.extend(running_tasks)
            await self._mutate(
                investigation_id,
                lambda replay: replay.reports.__setitem__(report_index, report),
            )
            await self._pause_work_unit(0.25)
            for task, template_task in zip(running_tasks, evidence_templates):
                self._complete_task(task, template_task)
            await self._mutate(
                investigation_id,
                lambda replay: replay.reports.__setitem__(report_index, report),
            )
            await self._pause_stage()

        if enrichment_templates:
            report.summary = self._reasoning_summary(len(enrichment_templates))
            running_tasks = [self._new_running_task(template) for template in enrichment_templates]
            report.raw_agent_outputs.extend(running_tasks)
            await self._mutate(
                investigation_id,
                lambda replay: replay.reports.__setitem__(report_index, report),
            )
            await self._pause_work_unit(0.25)
            for task, template_task in zip(running_tasks, enrichment_templates):
                self._complete_task(task, template_task)
            await self._mutate(
                investigation_id,
                lambda replay: replay.reports.__setitem__(report_index, report),
            )
            await self._pause_stage()

        ranking_task = self._new_running_task(ranking_template)
        report.raw_agent_outputs.append(ranking_task)
        report.summary = "Ranking suspicious listings."
        await self._mutate(
            investigation_id,
            lambda replay: replay.reports.__setitem__(report_index, report),
        )
        await self._pause_work_unit(0.25)
        self._complete_task(ranking_task, ranking_template)
        report.top_matches = [item.model_copy(deep=True) for item in template_report.top_matches]
        report.excluded_official_store_count = template_report.excluded_official_store_count
        report.summary = "Writing the final investigation summary."
        await self._mutate(
            investigation_id,
            lambda replay: replay.reports.__setitem__(report_index, report),
        )
        await self._pause_stage()

        summary_task = self._new_running_task(summary_template)
        report.raw_agent_outputs.append(summary_task)
        await self._mutate(
            investigation_id,
            lambda replay: replay.reports.__setitem__(report_index, report),
        )
        await self._pause_work_unit(0.25)
        self._complete_task(summary_task, summary_template)
        report.summary = template_report.summary
        report.error = template_report.error
        report.extracted_source_product = (
            template_report.extracted_source_product.model_copy(deep=True)
            if template_report.extracted_source_product is not None
            else source_product
        )
        report.top_matches = [item.model_copy(deep=True) for item in template_report.top_matches]
        report.excluded_official_store_count = template_report.excluded_official_store_count
        await self._mutate(
            investigation_id,
            lambda replay: replay.reports.__setitem__(report_index, report),
        )
        await self._pause_stage()


class DemoSellerCaseReplayService:
    """Replay completed seller cases from SQLite without running the live pipeline."""

    def __init__(
        self,
        store: InvestigationStore,
        *,
        step_delay_seconds: float | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self.store = store
        configured_delay = (
            settings.demo_replay_step_delay_seconds
            if step_delay_seconds is None
            else step_delay_seconds
        )
        self.step_delay_seconds = max(configured_delay, 1.4) if step_delay_seconds is None else configured_delay
        self._sleep = sleep or asyncio.sleep
        self._lock = asyncio.Lock()
        self._cases: dict[str, SellerCaseResponse] = {}
        self._jobs: dict[str, asyncio.Task[None]] = {}

    @staticmethod
    def _clone_task_templates(
        seller_case: SellerCaseResponse,
        agent_name: str,
    ) -> list[AgentTaskState]:
        return [
            task.model_copy(deep=True)
            for task in seller_case.raw_agent_outputs
            if task.agent_name == agent_name and task.status == TaskStatus.completed
        ]

    @staticmethod
    def _derive_profile_tasks(template: SellerCaseResponse) -> list[AgentTaskState]:
        tasks = DemoSellerCaseReplayService._clone_task_templates(template, "seller_profile")
        if tasks:
            return tasks
        if template.seller_profile is None:
            return []
        return [
            AgentTaskState(
                agent_name="seller_profile",
                status=TaskStatus.completed,
                input_payload={
                    "entry_url": (template.seller_profile.entry_urls or [template.seller_store_url or template.product_url])[0]
                },
                output_payload={"seller_profile": template.seller_profile.model_dump()},
            )
        ]

    @staticmethod
    def _derive_discovery_tasks(template: SellerCaseResponse) -> list[AgentTaskState]:
        tasks = DemoSellerCaseReplayService._clone_task_templates(template, "seller_listing_discovery")
        if tasks:
            return tasks
        if not template.discovered_listings:
            return []
        return [
            AgentTaskState(
                agent_name="seller_listing_discovery",
                status=TaskStatus.completed,
                input_payload={
                    "shard_url": (
                        template.seller_profile.storefront_shard_urls[0]
                        if template.seller_profile and template.seller_profile.storefront_shard_urls
                        else template.seller_store_url or template.product_url
                    )
                },
                output_payload={
                    "discovered_listings": [
                        listing.model_dump() for listing in template.discovered_listings
                    ]
                },
            )
        ]

    @staticmethod
    def _derive_triage_tasks(template: SellerCaseResponse) -> list[AgentTaskState]:
        tasks = DemoSellerCaseReplayService._clone_task_templates(template, "seller_listing_triage")
        if tasks:
            return tasks
        return [
            AgentTaskState(
                agent_name="seller_listing_triage",
                status=TaskStatus.completed,
                input_payload={"product_url": str(item.product_url)},
                output_payload={"triage": item.model_dump()},
            )
            for item in template.triage_assessments
        ]

    @staticmethod
    def _derive_official_match_tasks(template: SellerCaseResponse) -> list[AgentTaskState]:
        tasks = DemoSellerCaseReplayService._clone_task_templates(template, "official_product_match")
        if tasks:
            return tasks
        return [
            AgentTaskState(
                agent_name="official_product_match",
                status=TaskStatus.completed,
                input_payload={"product_url": str(item.product_url)},
                output_payload={"official_match": item.model_dump()},
            )
            for item in template.official_product_matches
        ]

    @staticmethod
    def _derive_analysis_tasks(template: SellerCaseResponse) -> list[AgentTaskState]:
        tasks = DemoSellerCaseReplayService._clone_task_templates(template, "seller_listing_analysis")
        if tasks:
            return tasks
        return [
            AgentTaskState(
                agent_name="seller_listing_analysis",
                status=TaskStatus.completed,
                input_payload={"product_url": str(item.product_url)},
                output_payload={"comparison": item.model_dump()},
            )
            for item in template.suspect_listings
        ]

    @staticmethod
    def _derive_evidence_task(template: SellerCaseResponse) -> AgentTaskState | None:
        tasks = DemoSellerCaseReplayService._clone_task_templates(template, "seller_case_evidence")
        if tasks:
            return tasks[-1]
        if not template.evidence:
            return None
        return AgentTaskState(
            agent_name="seller_case_evidence",
            status=TaskStatus.completed,
            input_payload={"suspect_listing_count": len(template.suspect_listings)},
            output_payload={"evidence": [item.model_dump() for item in template.evidence]},
        )

    @staticmethod
    def _derive_draft_task(template: SellerCaseResponse) -> AgentTaskState | None:
        tasks = DemoSellerCaseReplayService._clone_task_templates(template, "case_draft")
        if tasks:
            return tasks[-1]
        if template.action_request_draft is None:
            return None
        return AgentTaskState(
            agent_name="case_draft",
            status=TaskStatus.completed,
            input_payload={"evidence_count": len(template.evidence)},
            output_payload={"draft": template.action_request_draft.model_dump()},
        )

    async def _pause(self, multiplier: float = 1.0) -> None:
        delay = max(self.step_delay_seconds * multiplier, 0.0)
        if delay > 0:
            await self._sleep(delay)

    async def _pause_stage(self) -> None:
        await self._pause(1.0)

    async def _pause_work_unit(self, multiplier: float = 0.35) -> None:
        await self._pause(multiplier)

    async def _mutate(
        self,
        case_id: str,
        mutation: Callable[[SellerCaseResponse], None],
    ) -> None:
        async with self._lock:
            replay = self._cases.get(case_id)
            if replay is None:
                return
            mutation(replay)
            replay.updated_at = utc_now()

    async def _append_activity(
        self,
        case_id: str,
        *,
        agent_name: str,
        message: str,
        source_url: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> None:
        def mutation(replay: SellerCaseResponse) -> None:
            replay.activity_log.append(
                ActivityLogEntry(
                    agent_name=agent_name,
                    message=message,
                    source_url=source_url,
                    metadata=dict(metadata or {}),
                )
            )

        await self._mutate(case_id, mutation)

    async def create_replay(self, payload: SellerCaseCreateRequest) -> SellerCaseResponse:
        template = await self.store.find_latest_completed_case_by_source_and_product_url(
            str(payload.source_url),
            str(payload.product_url),
        )
        if template is None:
            raise LookupError(
                "Demo mode could not find a completed saved seller case matching the submitted source and listing URLs."
            )

        replay = SellerCaseResponse(
            case_id=str(uuid4()),
            investigation_id=payload.investigation_id,
            source_url=str(payload.source_url),
            product_url=str(payload.product_url),
            marketplace=template.marketplace,
            seller_name=template.seller_name,
            seller_store_url=template.seller_store_url,
            status=SellerCaseStatus.queued,
            summary="Queued for seller case demo replay.",
            source_product=template.source_product.model_copy(deep=True) if template.source_product else None,
            selected_listing=template.selected_listing.model_copy(deep=True) if template.selected_listing else None,
        )

        async with self._lock:
            self._cases[replay.case_id] = replay
            self._jobs[replay.case_id] = asyncio.create_task(
                self._run_replay(replay.case_id, template)
            )

        return replay.model_copy(deep=True)

    async def get(self, case_id: str) -> SellerCaseResponse | None:
        async with self._lock:
            replay = self._cases.get(case_id)
            return replay.model_copy(deep=True) if replay is not None else None

    async def wait(self, case_id: str) -> None:
        async with self._lock:
            job = self._jobs.get(case_id)
        if job is not None:
            await job

    async def _run_replay(self, case_id: str, template: SellerCaseResponse) -> None:
        try:
            await self._pause_work_unit(0.2)
            await self._mutate(
                case_id,
                lambda replay: setattr(replay, "status", SellerCaseStatus.running),
            )
            await self._replay_case(case_id, template)

            def complete(replay: SellerCaseResponse) -> None:
                replay.status = SellerCaseStatus.completed
                replay.error = None

            await self._mutate(case_id, complete)
            await self._append_activity(
                case_id,
                agent_name="seller_case_demo",
                message="Completed saved seller case replay.",
                source_url=str(template.source_url),
            )
        except Exception as exc:  # pragma: no cover
            def fail(replay: SellerCaseResponse) -> None:
                replay.status = SellerCaseStatus.failed
                replay.error = str(exc)
                replay.summary = f"Seller case demo replay failed: {exc}"

            await self._mutate(case_id, fail)
        finally:
            async with self._lock:
                self._jobs.pop(case_id, None)

    async def _replay_case(self, case_id: str, template: SellerCaseResponse) -> None:
        profile_tasks = self._derive_profile_tasks(template)
        discovery_tasks = self._derive_discovery_tasks(template)
        triage_tasks = self._derive_triage_tasks(template)
        official_match_tasks = self._derive_official_match_tasks(template)
        analysis_tasks = self._derive_analysis_tasks(template)
        evidence_task = self._derive_evidence_task(template)
        draft_task = self._derive_draft_task(template)

        if profile_tasks:
            await self._append_activity(
                case_id,
                agent_name="seller_profile",
                message="Replaying seller profile extraction.",
                source_url=str(template.source_url),
            )
            await self._run_group(
                case_id,
                profile_tasks,
                summary="Inspecting seller storefront entry points in parallel.",
                on_complete=lambda replay: setattr(
                    replay,
                    "seller_profile",
                    template.seller_profile.model_copy(deep=True) if template.seller_profile else None,
                ),
            )

        if discovery_tasks:
            await self._append_activity(
                case_id,
                agent_name="seller_listing_discovery",
                message="Replaying seller inventory discovery across storefront shards.",
                source_url=str(template.source_url),
            )
            await self._run_group(
                case_id,
                discovery_tasks,
                summary="Enumerating related listings from seller storefront shards.",
                on_complete=lambda replay: setattr(
                    replay,
                    "discovered_listings",
                    [item.model_copy(deep=True) for item in template.discovered_listings],
                ),
            )

        if triage_tasks:
            await self._append_activity(
                case_id,
                agent_name="seller_listing_triage",
                message="Replaying seller listing shortlist triage.",
                source_url=str(template.source_url),
            )
            await self._run_group(
                case_id,
                triage_tasks,
                summary=(
                    f"Shortlisting {len(template.triage_assessments)} seller listing"
                    f"{'' if len(template.triage_assessments) == 1 else 's'} for deeper review."
                ),
                on_complete=lambda replay: (
                    setattr(
                        replay,
                        "triage_assessments",
                        [item.model_copy(deep=True) for item in template.triage_assessments],
                    ),
                    setattr(
                        replay,
                        "shortlisted_listing_urls",
                        list(template.shortlisted_listing_urls),
                    ),
                ),
            )

        if official_match_tasks:
            await self._append_activity(
                case_id,
                agent_name="official_product_match",
                message="Replaying official-site matching for shortlisted seller listings.",
                source_url=str(template.source_url),
            )
            await self._run_group(
                case_id,
                official_match_tasks,
                summary=(
                    f"Matching {len(template.official_product_matches)} shortlisted seller listing"
                    f"{'' if len(template.official_product_matches) == 1 else 's'} to official product pages."
                ),
                on_complete=lambda replay: setattr(
                    replay,
                    "official_product_matches",
                    [item.model_copy(deep=True) for item in template.official_product_matches],
                ),
            )

        if analysis_tasks:
            await self._append_activity(
                case_id,
                agent_name="seller_listing_analysis",
                message="Replaying deep seller listing analysis.",
                source_url=str(template.source_url),
            )
            await self._run_group(
                case_id,
                analysis_tasks,
                summary=(
                    f"Analyzing {len(template.suspect_listings)} shortlisted seller listing"
                    f"{'' if len(template.suspect_listings) == 1 else 's'} in parallel."
                ),
                on_complete=lambda replay: setattr(
                    replay,
                    "suspect_listings",
                    [item.model_copy(deep=True) for item in template.suspect_listings],
                ),
            )

        if evidence_task is not None:
            await self._append_activity(
                case_id,
                agent_name="seller_case_evidence",
                message="Replaying seller-level evidence synthesis.",
                source_url=str(template.source_url),
            )
            await self._run_single(
                case_id,
                evidence_task,
                summary="Synthesizing seller-level evidence.",
                on_complete=lambda replay: setattr(
                    replay,
                    "evidence",
                    [item.model_copy(deep=True) for item in template.evidence],
                ),
            )

        if draft_task is not None:
            await self._append_activity(
                case_id,
                agent_name="case_draft",
                message="Replaying the seller enforcement case draft.",
                source_url=str(template.source_url),
            )
            await self._run_single(
                case_id,
                draft_task,
                summary="Drafting the seller enforcement case.",
                on_complete=lambda replay: (
                    setattr(
                        replay,
                        "action_request_draft",
                        template.action_request_draft.model_copy(deep=True)
                        if template.action_request_draft
                        else None,
                    ),
                    setattr(
                        replay,
                        "summary",
                        template.summary,
                    ),
                ),
            )
        else:
            await self._mutate(
                case_id,
                lambda replay: setattr(replay, "summary", template.summary),
            )

    async def _run_group(
        self,
        case_id: str,
        templates: list[AgentTaskState],
        *,
        summary: str,
        on_complete: Callable[[SellerCaseResponse], Any] | None = None,
    ) -> None:
        running_tasks = [DemoReplayService._new_running_task(template) for template in templates]

        def start(replay: SellerCaseResponse) -> None:
            replay.status = SellerCaseStatus.running
            replay.summary = summary
            replay.error = None
            replay.raw_agent_outputs.extend(running_tasks)

        await self._mutate(case_id, start)
        await self._pause_work_unit(0.65)
        for task, template_task in zip(running_tasks, templates):
            DemoReplayService._complete_task(task, template_task)

        def finish(replay: SellerCaseResponse) -> None:
            if on_complete is not None:
                on_complete(replay)

        await self._mutate(case_id, finish)
        await self._pause_stage()

    async def _run_single(
        self,
        case_id: str,
        template: AgentTaskState,
        *,
        summary: str,
        on_complete: Callable[[SellerCaseResponse], Any] | None = None,
    ) -> None:
        await self._run_group(
            case_id,
            [template],
            summary=summary,
            on_complete=on_complete,
        )
