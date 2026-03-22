"""Demo replay mode tests."""

from __future__ import annotations

import asyncio

from models.case_schemas import (
    ActionRequestDraft,
    OfficialProductMatch,
    SellerCaseCreateRequest,
    SellerCaseStatus,
    SellerListing,
    SellerListingTriageAssessment,
    SellerProfile,
)
from models.schemas import (
    AgentTaskState,
    CandidateProduct,
    ComparisonResult,
    InvestigationCreateRequest,
    InvestigationReport,
    InvestigationStatus,
    SourceProduct,
    TaskStatus,
)
from services.demo_replay_service import DemoReplayService, DemoSellerCaseReplayService
from services.investigation_store import InvestigationStore, normalize_source_url


async def _no_sleep(_: float) -> None:
    return None


def _build_completed_report(
    source_url: str,
    *,
    summary: str,
    synthesized_discovery: bool = False,
    top_matches_only: bool = False,
) -> InvestigationReport:
    source_product = SourceProduct(
        source_url=source_url,
        brand="Brand",
        product_name="Alpha Case",
        category="Accessories",
        subcategory="Phone Case",
    )
    candidates = [
        CandidateProduct(
            product_url=f"https://market.example/listing-{index}",
            marketplace="market.example",
            title=f"Alpha Case Listing {index}",
            discovery_queries=[f"brand alpha case query {index}"],
        )
        for index in range(1, 4)
    ]
    comparisons = [
        ComparisonResult(
            source_url=source_url,
            product_url=str(candidate.product_url),
            marketplace=candidate.marketplace,
            match_score=round(0.52 + (index * 0.05), 2),
            is_exact_match=False,
            counterfeit_risk_score=round(0.68 + (index * 0.07), 2),
            suspicious_signals=["title_similarity", "low_price"],
            reason=f"Saved suspicious comparison {index}.",
            candidate_product=candidate,
        )
        for index, candidate in enumerate(candidates, start=1)
    ]

    source_task = AgentTaskState(
        agent_name="source_extraction",
        status=TaskStatus.completed,
        input_payload={"source_url": source_url},
        output_payload={"source_product": source_product.model_dump()},
    )
    if synthesized_discovery:
        discovery_tasks = [
            AgentTaskState(
                agent_name="candidate_discovery",
                status=TaskStatus.completed,
                input_payload={
                    "comparison_site": "https://market.example/",
                    "search_query": "brand alpha case broad query",
                },
                output_payload={
                    "comparison_site": "https://market.example/",
                    "search_query": "brand alpha case broad query",
                    "candidate_count": len(candidates),
                    "candidates": [candidate.model_dump() for candidate in candidates],
                },
            )
        ]
    else:
        discovery_tasks = [
            AgentTaskState(
                agent_name="candidate_discovery",
                status=TaskStatus.completed,
                input_payload={
                    "comparison_site": "https://market.example/",
                    "search_query": candidate.discovery_queries[0],
                },
                output_payload={
                    "comparison_site": "https://market.example/",
                    "search_query": candidate.discovery_queries[0],
                    "candidate_count": 1,
                    "candidates": [candidate.model_dump()],
                },
            )
            for candidate in candidates
        ]

    comparison_tasks = [
        AgentTaskState(
            agent_name="product_comparison",
            status=TaskStatus.completed,
            input_payload={"product_url": str(comparison.product_url)},
            output_payload={"comparison": comparison.model_dump()},
        )
        for comparison in comparisons
    ]
    ranking_task = AgentTaskState(
        agent_name="ranking",
        status=TaskStatus.completed,
        input_payload={"comparison_count": len(comparisons)},
        output_payload={
            "ranked_product_urls": [str(comparison.product_url) for comparison in comparisons],
        },
    )
    summary_task = AgentTaskState(
        agent_name="research_summary",
        status=TaskStatus.completed,
        input_payload={"top_match_count": len(comparisons)},
        output_payload={"summary": summary},
    )

    raw_agent_outputs = [
        source_task,
        *discovery_tasks,
        *comparison_tasks,
        ranking_task,
        summary_task,
    ]
    if top_matches_only:
        raw_agent_outputs = [source_task, ranking_task, summary_task]

    return InvestigationReport(
        source_url=source_url,
        extracted_source_product=source_product,
        top_matches=comparisons,
        summary=summary,
        raw_agent_outputs=raw_agent_outputs,
    )


async def _create_completed_investigation(
    store: InvestigationStore,
    source_url: str,
    *,
    summary: str,
    synthesized_discovery: bool = False,
    top_matches_only: bool = False,
):
    created = await store.create(
        InvestigationCreateRequest(
            source_urls=[source_url],
            comparison_sites=["https://market.example/"],
        )
    )
    saved = await store.get(created.investigation_id)
    assert saved is not None
    saved.status = InvestigationStatus.completed
    saved.reports = [
        _build_completed_report(
            source_url,
            summary=summary,
            synthesized_discovery=synthesized_discovery,
            top_matches_only=top_matches_only,
        )
    ]
    await store.save(saved)
    return saved


async def _create_completed_seller_case(
    store: InvestigationStore,
    source_url: str,
    product_url: str,
    *,
    summary: str,
):
    investigation = await _create_completed_investigation(
        store,
        source_url,
        summary=f"source for {summary}",
    )
    source_report = investigation.reports[0]
    selected_listing = next(
        comparison
        for comparison in source_report.top_matches
        if normalize_source_url(str(comparison.product_url)) == normalize_source_url(product_url)
    )
    seller_profile = SellerProfile(
        seller_name="Demo Seller",
        seller_url="https://market.example/store/demo-seller",
        marketplace=selected_listing.marketplace,
        entry_urls=["https://market.example/store/demo-seller"],
        storefront_shard_urls=[
            "https://market.example/store/demo-seller?page=1",
            "https://market.example/store/demo-seller?page=2",
        ],
    )
    discovered_listings = [
        SellerListing(
            product_url=str(comparison.product_url),
            marketplace=comparison.marketplace,
            seller_name="Demo Seller",
            seller_store_url="https://market.example/store/demo-seller",
            title=comparison.candidate_product.title,
            price=comparison.candidate_product.price,
            currency=comparison.candidate_product.currency,
            brand=comparison.candidate_product.brand,
            model=comparison.candidate_product.model,
            description=comparison.candidate_product.description,
        )
        for comparison in source_report.top_matches
    ]
    triage_assessments = [
        SellerListingTriageAssessment(
            product_url=str(listing.product_url),
            investigation_priority_score=0.82,
            suspicion_score=0.77,
            should_shortlist=True,
            rationale="Saved seller-case shortlist rationale.",
            suspicious_signals=["repeat_brand_targeting"],
        )
        for listing in discovered_listings
    ]
    official_matches = [
        OfficialProductMatch(
            product_url=str(listing.product_url),
            official_product_url=source_url,
            official_product=source_report.extracted_source_product.model_copy(deep=True),
            match_confidence=0.88,
            rationale="Matched back to the official source product.",
            search_queries=["brand alpha case"],
        )
        for listing in discovered_listings
    ]
    suspect_listings = [item.model_copy(deep=True) for item in source_report.top_matches]
    evidence = []
    draft = ActionRequestDraft(
        case_title="Demo Seller Enforcement Case",
        summary=summary,
        reasoning="Seller repeatedly lists suspicious products matching the protected brand.",
        suspected_violation_type="suspected counterfeit",
        recommended_action="manual review",
        request_text="Please review this seller and the referenced listings for potential enforcement action.",
        evidence_references=[product_url],
        confidence=0.79,
    )

    created_case = await store.create_case(
        SellerCaseCreateRequest(
            investigation_id=investigation.investigation_id,
            source_url=source_url,
            product_url=product_url,
        )
    )
    saved_case = await store.get_case(created_case.case_id)
    assert saved_case is not None
    saved_case.status = SellerCaseStatus.completed
    saved_case.summary = summary
    saved_case.source_product = source_report.extracted_source_product.model_copy(deep=True)
    saved_case.selected_listing = selected_listing.model_copy(deep=True)
    saved_case.marketplace = selected_listing.marketplace
    saved_case.seller_name = "Demo Seller"
    saved_case.seller_store_url = "https://market.example/store/demo-seller"
    saved_case.seller_profile = seller_profile
    saved_case.discovered_listings = discovered_listings
    saved_case.triage_assessments = triage_assessments
    saved_case.shortlisted_listing_urls = [str(item.product_url) for item in discovered_listings]
    saved_case.official_product_matches = official_matches
    saved_case.suspect_listings = suspect_listings
    saved_case.evidence = evidence
    saved_case.action_request_draft = draft
    saved_case.raw_agent_outputs = [
        AgentTaskState(
            agent_name="seller_profile",
            status=TaskStatus.completed,
            input_payload={"entry_url": seller_profile.entry_urls[0]},
            output_payload={"seller_profile": seller_profile.model_dump()},
        ),
        AgentTaskState(
            agent_name="seller_listing_discovery",
            status=TaskStatus.completed,
            input_payload={"shard_url": seller_profile.storefront_shard_urls[0]},
            output_payload={"discovered_listings": [item.model_dump() for item in discovered_listings]},
        ),
        *[
            AgentTaskState(
                agent_name="seller_listing_triage",
                status=TaskStatus.completed,
                input_payload={"product_url": item.product_url},
                output_payload={"triage": item.model_dump()},
            )
            for item in triage_assessments
        ],
        *[
            AgentTaskState(
                agent_name="official_product_match",
                status=TaskStatus.completed,
                input_payload={"product_url": item.product_url},
                output_payload={"official_match": item.model_dump()},
            )
            for item in official_matches
        ],
        *[
            AgentTaskState(
                agent_name="seller_listing_analysis",
                status=TaskStatus.completed,
                input_payload={"product_url": str(item.product_url)},
                output_payload={"comparison": item.model_dump()},
            )
            for item in suspect_listings
        ],
        AgentTaskState(
            agent_name="seller_case_evidence",
            status=TaskStatus.completed,
            input_payload={"suspect_listing_count": len(suspect_listings)},
            output_payload={"evidence": []},
        ),
        AgentTaskState(
            agent_name="case_draft",
            status=TaskStatus.completed,
            input_payload={"evidence_count": 0},
            output_payload={"draft": draft.model_dump()},
        ),
    ]
    await store.save_case(saved_case)
    return saved_case


def test_store_matches_latest_completed_run_by_normalized_source_url(tmp_path) -> None:
    async def run() -> None:
        store = InvestigationStore(tmp_path / "demo-match.sqlite3")
        first = await _create_completed_investigation(
            store,
            "https://brand.example/products/alpha-case/",
            summary="older replay",
        )
        second = await _create_completed_investigation(
            store,
            "https://brand.example/products/alpha-case",
            summary="newer replay",
        )

        matched = await store.find_latest_completed_by_source_urls(
            ["https://brand.example/products/alpha-case/"]
        )

        assert matched is not None
        assert matched.investigation_id == second.investigation_id
        assert matched.reports[0].summary == "newer replay"
        assert matched.investigation_id != first.investigation_id

    asyncio.run(run())


def test_demo_replays_are_in_memory_only_and_not_saved_to_sqlite(tmp_path) -> None:
    async def run() -> None:
        store = InvestigationStore(tmp_path / "demo-replay.sqlite3")
        await _create_completed_investigation(
            store,
            "https://brand.example/products/alpha-case",
            summary="completed replay source",
        )
        recent_before = await store.list_recent(limit=10)

        replay_service = DemoReplayService(
            store,
            step_delay_seconds=0.0,
            sleep=_no_sleep,
        )
        demo = await replay_service.create_replay(
            InvestigationCreateRequest(
                source_urls=["https://brand.example/products/alpha-case/"],
                comparison_sites=["https://market.example/"],
            )
        )
        assert demo.status == InvestigationStatus.queued

        await replay_service.wait(demo.investigation_id)
        replay = await replay_service.get(demo.investigation_id)
        assert replay is not None
        assert replay.status == InvestigationStatus.completed
        assert await store.get(demo.investigation_id) is None

        recent_after = await store.list_recent(limit=10)
        assert [item.investigation_id for item in recent_after] == [
            item.investigation_id for item in recent_before
        ]
        assert all(item.investigation_id != demo.investigation_id for item in recent_after)

    asyncio.run(run())


def test_demo_replay_synthesizes_search_fanout_from_saved_comparisons(tmp_path) -> None:
    async def run() -> None:
        store = InvestigationStore(tmp_path / "demo-fanout.sqlite3")
        await _create_completed_investigation(
            store,
            "https://brand.example/products/alpha-case",
            summary="fanout replay",
            synthesized_discovery=True,
        )

        replay_service = DemoReplayService(
            store,
            step_delay_seconds=0.0,
            sleep=_no_sleep,
        )
        demo = await replay_service.create_replay(
            InvestigationCreateRequest(
                source_urls=["https://brand.example/products/alpha-case"],
                comparison_sites=["https://market.example/"],
            )
        )
        await replay_service.wait(demo.investigation_id)
        replay = await replay_service.get(demo.investigation_id)

        assert replay is not None
        discovery_tasks = [
            task
            for task in replay.reports[0].raw_agent_outputs
            if task.agent_name == "candidate_discovery"
        ]
        search_queries = {
            task.output_payload.get("search_query") or task.input_payload.get("search_query")
            for task in discovery_tasks
        }

        assert len(discovery_tasks) == 3
        assert search_queries == {
            "brand alpha case query 1",
            "brand alpha case query 2",
            "brand alpha case query 3",
        }

    asyncio.run(run())


def test_demo_replay_synthesizes_missing_intermediate_steps_from_top_matches(tmp_path) -> None:
    async def run() -> None:
        store = InvestigationStore(tmp_path / "demo-sparse.sqlite3")
        await _create_completed_investigation(
            store,
            "https://brand.example/products/alpha-case",
            summary="sparse replay",
            top_matches_only=True,
        )

        replay_service = DemoReplayService(
            store,
            step_delay_seconds=0.0,
            sleep=_no_sleep,
        )
        demo = await replay_service.create_replay(
            InvestigationCreateRequest(
                source_urls=["https://brand.example/products/alpha-case"],
                comparison_sites=["https://market.example/"],
            )
        )
        await replay_service.wait(demo.investigation_id)
        replay = await replay_service.get(demo.investigation_id)

        assert replay is not None
        tasks = replay.reports[0].raw_agent_outputs
        task_names = [task.agent_name for task in tasks]

        assert "candidate_discovery" in task_names
        assert "candidate_triage" in task_names
        assert "product_comparison" in task_names
        assert "evidence" in task_names
        assert "reasoning_enrichment" in task_names

        discovery_tasks = [task for task in tasks if task.agent_name == "candidate_discovery"]
        triage_tasks = [task for task in tasks if task.agent_name == "candidate_triage"]
        comparison_tasks = [task for task in tasks if task.agent_name == "product_comparison"]

        assert len(discovery_tasks) == 3
        assert len(triage_tasks) == 3
        assert len(comparison_tasks) == 3

    asyncio.run(run())


def test_store_matches_latest_completed_seller_case_by_normalized_urls(tmp_path) -> None:
    async def run() -> None:
        store = InvestigationStore(tmp_path / "demo-case-match.sqlite3")
        await _create_completed_seller_case(
            store,
            "https://brand.example/products/alpha-case/",
            "https://market.example/listing-1/",
            summary="older seller case",
        )
        newest = await _create_completed_seller_case(
            store,
            "https://brand.example/products/alpha-case",
            "https://market.example/listing-1",
            summary="newer seller case",
        )

        matched = await store.find_latest_completed_case_by_source_and_product_url(
            "https://brand.example/products/alpha-case/",
            "https://market.example/listing-1/",
        )

        assert matched is not None
        assert matched.case_id == newest.case_id
        assert matched.summary == "newer seller case"

    asyncio.run(run())


def test_demo_seller_case_replays_are_in_memory_only_and_not_saved_to_sqlite(tmp_path) -> None:
    async def run() -> None:
        store = InvestigationStore(tmp_path / "demo-seller-case.sqlite3")
        saved_case = await _create_completed_seller_case(
            store,
            "https://brand.example/products/alpha-case",
            "https://market.example/listing-1",
            summary="saved seller case replay",
        )
        recent_before = await store.list_recent_cases(limit=10)

        replay_service = DemoSellerCaseReplayService(
            store,
            step_delay_seconds=0.0,
            sleep=_no_sleep,
        )
        demo_case = await replay_service.create_replay(
            SellerCaseCreateRequest(
                investigation_id="demo-investigation-id",
                source_url="https://brand.example/products/alpha-case/",
                product_url="https://market.example/listing-1/",
            )
        )

        assert demo_case.status == SellerCaseStatus.queued
        assert demo_case.investigation_id == "demo-investigation-id"

        await replay_service.wait(demo_case.case_id)
        replayed = await replay_service.get(demo_case.case_id)
        assert replayed is not None
        assert replayed.status == SellerCaseStatus.completed
        assert replayed.summary == "saved seller case replay"
        assert await store.get_case(demo_case.case_id) is None

        recent_after = await store.list_recent_cases(limit=10)
        assert [item.case_id for item in recent_after] == [item.case_id for item in recent_before]
        assert all(item.case_id != demo_case.case_id for item in recent_after)
        assert saved_case.case_id in {item.case_id for item in recent_after}

    asyncio.run(run())
