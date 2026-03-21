"""Candidate discovery agent."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

from adapters.comparison_site_adapter import TinyFishComparisonSiteAdapter
from models.schemas import CandidateProduct, SourceProduct
from services.tinyfish_client import TinyFishRun


class CandidateDiscoveryAgent:
    """Find likely marketplace candidates per comparison site."""

    def __init__(self, adapter: TinyFishComparisonSiteAdapter | None = None) -> None:
        self.adapter = adapter or TinyFishComparisonSiteAdapter()

    async def run(
        self,
        source_product: SourceProduct,
        comparison_sites: list[str],
        top_n: int = 3,
        on_update: Callable[[TinyFishRun], Awaitable[None] | None] | None = None,
    ) -> tuple[list[CandidateProduct], list[dict[str, Any]]]:
        candidates: list[CandidateProduct] = []
        raw_outputs: list[dict[str, Any]] = []
        for site in comparison_sites:
            if on_update is None:
                site_candidates, raw_output = await self.adapter.search(source_product, site, top_n=top_n)
            else:
                site_candidates, raw_output = await self.adapter.search(
                    source_product,
                    site,
                    top_n=top_n,
                    on_update=on_update,
                )
            candidates.extend(site_candidates)
            raw_outputs.append({"comparison_site": site, **raw_output})
        return candidates, raw_outputs

    async def run_for_site(
        self,
        source_product: SourceProduct,
        comparison_site: str,
        top_n: int = 3,
        on_update: Callable[[TinyFishRun], Awaitable[None] | None] | None = None,
    ) -> tuple[list[CandidateProduct], dict[str, Any]]:
        if on_update is None:
            return await self.adapter.search(source_product, comparison_site, top_n=top_n)
        return await self.adapter.search(source_product, comparison_site, top_n=top_n, on_update=on_update)

    async def resume_for_site(
        self,
        source_product: SourceProduct,
        comparison_site: str,
        run_id: str,
        top_n: int = 3,
        on_update: Callable[[TinyFishRun], Awaitable[None] | None] | None = None,
        started_at: datetime | None = None,
        last_progress_at: datetime | None = None,
    ) -> tuple[list[CandidateProduct], dict[str, Any]]:
        if on_update is None:
            return await self.adapter.resume_search(
                source_product,
                comparison_site,
                run_id,
                top_n=top_n,
                started_at=started_at,
                last_progress_at=last_progress_at,
            )
        return await self.adapter.resume_search(
            source_product,
            comparison_site,
            run_id,
            top_n=top_n,
            on_update=on_update,
            started_at=started_at,
            last_progress_at=last_progress_at,
        )
