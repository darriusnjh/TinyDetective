"""Candidate discovery agent."""

from __future__ import annotations

from typing import Any

from adapters.comparison_site_adapter import TinyFishComparisonSiteAdapter
from models.schemas import CandidateProduct, SourceProduct


class CandidateDiscoveryAgent:
    """Find likely marketplace candidates per comparison site."""

    def __init__(self, adapter: TinyFishComparisonSiteAdapter | None = None) -> None:
        self.adapter = adapter or TinyFishComparisonSiteAdapter()

    async def run(
        self,
        source_product: SourceProduct,
        comparison_sites: list[str],
        top_n: int = 3,
    ) -> tuple[list[CandidateProduct], list[dict[str, Any]]]:
        candidates: list[CandidateProduct] = []
        raw_outputs: list[dict[str, Any]] = []
        for site in comparison_sites:
            site_candidates, raw_output = await self.adapter.search(source_product, site, top_n=top_n)
            candidates.extend(site_candidates)
            raw_outputs.append({"comparison_site": site, **raw_output})
        return candidates, raw_outputs
