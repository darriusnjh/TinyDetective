"""Candidate discovery agent."""

from __future__ import annotations

import asyncio
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
        search_queries = self._build_search_queries(source_product)
        site_query_pairs = [
            (site, search_query)
            for site in comparison_sites
            for search_query in search_queries
        ]
        site_results = await asyncio.gather(
            *[
                self.adapter.search(
                    source_product,
                    site,
                    search_query=search_query,
                    top_n=top_n,
                )
                for site, search_query in site_query_pairs
            ]
        )
        candidates_by_url: dict[str, CandidateProduct] = {}
        raw_outputs: list[dict[str, Any]] = []
        for (site, search_query), (site_candidates, raw_output) in zip(
            site_query_pairs,
            site_results,
            strict=False,
        ):
            raw_outputs.append(
                {
                    "comparison_site": site,
                    "search_query": search_query,
                    **raw_output,
                }
            )
            for candidate in site_candidates:
                candidate_url = str(candidate.product_url)
                existing = candidates_by_url.get(candidate_url)
                if existing is None:
                    candidates_by_url[candidate_url] = candidate
                    continue
                merged_queries = list(
                    dict.fromkeys(existing.discovery_queries + candidate.discovery_queries)
                )
                existing.discovery_queries = merged_queries
        return list(candidates_by_url.values()), raw_outputs

    def _build_search_queries(self, source_product: SourceProduct) -> list[str]:
        brand = self._clean(source_product.brand)
        exact_name = self._clean(source_product.product_name)
        exact_query = ""
        product_type = self._product_type(source_product)
        size = self._clean(source_product.size)
        material = self._clean(source_product.material)
        color = self._clean(source_product.color)
        feature_terms = [self._feature_fragment(feature) for feature in source_product.features]

        queries: list[str] = []
        if brand and product_type:
            queries.append(f"{brand} {product_type}")
        if brand and material and product_type:
            queries.append(f"{brand} {material} {product_type}")
        if brand and size and product_type:
            queries.append(f"{brand} {size} {product_type}")
        if brand and color and product_type:
            queries.append(f"{brand} {color} {product_type}")
        for feature in feature_terms[:2]:
            if brand and product_type and feature:
                queries.append(f"{brand} {feature} {product_type}")

        # Keep a single relaxed fallback if extraction was sparse.
        if brand and source_product.category:
            queries.append(f"{brand} {self._clean(source_product.category)}")

        if exact_name:
            if brand and not exact_name.startswith(f"{brand} "):
                exact_query = f"{brand} {exact_name}"
            else:
                exact_query = exact_name

        deduped: list[str] = []
        for query in queries:
            normalized = self._clean(query)
            if not normalized:
                continue
            if normalized not in deduped:
                deduped.append(normalized)
        limited = deduped[:4] if exact_query else deduped[:5]
        normalized_exact = self._clean(exact_query)
        if normalized_exact and normalized_exact not in limited:
            limited.append(normalized_exact)
        return limited

    @staticmethod
    def _product_type(source_product: SourceProduct) -> str:
        for value in (source_product.subcategory, source_product.category):
            cleaned = CandidateDiscoveryAgent._clean(value)
            if cleaned:
                return cleaned
        return "product"

    @staticmethod
    def _feature_fragment(feature: str | None) -> str:
        cleaned = CandidateDiscoveryAgent._clean(feature)
        if not cleaned:
            return ""
        tokens = cleaned.split()
        return " ".join(tokens[:3])

    @staticmethod
    def _clean(value: str | None) -> str:
        if not value:
            return ""
        return " ".join(value.lower().replace("/", " ").replace("-", " ").split())
