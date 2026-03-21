"""TinyFish-backed marketplace discovery and extraction adapter."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

from models.schemas import CandidateProduct, SourceProduct
from services.tinyfish_client import TinyFishClient, TinyFishRun


class TinyFishComparisonSiteAdapter:
    """Use TinyFish to search marketplace sites and extract candidate product pages."""

    def __init__(self, client: TinyFishClient | None = None) -> None:
        self.client = client or TinyFishClient()

    async def search(
        self,
        source_product: SourceProduct,
        comparison_site: str,
        top_n: int = 3,
    ) -> tuple[list[CandidateProduct], dict[str, Any]]:
        marketplace = self._marketplace_name(comparison_site)
        goal = (
            f"You are investigating counterfeit or suspicious product listings. Search this marketplace or store "
            f"for up to {top_n} candidate listings that may match the official source product. "
            f"Official product details: brand={source_product.brand!r}, product_name={source_product.product_name!r}, "
            f"category={source_product.category!r}, subcategory={source_product.subcategory!r}, "
            f"price={source_product.price!r} {source_product.currency!r}, color={source_product.color!r}, "
            f"size={source_product.size!r}, material={source_product.material!r}, model={source_product.model!r}, "
            f"sku={source_product.sku!r}, features={source_product.features!r}. "
            "Return valid JSON only with this exact shape: "
            '{"candidates":[{"product_url":"","marketplace":"","seller_name":"","title":"","price":0,'
            '"currency":"","brand":"","color":"","size":"","material":"","model":"","sku":"",'
            '"description":"","image_urls":[]}]} '
            "Only include real listing URLs found on this site. Do not fabricate listings."
        )
        run = await self.client.run_json(comparison_site, goal)
        result = self._coerce_result_object(run)
        candidates = [
            CandidateProduct.model_validate({**candidate, "marketplace": candidate.get("marketplace") or marketplace})
            for candidate in result.get("candidates", [])
            if candidate.get("product_url")
        ]
        return candidates[:top_n], self._raw_output(run)

    async def fetch_candidate_product(
        self,
        candidate_url: str,
        marketplace: str,
    ) -> tuple[CandidateProduct, dict[str, Any]]:
        goal = (
            "Visit this product listing page and extract structured product data for counterfeit research. "
            "Return valid JSON only with this exact shape: "
            '{"seller_name":"","title":"","price":0,"currency":"","brand":"","color":"","size":"",'
            '"material":"","model":"","sku":"","description":"","image_urls":[]} '
            "Use null for unknown scalar values and [] for unknown lists. Do not invent values."
        )
        run = await self.client.run_json(candidate_url, goal)
        result = self._coerce_result_object(run)
        result["product_url"] = candidate_url
        result["marketplace"] = marketplace
        return CandidateProduct.model_validate(result), self._raw_output(run)

    @staticmethod
    def _coerce_result_object(run: TinyFishRun) -> dict[str, Any]:
        result = run.result
        if isinstance(result, dict):
            return result
        if isinstance(result, str):
            try:
                return json.loads(result)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Marketplace result was not valid JSON: {result}") from exc
        raise ValueError(f"Unexpected TinyFish marketplace result: {result!r}")

    @staticmethod
    def _marketplace_name(site: str) -> str:
        host = urlparse(site).netloc.lower().replace("www.", "")
        return (host.split(".")[0] if host else site).title()

    @staticmethod
    def _raw_output(run: TinyFishRun) -> dict[str, Any]:
        return {
            "tinyfish_run_id": run.run_id,
            "tinyfish_status": run.status,
            "tinyfish_result": run.result,
        }
