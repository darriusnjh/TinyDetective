"""Product comparison agent."""

from __future__ import annotations

from typing import Any

from adapters.comparison_site_adapter import TinyFishComparisonSiteAdapter
from models.schemas import CandidateProduct, ComparisonResult, SourceProduct


def counterfeit_risk_score_safe(score: float) -> bool:
    """Guard exact-match classification with a conservative risk threshold."""
    return score <= 0.3


class ProductComparisonAgent:
    """Compare source and candidate products with explainable heuristics."""

    def __init__(self, adapter: TinyFishComparisonSiteAdapter | None = None) -> None:
        self.adapter = adapter or TinyFishComparisonSiteAdapter()

    async def run(
        self,
        source_product: SourceProduct,
        candidate: CandidateProduct,
    ) -> tuple[ComparisonResult, dict[str, Any]]:
        candidate_full, raw_output = await self.adapter.fetch_candidate_product(
            str(candidate.product_url),
            candidate.marketplace,
        )
        comparisons = {
            "brand": self._eq(source_product.brand, candidate_full.brand),
            "title": self._contains(source_product.product_name, candidate_full.title),
            "sku": self._eq(source_product.sku, candidate_full.sku),
            "model": self._eq(source_product.model, candidate_full.model),
            "color": self._eq(source_product.color, candidate_full.color),
            "material": self._eq(source_product.material, candidate_full.material),
            "size": self._eq(source_product.size, candidate_full.size),
            "description": self._description_similarity(
                source_product.description, candidate_full.description
            ),
        }
        match_score = round(
            (
                comparisons["brand"] * 0.20
                + comparisons["title"] * 0.20
                + comparisons["sku"] * 0.20
                + comparisons["model"] * 0.15
                + comparisons["color"] * 0.05
                + comparisons["material"] * 0.05
                + comparisons["size"] * 0.05
                + comparisons["description"] * 0.10
            ),
            2,
        )
        suspicious_signals: list[str] = []
        price_gap = self._price_gap_ratio(source_product.price, candidate_full.price)
        if price_gap >= 0.4:
            suspicious_signals.append("suspiciously_low_price")
        if comparisons["brand"] < 1.0:
            suspicious_signals.append("brand_mismatch")
        if comparisons["sku"] == 0 and candidate_full.sku:
            suspicious_signals.append("sku_mismatch")
        if comparisons["description"] >= 0.7 and price_gap >= 0.4:
            suspicious_signals.append("copied_description_with_discount_pricing")

        counterfeit_risk = round(
            min(
                1.0,
                0.2
                + (0.45 if price_gap >= 0.4 else 0.0)
                + (0.15 if comparisons["brand"] < 1.0 else 0.0)
                + (0.1 if comparisons["sku"] == 0 and candidate_full.sku else 0.0)
                + (0.1 if comparisons["description"] >= 0.7 else 0.0),
            ),
            2,
        )
        is_exact_match = (
            comparisons["brand"] == 1.0
            and comparisons["title"] >= 0.9
            and comparisons["sku"] == 1.0
            and comparisons["model"] == 1.0
            and counterfeit_risk_score_safe(counterfeit_risk)
        )
        reason = self._build_reason(match_score, counterfeit_risk, suspicious_signals)
        comparison = ComparisonResult(
            source_url=source_product.source_url,
            product_url=candidate_full.product_url,
            marketplace=candidate_full.marketplace,
            match_score=match_score,
            is_exact_match=is_exact_match,
            counterfeit_risk_score=counterfeit_risk,
            suspicious_signals=suspicious_signals,
            reason=reason,
            candidate_product=candidate_full,
        )
        return comparison, raw_output

    @staticmethod
    def _eq(left: str | None, right: str | None) -> float:
        return 1.0 if left and right and left.lower() == right.lower() else 0.0

    @staticmethod
    def _contains(left: str | None, right: str | None) -> float:
        if not left or not right:
            return 0.0
        left_norm = left.lower()
        right_norm = right.lower()
        if left_norm == right_norm:
            return 1.0
        if left_norm in right_norm or right_norm in left_norm:
            return 0.8
        overlap = len(set(left_norm.split()) & set(right_norm.split()))
        return min(0.7, overlap / max(len(left_norm.split()), 1))

    @staticmethod
    def _description_similarity(left: str | None, right: str | None) -> float:
        if not left or not right:
            return 0.0
        left_words = set(left.lower().split())
        right_words = set(right.lower().split())
        if not left_words or not right_words:
            return 0.0
        return round(len(left_words & right_words) / len(left_words | right_words), 2)

    @staticmethod
    def _price_gap_ratio(source_price: float | None, candidate_price: float | None) -> float:
        if not source_price or not candidate_price:
            return 0.0
        return round(max(0.0, (source_price - candidate_price) / source_price), 2)

    @staticmethod
    def _build_reason(
        match_score: float,
        counterfeit_risk: float,
        suspicious_signals: list[str],
    ) -> str:
        if match_score >= 0.85 and counterfeit_risk < 0.35:
            return "Strong structured attribute match with limited counterfeit signals."
        if suspicious_signals:
            return (
                "Candidate shares some product attributes but shows risk indicators: "
                + ", ".join(suspicious_signals)
                + "."
            )
        return "Candidate is directionally similar but lacks enough aligned attributes."
