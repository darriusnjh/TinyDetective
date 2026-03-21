"""Ranking agent."""

from __future__ import annotations

from models.schemas import ComparisonResult


class RankingAgent:
    """Rank candidates with precision-oriented heuristics."""

    TOP_MATCH_LIMIT = 5

    async def run(self, comparisons: list[ComparisonResult]) -> list[ComparisonResult]:
        ranked = sorted(
            comparisons,
            key=lambda item: (
                1 if item.is_exact_match else 0,
                item.match_score - (item.counterfeit_risk_score * 0.35),
                -item.counterfeit_risk_score,
            ),
            reverse=True,
        )
        return ranked[: self.TOP_MATCH_LIMIT]
