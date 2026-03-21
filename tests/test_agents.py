"""Non-network agent tests."""

from __future__ import annotations

import asyncio

from agents.evidence_agent import EvidenceAgent
from agents.product_comparison_agent import ProductComparisonAgent
from agents.ranking_agent import RankingAgent
from models.schemas import CandidateProduct, ComparisonResult, SourceProduct


class StubComparisonAdapter:
    async def fetch_candidate_product(self, candidate_url: str, marketplace: str):
        candidate = CandidateProduct(
            product_url=candidate_url,
            marketplace=marketplace,
            seller_name="Discount Device Hub",
            title="Impact Case Hello Kitty Compatible Case",
            price=19.9,
            currency="SGD",
            brand="CasetifyX",
            color="Midnight Black",
            size="iPhone 16 Pro",
            material="Shock-absorbing TPU",
            model="CAS-1234",
            sku="CAS-HELLO1-ALT",
            description="Premium impact protection with MagSafe support. Compatible edition.",
            image_urls=[],
        )
        return candidate, {"tinyfish_run_id": "stub-run", "tinyfish_status": "COMPLETED"}


def test_product_comparison_agent_flags_low_priced_copy() -> None:
    async def run() -> None:
        source_product = SourceProduct(
            source_url="https://brand.example/products/alpha-case",
            brand="Casetify",
            product_name="Impact Case Hello Kitty",
            category="Accessories",
            subcategory="Phone Case",
            price=89.0,
            currency="SGD",
            color="Midnight Black",
            size="iPhone 16 Pro",
            material="Shock-absorbing TPU",
            model="CAS-1234",
            sku="CAS-HELLO1",
            description="Premium impact protection with MagSafe support.",
        )
        candidate = CandidateProduct(product_url="https://shopee.sg/product/alpha-copy", marketplace="Shopee")
        agent = ProductComparisonAgent(adapter=StubComparisonAdapter())
        result, _ = await agent.run(source_product, candidate)
        assert result.counterfeit_risk_score >= 0.6
        assert "suspiciously_low_price" in result.suspicious_signals
        assert "brand_mismatch" in result.suspicious_signals

    asyncio.run(run())


def test_evidence_agent_emits_structured_differences() -> None:
    async def run() -> None:
        source_product = SourceProduct(
            source_url="https://brand.example/products/alpha-case",
            brand="Casetify",
            product_name="Impact Case Hello Kitty",
            price=89.0,
            currency="SGD",
            color="Midnight Black",
            size="iPhone 16 Pro",
            material="Shock-absorbing TPU",
            model="CAS-1234",
            sku="CAS-HELLO1",
            description="Premium impact protection with MagSafe support.",
        )
        comparison_agent = ProductComparisonAgent(adapter=StubComparisonAdapter())
        candidate = CandidateProduct(product_url="https://shopee.sg/product/alpha-copy", marketplace="Shopee")
        comparison, _ = await comparison_agent.run(source_product, candidate)
        evidence = await EvidenceAgent().run(source_product, comparison)
        assert any(item.field == "price" for item in evidence)
        assert any(item.field == "brand" for item in evidence)

    asyncio.run(run())


def test_ranking_agent_returns_five_matches() -> None:
    async def run() -> None:
        source_url = "https://brand.example/products/alpha-case"
        comparisons = [
            ComparisonResult(
                source_url=source_url,
                product_url=f"https://market.example/listing/{index}",
                marketplace="Shopee",
                match_score=0.9 - (index * 0.05),
                is_exact_match=index < 2,
                counterfeit_risk_score=0.15 + (index * 0.1),
                suspicious_signals=[],
                reason=f"Candidate {index}",
                candidate_product=CandidateProduct(
                    product_url=f"https://market.example/listing/{index}",
                    marketplace="Shopee",
                ),
            )
            for index in range(6)
        ]

        ranked = await RankingAgent().run(comparisons)

        assert len(ranked) == 5
        assert [str(item.product_url) for item in ranked] == [
            "https://market.example/listing/0",
            "https://market.example/listing/1",
            "https://market.example/listing/2",
            "https://market.example/listing/3",
            "https://market.example/listing/4",
        ]

    asyncio.run(run())
