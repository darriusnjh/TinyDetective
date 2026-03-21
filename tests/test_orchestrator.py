"""Non-network orchestrator-adjacent tests."""

from __future__ import annotations

from models.schemas import InvestigationCreateRequest


def test_investigation_request_defaults_comparison_sites() -> None:
    request = InvestigationCreateRequest(
        source_urls=["https://brand.example/products/alpha-case"],
    )
    assert request.comparison_sites == []
