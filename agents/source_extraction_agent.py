"""Source extraction agent."""

from __future__ import annotations

from typing import Any

from adapters.source_page_adapter import TinyFishSourcePageAdapter
from models.schemas import SourceProduct


class SourceExtractionAgent:
    """Extract normalized source product details from an official URL."""

    def __init__(self, adapter: TinyFishSourcePageAdapter | None = None) -> None:
        self.adapter = adapter or TinyFishSourcePageAdapter()

    async def run(self, source_url: str) -> tuple[SourceProduct, dict[str, Any]]:
        return await self.adapter.extract_product(source_url)
