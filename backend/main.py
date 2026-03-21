"""FastAPI entrypoint for the counterfeit research MVP."""

from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from models.schemas import InvestigationCreateRequest, InvestigationResponse
from services.investigation_orchestrator import InvestigationOrchestrator
from services.settings import settings
from services.investigation_store import InvestigationStore


BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"

app = FastAPI(
    title="TinyDetective Counterfeit Research MVP",
    version="0.1.0",
    description="Agent-based counterfeit investigation workflow scaffold.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

store = InvestigationStore()
orchestrator = InvestigationOrchestrator(store=store)

app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "ok",
        "tinyfish_enabled": "true" if settings.tinyfish_enabled else "false",
    }


@app.get("/config")
async def config() -> dict[str, object]:
    return {
        "brand_landing_page_url": settings.brand_landing_page_url,
        "ecommerce_store_urls": settings.ecommerce_store_urls,
        "tinyfish_browser_profile": settings.tinyfish_browser_profile,
    }


@app.post("/investigate", response_model=InvestigationResponse)
async def investigate(payload: InvestigationCreateRequest) -> InvestigationResponse:
    investigation = await store.create(payload)
    asyncio.create_task(orchestrator.run_investigation(investigation.investigation_id))
    return investigation


@app.get("/investigation/{investigation_id}", response_model=InvestigationResponse)
async def get_investigation(investigation_id: str) -> InvestigationResponse:
    investigation = await store.get(investigation_id)
    if investigation is None:
        raise HTTPException(status_code=404, detail="Investigation not found")
    return investigation
