# TinyDetective

TinyDetective is an MVP counterfeit research platform with a modular multi-agent pipeline. It accepts official source product URLs plus comparison-site inputs, runs TinyFish-powered source extraction and candidate analysis, then returns ranked findings with evidence and a concise risk summary.

This version is wired to the real TinyFish service for live browser automations. The orchestration runtime remains modular so the workflow can evolve without changing agent interfaces.

## Architecture

- `backend/`: FastAPI app and API entrypoint.
- `agents/`: Source extraction, discovery, comparison, evidence, ranking, and summary agents.
- `adapters/`: TinyFish-backed official-product extraction and marketplace candidate discovery adapters.
- `models/`: Typed Pydantic schemas for API payloads and pipeline data.
- `services/`: Investigation orchestrator, in-memory persistence, and TinyFish-compatible runtime abstraction.
- `frontend/`: Minimal static UI for launching investigations and inspecting results.
- `tests/`: Basic tests and sample fixture output.

## Workflow

1. `POST /investigate` creates an investigation and starts async orchestration.
2. `SourceExtractionAgent` extracts a normalized `SourceProduct`.
3. `CandidateDiscoveryAgent` searches the target comparison sites with TinyFish.
4. `ProductComparisonAgent` scores similarity and counterfeit risk.
5. `EvidenceAgent` converts comparisons into audit-friendly evidence.
6. `RankingAgent` returns up to 3 precision-oriented matches.
7. `ResearchSummaryAgent` writes the final source-level investigation summary.
8. `GET /investigation/{id}` returns status, reports, and raw agent outputs.

## Setup

```bash
uv sync --dev
uv run python -m backend
```

You can also run the backend entry file directly:

```bash
cd backend
uv run main.py
```

Open `http://127.0.0.1:8000`.

## Dependency Management

```bash
uv add <package>
uv add --dev <package>
uv sync --dev
uv run pytest
```

## Environment

Create `.env` from `.env.example` and set:

```bash
TINYFISH_API_KEY=your-real-key
TINYFISH_HTTP_TIMEOUT_SECONDS=15.0
TINYFISH_RUN_SOFT_TIMEOUT_SECONDS=300.0
TINYFISH_RUN_HARD_TIMEOUT_SECONDS=1800.0
TINYFISH_RUN_STALL_TIMEOUT_SECONDS=120.0
BRAND_LANDING_PAGE_URL=https://www.yourbrand.com/
ECOMMERCE_STORE_URLS=https://shopee.sg/,https://www.lazada.sg/
```

If `comparison_sites` is omitted from `POST /investigate`, the backend falls back to `ECOMMERCE_STORE_URLS`.

## Notes

- Persistence is currently in-memory and resets on restart.
- TinyFish calls are made through `services/tinyfish_client.py`, which uses the documented async run and run-status endpoints.
- `services/tinyfish_runtime.py` keeps orchestration logic separate from execution flow.
- The UI exposes extracted source data, ranked results, evidence, suspicious signals, and raw agent reasoning.
