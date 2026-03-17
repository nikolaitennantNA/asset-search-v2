# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A 6-stage async pipeline that discovers physical assets (facilities, plants, mines, warehouses, offices) for corporate entities using LLM agents, web scraping, and structured extraction. Output is TREX ALD-aligned asset records persisted to Postgres.

## Commands

All commands require `uv run` — there is no system-level `python` on this machine.

```bash
# Install (uses uv, requires sibling repos for editable deps)
uv sync

# Run pipeline for a company (requires corp-graph Postgres)
uv run python -m asset_discovery run AU000000BLD2

# Run from JSON profile (no corp-graph needed)
uv run python -m asset_discovery run --from-file boral.json

# Partial run (stop after a specific stage)
uv run python -m asset_discovery run --from-file boral.json --stop-after discover

# Resume from a specific stage (loads prior results from DB/cache)
uv run python -m asset_discovery run AU000000BLD2 --start-from extract

# Skip all DB caches — re-run everything fresh
uv run python -m asset_discovery run --from-file boral.json --no-cache

# Verbose mode (tool calls, search queries, LLM interactions)
uv run python -m asset_discovery run --from-file boral.json --verbose

# Initialize Postgres cache tables
psql $CORPGRAPH_DB_URL -f scripts/init_cache_db.sql

# Run unit tests
uv run pytest tests/unit/

# Run a single test file
uv run pytest tests/unit/test_config.py

# Run integration tests (needs live Postgres)
uv run pytest tests/integration/ -m integration
```

## Architecture

### Pipeline Stages (`pipeline.py`)

```
Profile → Discover → Scrape → Extract → Merge → (Geocode) → QA
   ↓          ↓          ↓         ↓         ↓        ↓         ↓
context   discovered   scraped   extracted  merged  geo-       qa_report
 _doc      _urls       _pages    _assets   _assets  resolved   + gaps
```

1. **Profile** — Load company context from corp-graph Postgres or JSON file. Optional LLM enrichment via `corp_profile.enrich`. Produces `context_doc` (markdown) consumed by all downstream agents.
2. **Discover** — Supervisor-worker pattern: a pydantic-ai supervisor agent spawns up to `max_discover_workers` focused worker agents (e.g. subsidiary exploration, regulatory DB searches). Both supervisor and workers share tools (`fetch_sitemap`, `spider_links`, `crawl_page`, `map_domain`, `probe_urls`, `group_by_prefix`, `save_urls`, `save_sitemap_urls`, `remove_urls`, `get_saved_urls`) + pluggable web search (Exa/Brave/Tavily/OpenAI). Workers use `spawn_worker(task)` tool. URLs saved progressively to DB with structured scrape hints (strategy, proxy_mode, wait_for, js_code). Timeout + tool-call limits protect against runaway agents.
3. **Scrape** — Reads discovered URLs from DB, checks page cache for freshness (`page_stale_days`). Builds per-URL `ScrapeConfig` from agent's structured fields. Crawl4AI Cloud API via `web-scraper` package. Injects pages into RAG store if available.
4. **Extract** — instructor structured extraction via `doc-extractor`. Batches pages by token budget (`max_batch_tokens`). Cache keyed on `(page_id, model, content_hash)` — invalidates if page content changes. Deduplicates by coordinates (55m threshold).
5. **Merge** — Concurrent batch dedup (50 assets per batch with `asyncio.Lock`), then cross-batch LLM dedup for duplicates spanning batch boundaries, then existing asset matching from `discovered_assets` table. NatureSense classification maps raw types to predefined categories. Sets `asset_id`, `naturesense_asset_type`, `industry_code`.
6. **Geocode** (optional) — Uses `geo-resolve` package to geocode assets with valid addresses but missing lat/lon. Gracefully skips if not installed.
7. **QA** — pydantic-ai agent evaluates coverage, fills gaps via `rag_query` (cheapest — searches already-scraped pages) and `scrape_and_extract` (web search + scrape + extract new URLs). Outputs `QAReport` with quality score, summary, missing types/regions, and `CoverageFlag`s (with severity levels). High-severity flags propagate to asset `qa_flag` fields.

Each run saves intermediate files to `output/<company_slug>_<issuer_id>/<timestamp>/`. Final output: `final_assets.json`, `final_assets.csv` (TREX ALD format), `final_assets.xlsx` (with "Key" sheet for QA summary + "Assets" data sheet).

### Configuration (`config.py` + `config.toml`)

Triple-layer resolution: **env var > config.toml > hardcoded default**.

- Secrets (API keys, DB URL) live in `.env` only — see `.env.example`
- Models, caps, and sub-module settings live in `config.toml`
- `Config` dataclass is constructed once and threaded through all stages
- Sub-module configs are built on demand: `Config.scraper_config()`, `.extractor_config()`, `.rag_config()`, `.profile_enrich_config()`, `.profile_web_config()`, `.profile_research_config()`
- Discover stage uses separate models: `discover.supervisor` and `discover.worker` in config.toml

Model strings use litellm format (e.g. `bedrock/us.anthropic.claude-opus-4-6-v1`). For pydantic-ai agents, these are wrapped as `litellm:<model>` via `_to_pydantic_ai_model()`.

### Editable Local Dependencies

Five sibling repos are linked as editable deps via `[tool.uv.sources]` in pyproject.toml:

| Package | Path | Purpose |
|---|---|---|
| `corp-profile` | `../corp-profile` | Company profiling from corp-graph + LLM enrichment |
| `web-scraper` | `../web-scraper` | Crawl4AI Cloud API wrapper with batching + proxy |
| `doc-extractor` | `../doc-extractor` | LLM structured extraction via instructor |
| `rag` | `../rag` | pgvector ingest + Cohere rerank retrieval |
| `geo-resolve` | `../geo-resolve` | Geocoding (address → lat/lon) |

### Key Patterns

- **Idempotent caching**: All stages check Postgres cache before doing work. Pages stale after `page_stale_days`. Extraction cache keyed on `(page_id, model, content_hash)` — re-extracts if page content changes.
- **Cost tracking**: `CostTracker` (`cost.py`) tracks per-model tokens, per-stage tokens, and non-LLM API costs (Crawl4AI credits, Exa searches, embedding tokens, Cohere rerank calls). Use `track_pydantic_ai()` for agent results, `track_litellm()` for direct litellm calls.
- **Global tool state**: `stages/tools.py` uses module-level `_config`, `_issuer_id`, `_costs` initialized via `init_tools()`. Discover and QA agents share these tools.
- **Supervisor-worker discovery**: Supervisor delegates subtasks to worker agents via `spawn_worker(task)`. Workers run in parallel (up to `max_discover_workers`), each with the full tool set plus web search. Both save URLs independently to DB.
- **Lazy imports**: `pipeline.py` imports stage modules and display functions inside the stage blocks, not at the top. This keeps startup fast and avoids importing heavy deps for partial runs.
- **Postgres persistence**: 5 tables — `discovered_urls`, `scraped_pages`, `extraction_results`, `discovered_assets` (with PostGIS geometry), `qa_results`. Schema in `scripts/init_cache_db.sql`. All DB access via `psycopg` with raw SQL (no ORM).
- **Data files**: `models.py` loads `data/naturesense_asset_types.csv` and `data/gics_industries.csv` at import time into module-level lists (`NATURESENSE_DATA`, `GICS_INDUSTRIES`). These provide reference blocks for extraction prompts.

### Key Files

| File | Role |
|---|---|
| `src/asset_discovery/__main__.py` | CLI entry point |
| `src/asset_discovery/pipeline.py` | 6-stage orchestrator |
| `src/asset_discovery/config.py` | Master config with triple-layer resolution |
| `src/asset_discovery/models.py` | Pydantic models (`Asset`, `DiscoveredUrl`, `QAReport`, `CoverageFlag`) + GICS/NatureSense data loading |
| `src/asset_discovery/cost.py` | LLM + API cost tracking with per-model pricing |
| `src/asset_discovery/db.py` | Postgres helpers (cache reads/writes, hash functions) |
| `src/asset_discovery/helpers.py` | URL normalization, domain extraction, tracking param stripping |
| `src/asset_discovery/display.py` | Rich terminal display (stage progress, asset tables, cost summary) |
| `src/asset_discovery/stages/prompts.py` | System prompts for discover and QA agents |
| `src/asset_discovery/stages/tools.py` | Agent tools (sitemap, spider, crawl, map, probe, save/get URLs, spawn_worker) |
| `src/asset_discovery/stages/discover.py` | Stage 2: supervisor-worker URL discovery |
| `src/asset_discovery/stages/scrape.py` | Stage 3: Crawl4AI Cloud API via web-scraper |
| `src/asset_discovery/stages/extract.py` | Stage 4: instructor structured extraction |
| `src/asset_discovery/stages/merge.py` | Stage 5: concurrent batch dedup + classification |
| `src/asset_discovery/stages/qa.py` | Stage 6: pydantic-ai QA + gap-fill agent |

## Conventions

- Python 3.13+, async throughout (stages are `async def`)
- Pydantic v2 for all data models
- pydantic-ai for agent-based stages (discover, QA) — agents get tools + system prompt + usage limits
- instructor for structured extraction (extract stage)
- litellm as the unified LLM routing layer — also used directly for merge/dedup with `response_format={"type": "json_object"}`
- Rich for terminal display (`display.py`)
- All DB access via `psycopg` with raw SQL (no ORM), `dict_row` factory
- pytest with `asyncio_mode = "auto"` — no need for `@pytest.mark.asyncio` on async tests
- Unit tests mock DB via `mock_conn` fixture (`tests/unit/conftest.py`); integration tests use live Postgres with per-test cleanup via unique `test_issuer_id` (`tests/integration/conftest.py`)
- Integration tests marked with `@pytest.mark.integration`
