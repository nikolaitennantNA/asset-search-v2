# asset-discovery

A 6-stage async pipeline that discovers physical assets (facilities, plants, mines, warehouses, offices) for corporate entities using LLM agents, web scraping, and structured extraction. Output is TREX ALD-aligned asset records persisted to Postgres.

## Setup

Requires Python 3.13+ and [uv](https://docs.astral.sh/uv/).

```bash
# Install (editable local deps require sibling repos — see Sub-modules below)
uv sync

# Copy and fill in API keys
cp .env.example .env
```

### Postgres

The pipeline caches discovered URLs, scraped pages, extraction results, and final assets in Postgres (with PostGIS + pgvector extensions).

```bash
createdb asset_discovery
psql asset_discovery -f scripts/init_cache_db.sql
```

Then set `CORPGRAPH_DB_URL` in `.env`.

### Environment variables

Secrets live in `.env` only — everything else is configured via `config.toml`.

| Variable | Purpose |
|---|---|
| `CORPGRAPH_DB_URL` | Postgres connection string |
| `CRAWL4AI_API_KEY` | Crawl4AI Cloud API |
| `OPENAI_API_KEY` | OpenAI models + embeddings |
| `COHERE_API_KEY` | Cohere rerank |
| `EXA_API_KEY` | Exa web search |
| `FIRECRAWL_API_KEY` | Firecrawl (optional) |

## Usage

```bash
# Run full pipeline for a single ISIN (requires corp-graph Postgres)
uv run python -m asset_discovery run AU000000BLD2

# Run from a JSON profile (no corp-graph needed)
uv run python -m asset_discovery run --from-file boral.json

# Stop after a specific stage
uv run python -m asset_discovery run --from-file boral.json --stop-after discover

# Resume from a specific stage (loads prior results from DB/cache)
uv run python -m asset_discovery run AU000000BLD2 --start-from extract

# Skip all DB caches — re-run everything fresh
uv run python -m asset_discovery run --from-file boral.json --no-cache

# Verbose mode (tool calls, search queries, LLM interactions)
uv run python -m asset_discovery run --from-file boral.json --verbose
```

Each run saves outputs to `output/<company_slug>_<issuer_id>/<timestamp>/` — profile JSON, discovered URLs CSV, scraped page markdown/HTML, extracted assets, and final results as JSON, CSV (TREX ALD format), and XLSX (with QA summary sheet).

## Architecture

### Pipeline stages

| # | Stage | What it does |
|---|---|---|
| 1 | **Profile** | Load company context from corp-graph Postgres or JSON file. Optional LLM enrichment. |
| 2 | **Discover** | Supervisor-worker pattern: a pydantic-ai supervisor agent spawns focused worker agents (up to `max_discover_workers`) for parallel URL discovery. Tools include `fetch_sitemap`, `spider_links`, `crawl_page`, `map_domain`, `probe_urls`, `group_by_prefix`, `save_urls`, `save_sitemap_urls`, `remove_urls` + pluggable web search (Exa/Brave/Tavily/OpenAI). |
| 3 | **Scrape** | Crawl4AI Cloud API via `web-scraper` package. Caches pages in Postgres with staleness tracking. Parses scrape hints from discover agent (WAF detection, AJAX waits, proxy mode). |
| 4 | **Extract** | instructor structured extraction via `doc-extractor` package. Batches pages by token budget, deduplicates by coordinates. |
| 5 | **Merge** | Concurrent batch LLM dedup (50 per batch) + cross-batch dedup + existing asset matching. NatureSense classification maps raw types to predefined categories. |
| 6 | **Geocode** | Optional address → lat/lon via `geo-resolve`. Runs only for assets with valid addresses but missing coordinates. |
| 7 | **QA** | pydantic-ai agent evaluates coverage, fills gaps via RAG queries and `scrape_and_extract` tool. Outputs `QAReport` with quality score, summary, and coverage flags (high-severity flags propagate to assets). |

### Key patterns

- **Idempotent caching** — All stages check Postgres cache before doing work. Pages stale after `page_stale_days`. Extraction cache keyed on `(page_id, model, content_hash)`.
- **Cost tracking** — `CostTracker` tracks per-model tokens, per-stage tokens, and non-LLM API costs (Crawl4AI, Exa, embeddings, Cohere rerank). Summary printed at end of each run.
- **Supervisor-worker discovery** — Supervisor delegates subtasks (subsidiary exploration, regulatory DB searches) to worker agents that run in parallel, each with full tool access + web search.
- **Postgres persistence** — 5 tables: `discovered_urls`, `scraped_pages`, `extraction_results`, `discovered_assets` (with PostGIS geometry), `qa_results`. Schema in `scripts/init_cache_db.sql`.

## Configuration

Triple-layer resolution: **env var > `config.toml` > hardcoded default**.

- Secrets (API keys, DB URL) live in `.env` only
- Models, caps, and sub-module settings live in `config.toml`
- `Config` dataclass is constructed once and threaded through all stages
- Discover stage uses separate models for supervisor and workers (`discover.supervisor`, `discover.worker` in config.toml)
- Sub-module configs are built via `Config.scraper_config()`, `.extractor_config()`, `.rag_config()`, `.profile_enrich_config()`

Model strings use [litellm](https://docs.litellm.ai/) format (e.g. `bedrock/us.anthropic.claude-opus-4-6-v1`). For pydantic-ai agents, these are wrapped as `litellm:<model>` via `_to_pydantic_ai_model()`.

See `config.toml` for the full list of tunables with comments.

## Sub-modules

Five sibling repos are linked as editable deps via `[tool.uv.sources]` in `pyproject.toml`:

| Package | Path | Purpose |
|---|---|---|
| **corp-profile** | `../corp-profile` | Company profiling from corp-graph + LLM enrichment |
| **web-scraper** | `../web-scraper` | Crawl4AI Cloud API wrapper with batching + proxy support |
| **doc-extractor** | `../doc-extractor` | LLM structured extraction via instructor |
| **rag** | `../rag` | pgvector ingest + Cohere rerank retrieval |
| **geo-resolve** | `../geo-resolve` | Geocoding (address → lat/lon) |

## Testing

```bash
# Unit tests (mock DB via mock_conn fixture)
uv run pytest tests/unit/

# Single test file
uv run pytest tests/unit/test_config.py

# Integration tests (needs live Postgres, per-test cleanup)
uv run pytest tests/integration/ -m integration
```

## Key files

| File | Role |
|---|---|
| `src/asset_discovery/__main__.py` | CLI entry point |
| `src/asset_discovery/pipeline.py` | Stage orchestrator |
| `src/asset_discovery/config.py` | Master config with triple-layer resolution |
| `src/asset_discovery/models.py` | Pydantic models + GICS/NatureSense data loading |
| `src/asset_discovery/cost.py` | LLM + API cost tracking |
| `src/asset_discovery/db.py` | Postgres helper functions |
| `src/asset_discovery/stages/discover.py` | Supervisor-worker URL discovery |
| `src/asset_discovery/stages/scrape.py` | Web scraping via Crawl4AI |
| `src/asset_discovery/stages/extract.py` | Structured extraction |
| `src/asset_discovery/stages/merge.py` | Concurrent batch dedup + classification |
| `src/asset_discovery/stages/qa.py` | QA + gap-fill agent |
| `src/asset_discovery/stages/tools.py` | Agent tools (sitemap, spider, crawl, save URLs, spawn_worker) |
| `src/asset_discovery/stages/prompts.py` | System prompts for LLM agents |
| `src/asset_discovery/display.py` | Rich terminal display |
| `config.toml` | Runtime configuration |
| `scripts/init_cache_db.sql` | Postgres schema |
