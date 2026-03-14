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
```

Each run saves intermediate outputs to `output/<issuer_id>/<timestamp>/` — profile JSON, discovered URLs CSV, scraped page markdown/HTML, extracted assets, final merged assets, and the QA report.

## Architecture

### Pipeline stages

| # | Stage | What it does |
|---|---|---|
| 1 | **Profile** | Load company context from corp-graph Postgres or JSON file. Optional LLM enrichment. |
| 2 | **Discover** | pydantic-ai agent with tools (`fetch_sitemap`, `crawl_page`, `map_domain`, `mark_url_found`) + pluggable web search (Exa/Brave/Tavily/OpenAI). Finds and classifies asset-related URLs. |
| 3 | **Scrape** | Crawl4AI Cloud API via `web-scraper` package. Caches pages in Postgres with staleness tracking. Parses scrape hints from discover agent notes (WAF detection, AJAX waits). |
| 4 | **Extract** | instructor structured extraction via `doc-extractor` package. Batches pages by token budget, deduplicates by (name, entity). |
| 5 | **Merge** | LLM dedup against existing ALD assets + cross-batch dedup. Naturesense classification maps raw types to 16 predefined categories. |
| 6 | **QA** | pydantic-ai agent evaluates coverage, fills gaps via RAG queries and `scrape_and_extract` tool. Iterates up to `max_qa_iterations`. |

### Key patterns

- **Idempotent caching** — All stages check Postgres cache before doing work. Pages stale after `page_stale_days`. Extraction cache keyed on `(page_id, model, content_hash)`.
- **Cost tracking** — `CostTracker` tracks per-model tokens, per-stage tokens, and non-LLM API costs (Crawl4AI, Exa, embeddings, Cohere rerank). Summary printed at end of each run.
- **Agent tools** — Discover and QA agents compose domain-specific tools from `stages/tools.py` with pluggable web search tools.
- **Postgres persistence** — 5 tables: `discovered_urls`, `scraped_pages`, `extraction_results`, `discovered_assets` (with PostGIS geometry), `qa_results`. Schema in `scripts/init_cache_db.sql`.

## Configuration

Triple-layer resolution: **env var > `config.toml` > hardcoded default**.

- Secrets (API keys, DB URL) live in `.env` only
- Models, caps, and sub-module settings live in `config.toml`
- `Config` dataclass is constructed once and threaded through all stages
- Sub-module configs are built via `Config.scraper_config()`, `.extractor_config()`, `.rag_config()`, `.profile_enrich_config()`

Model strings use [litellm](https://docs.litellm.ai/) format (e.g. `bedrock/us.anthropic.claude-opus-4-6-v1`). For pydantic-ai agents, these are wrapped as `litellm:<model>` via `_to_pydantic_ai_model()`.

See `config.toml` for the full list of tunables with comments.

## Sub-modules

Four sibling repos are linked as editable deps via `[tool.uv.sources]` in `pyproject.toml`:

| Package | Path | Purpose |
|---|---|---|
| **corp-profile** | `../corp-profile` | Company profiling from corp-graph + LLM enrichment |
| **web-scraper** | `../web-scraper` | Crawl4AI Cloud API wrapper with batching + proxy support |
| **doc-extractor** | `../doc-extractor` | LLM structured extraction via instructor |
| **rag** | `../rag` | pgvector ingest + Cohere rerank retrieval |

## Key files

| File | Role |
|---|---|
| `src/asset_discovery/__main__.py` | CLI entry point |
| `src/asset_discovery/pipeline.py` | 6-stage orchestrator |
| `src/asset_discovery/config.py` | Master config with triple-layer resolution |
| `src/asset_discovery/models.py` | Pydantic models (`Asset`, `QAReport`, `CoverageFlag`) |
| `src/asset_discovery/cost.py` | LLM + API cost tracking |
| `src/asset_discovery/db.py` | Postgres helper functions |
| `src/asset_discovery/stages/discover.py` | Stage 2: URL discovery agent |
| `src/asset_discovery/stages/scrape.py` | Stage 3: Web scraping |
| `src/asset_discovery/stages/extract.py` | Stage 4: Structured extraction |
| `src/asset_discovery/stages/merge.py` | Stage 5: Dedup + classification |
| `src/asset_discovery/stages/qa.py` | Stage 6: QA + gap-fill agent |
| `src/asset_discovery/stages/prompts.py` | System prompts for all LLM agents |
| `src/asset_discovery/stages/tools.py` | Agent tools (sitemap, crawl, map, mark_url) |
| `src/asset_discovery/display.py` | Rich terminal display |
| `config.toml` | Runtime configuration |
| `scripts/init_cache_db.sql` | Postgres schema |
