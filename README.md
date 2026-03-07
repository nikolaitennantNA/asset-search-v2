# asset-search-v2

Web-based physical asset discovery pipeline for corporate entities.

## Setup

```bash
uv sync
cp .env.example .env  # fill in API keys
```

## Usage

```bash
# Run full pipeline for a single ISIN
python -m asset_search run <ISIN>

# Batch mode (stub)
python -m asset_search run --portfolio --max-companies 5
```

## Architecture

7-stage pipeline:

1. **Profile** -- company profiling via corp-profile
2. **Collect** -- enrichment data via corp-enrich
3. **Discover** -- pydantic-ai agent finds + classifies URLs
4. **Scrape** -- Crawl4AI Cloud API
5. **Extract + Ingest** -- instructor structured extraction + pgvector
6. **Validate** -- dedup + enrich + clean
7. **QA** -- coverage evaluation + deep search
