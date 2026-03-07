"""Pipeline orchestrator -- runs the 7-stage asset discovery pipeline."""

from __future__ import annotations

from .config import Config
from .display import show_stage


async def run(isin: str, config: Config | None = None) -> None:
    """Run the full asset discovery pipeline.

    Stages:
        1. Profile  -- company profiling via corp-profile
        2. Collect  -- enrichment data via corp-enrich
        3. Discover -- pydantic-ai agent finds + classifies URLs
        4. Scrape   -- Crawl4AI Cloud API
        5. Extract + Ingest -- instructor structured extraction + pgvector
        6. Validate -- dedup + enrich + clean
        7. QA       -- coverage evaluation + deep search
    """
    cfg = config or Config()

    show_stage(1, "Profiling company...")
    # Stage 1: Profile (calls corp-profile)

    show_stage(2, "Collecting enrichment data...")
    # Stage 2: Collect (calls corp-enrich)

    show_stage(3, "Discovering asset URLs...")
    # Stage 3: Discover (pydantic-ai agent)

    show_stage(4, "Scraping pages...")
    # Stage 4: Scrape (Crawl4AI Cloud API)

    show_stage(5, "Extracting assets...")
    # Stage 5: Extract + Ingest (parallel)

    show_stage(6, "Validating results...")
    # Stage 6: Validate (dedup + enrich + clean)

    show_stage(7, "Running QA checks...")
    # Stage 7: QA (coverage evaluation + deep search)

    raise NotImplementedError("Pipeline stages not yet implemented")
