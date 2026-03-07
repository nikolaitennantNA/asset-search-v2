"""Stage 4: Scrape -- Crawl4AI Cloud API batch scraping.

Takes classified URLs from the discover stage and scrapes them via
Crawl4AI Cloud API with concurrency control. Caches results in Postgres.
"""

from __future__ import annotations


async def run_scrape(
    urls: list[dict],
    config: object,
) -> list[dict]:
    """Scrape classified URLs via Crawl4AI Cloud API.

    Returns a list of scraped page dicts with markdown content, HTML,
    and metadata.
    """
    raise NotImplementedError("Stage 4 (Scrape) not yet implemented")
