"""Stage 3: Scrape — cache check → web-scraper → save to Postgres + RAG ingest."""

from __future__ import annotations

from typing import Any

from web_scraper import scrape, ScrapeConfig, Usage as ScraperUsage

from ..config import Config
from ..cost import CostTracker
from ..db import get_connection, get_cached_page, save_scraped_page, url_hash
from ..display import show_stage


# Fields that map directly from discovered_urls row to ScrapeConfig
_SCRAPE_CONFIG_FIELDS = ("strategy", "proxy_mode", "wait_for", "js_code", "scan_full_page", "screenshot")


def _config_from_url(url_row: dict[str, Any]) -> ScrapeConfig:
    """Build per-URL ScrapeConfig from structured fields set by the discover agent.

    Reads typed fields (strategy, proxy_mode, wait_for, etc.) directly from the
    discovered URL row. None/missing values are skipped, falling back to defaults.
    """
    kwargs: dict = {}
    for field in _SCRAPE_CONFIG_FIELDS:
        val = url_row.get(field)
        if val is not None and val != "" and val is not False:
            kwargs[field] = val
    # scan_full_page and screenshot are booleans — only set if True
    for bool_field in ("scan_full_page", "screenshot"):
        if url_row.get(bool_field) is True:
            kwargs[bool_field] = True
    return ScrapeConfig(**kwargs) if kwargs else ScrapeConfig()


async def run_scrape(
    issuer_id: str, discovered_urls: list[dict[str, Any]], config: Config,
    rag_store=None, costs: CostTracker | None = None,
) -> list[dict[str, Any]]:
    """Scrape URLs, skip cached fresh pages. Returns list of page dicts."""
    show_stage(3, "Scraping pages")

    conn = get_connection(config)
    try:
        to_scrape: list[dict[str, Any]] = []
        cached_pages: list[dict[str, Any]] = []

        for url_row in discovered_urls:
            cached = get_cached_page(conn, url_row["url"])
            if cached:
                cached_pages.append(cached)
            else:
                to_scrape.append(url_row)

        configs: dict[str, ScrapeConfig] = {}
        for url_row in to_scrape:
            cfg = _config_from_url(url_row)
            if cfg != ScrapeConfig():
                configs[url_row["url"]] = cfg

        scraped = []
        scraper_usage = ScraperUsage()
        if to_scrape:
            scraped = await scrape(
                urls=[u["url"] for u in to_scrape],
                api_key=config.crawl4ai_api_key,
                configs=configs,
                max_concurrency=config.max_scrape_concurrency,
                scraper_config=config.scraper_config(),
                usage=scraper_usage,
            )

        all_pages: list[dict[str, Any]] = list(cached_pages)
        for page in scraped:
            if page.success and page.markdown:
                pid, chash = save_scraped_page(
                    conn, issuer_id, page.url, page.markdown, page.raw_html,
                    page.signals, None, stale_days=config.page_stale_days,
                )
                all_pages.append({
                    "page_id": pid, "url": page.url,
                    "markdown": page.markdown, "raw_html": page.raw_html,
                    "signals": page.signals, "content_hash": chash,
                })

        if costs and scraper_usage.pages_crawled:
            costs.track_crawl4ai(scraper_usage.pages_crawled, credits_used=scraper_usage.credits_used)

        if rag_store and all_pages:
            from rag import Usage as RAGUsage
            rag_usage = RAGUsage()
            rag_docs = [
                {"id": p.get("page_id", url_hash(p["url"])), "content": p["markdown"],
                 "metadata": {"url": p["url"]}}
                for p in all_pages if p.get("markdown")
            ]
            await rag_store.ingest(rag_docs, namespace=issuer_id, usage=rag_usage)
            if costs:
                costs.track_embedding(rag_usage.embedding_tokens)

    finally:
        conn.close()

    return all_pages
