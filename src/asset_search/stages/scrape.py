"""Stage 3: Scrape — cache check → web-scraper → save to Postgres + RAG ingest."""

from __future__ import annotations

from typing import Any

from web_scraper import scrape, ScrapeConfig

from ..config import Config
from ..db import get_connection, get_cached_page, save_scraped_page, url_hash
from ..display import show_stage


def _config_from_notes(notes: str | None) -> ScrapeConfig:
    if not notes:
        return ScrapeConfig()
    notes_lower = notes.lower()
    return ScrapeConfig(
        scan_full_page="store_locator" in notes_lower or "scroll" in notes_lower,
        proxy="residential" if "waf_blocked" in notes_lower else "datacenter",
    )


async def run_scrape(
    issuer_id: str, discovered_urls: list[dict[str, Any]], config: Config,
    rag_store=None,
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
            cfg = _config_from_notes(url_row.get("notes"))
            if cfg != ScrapeConfig():
                configs[url_row["url"]] = cfg

        scraped = []
        if to_scrape:
            scraped = await scrape(
                urls=[u["url"] for u in to_scrape],
                api_key=config.crawl4ai_api_key,
                configs=configs,
                max_concurrency=config.max_scrape_concurrency,
            )

        all_pages: list[dict[str, Any]] = list(cached_pages)
        for page in scraped:
            if page.success and page.markdown:
                pid = save_scraped_page(
                    conn, issuer_id, page.url, page.markdown, page.raw_html,
                    page.signals, None, stale_days=config.page_stale_days,
                )
                all_pages.append({
                    "page_id": pid, "url": page.url,
                    "markdown": page.markdown, "signals": page.signals,
                })

        if rag_store and all_pages:
            rag_docs = [
                {"id": p.get("page_id", url_hash(p["url"])), "content": p["markdown"],
                 "metadata": {"url": p["url"]}}
                for p in all_pages if p.get("markdown")
            ]
            await rag_store.ingest(rag_docs, namespace=issuer_id)

    finally:
        conn.close()

    return all_pages
