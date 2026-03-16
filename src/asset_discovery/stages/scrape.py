"""Stage 3: Scrape -- cache check -> web-scraper -> save to Postgres + RAG ingest."""

from __future__ import annotations

from typing import Any

from web_scraper import scrape_stream, ScrapeConfig, Usage as ScraperUsage

from ..config import Config
from ..cost import CostTracker
from ..db import get_connection, get_cached_page, save_scraped_page, url_hash
from ..display import show_detail, show_spinner, show_stage


def _config_from_url(url_row: dict[str, Any]) -> ScrapeConfig | None:
    """Build per-URL ScrapeConfig from discovered URL row.

    Spider's smart mode handles rendering detection, proxy rotation, and
    lazy loading automatically. The only per-URL override is automation_scripts
    for pages requiring specific interaction (e.g. clicking "Show all locations").
    """
    if url_row.get("automation_scripts"):
        return ScrapeConfig(automation_scripts=url_row["automation_scripts"])
    return None  # use global defaults


async def run_scrape(
    issuer_id: str, discovered_urls: list[dict[str, Any]], config: Config,
    rag_store=None, costs: CostTracker | None = None,
) -> list[dict[str, Any]]:
    """Scrape URLs, skip cached fresh pages. Returns list of page dicts.

    Uses scrape_stream() for per-page processing as pages arrive from Spider.
    """
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
            if cfg is not None:
                configs[url_row["url"]] = cfg

        if cached_pages:
            show_detail(f"{len(cached_pages)} of {len(discovered_urls)} pages loaded from cache")
        if to_scrape:
            show_detail(f"{len(to_scrape)} pages to scrape")

        all_pages: list[dict[str, Any]] = list(cached_pages)
        scraper_usage = ScraperUsage()

        # Create RAG usage tracker once (not per page)
        rag_usage = None
        if rag_store:
            from rag import Usage as RAGUsage
            rag_usage = RAGUsage()

        if to_scrape:
            scraped_count = 0
            async for page in scrape_stream(
                urls=[u["url"] for u in to_scrape],
                api_key=config.spider_api_key,
                configs=configs if configs else None,
                scraper_config=config.scraper_config(),
                usage=scraper_usage,
            ):
                scraped_count += 1
                if page.success and page.markdown:
                    pid, chash = save_scraped_page(
                        conn, issuer_id, page.url, page.markdown, page.raw_html,
                        page.signals, None, stale_days=config.page_stale_days,
                    )
                    page_dict = {
                        "page_id": pid, "url": page.url,
                        "markdown": page.markdown, "raw_html": page.raw_html,
                        "signals": page.signals, "content_hash": chash,
                    }
                    all_pages.append(page_dict)
                    show_detail(f"Scraped {scraped_count}/{len(to_scrape)}: {page.url[:60]}")

                    if rag_store and rag_usage is not None:
                        rag_doc = {
                            "id": pid,
                            "content": page.markdown,
                            "metadata": {"url": page.url},
                        }
                        await rag_store.ingest([rag_doc], namespace=issuer_id, usage=rag_usage)
                else:
                    show_detail(f"Failed {scraped_count}/{len(to_scrape)}: {page.url[:60]}")
            show_detail(f"Scraping complete: {len(all_pages) - len(cached_pages)} new pages")

        if costs and rag_usage and rag_usage.embedding_tokens:
            costs.track_embedding(rag_usage.embedding_tokens)

        if costs and scraper_usage.pages_scraped:
            costs.track_spider(scraper_usage.pages_scraped, cost_usd=scraper_usage.total_cost)

    finally:
        conn.close()

    return all_pages
