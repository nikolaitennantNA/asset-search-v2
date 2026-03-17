"""Stage 3: Scrape -- cache check -> web-scraper -> save to Postgres + RAG ingest."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any


from rich.text import Text
from web_scraper import scrape_stream, ScrapeConfig, Usage as ScraperUsage

from ..config import Config
from ..cost import CostTracker
from ..db import get_connection, get_cached_page, save_scraped_page, url_hash
from ..display import console, show_detail, show_warning, stage_progress


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
    rag_store=None, costs: CostTracker | None = None, no_cache: bool = False,
) -> list[dict[str, Any]]:
    """Scrape URLs, skip cached fresh pages. Returns list of page dicts.

    Uses scrape_stream() for per-page processing as pages arrive from Spider.
    """
    from rich.panel import Panel
    from rich.padding import Padding
    import logging

    # Style web-scraper log messages with indentation
    from rich.logging import RichHandler
    scraper_log = logging.getLogger("web_scraper.scraper")
    scraper_log.setLevel(logging.WARNING)
    if not any(isinstance(h, RichHandler) for h in scraper_log.handlers):
        handler = RichHandler(
            console=console, show_path=False, show_time=False,
            show_level=False, markup=True,
        )
        handler.setLevel(logging.WARNING)
        scraper_log.addHandler(handler)
        scraper_log.propagate = False

    start = time.monotonic()

    conn = get_connection(config)
    try:
        to_scrape: list[dict[str, Any]] = []
        cached_pages: list[dict[str, Any]] = []

        if no_cache:
            to_scrape = list(discovered_urls)
        else:
            for url_row in discovered_urls:
                cached = get_cached_page(conn, url_row["url"])
                if cached:
                    cached_pages.append(cached)
                else:
                    to_scrape.append(url_row)

        # Panel header
        header = Text()
        header.append("[3/6]", style="bold cyan")
        header.append(" Scraping pages", style="bold")
        header.append("  ·  ", style="dim")
        header.append(f"{len(discovered_urls)} urls")
        if cached_pages:
            header.append(f" ({len(cached_pages)} cached)", style="dim")
        console.print(Panel(header, border_style="dim", padding=(0, 1)))

        configs: dict[str, ScrapeConfig] = {}
        for url_row in to_scrape:
            cfg = _config_from_url(url_row)
            if cfg is not None:
                configs[url_row["url"]] = cfg

        all_pages: list[dict[str, Any]] = list(cached_pages)
        scraper_usage = ScraperUsage()

        # Create RAG usage tracker once (not per page)
        rag_usage = None
        if rag_store:
            from rag import Usage as RAGUsage
            rag_usage = RAGUsage()

        succeeded = 0
        failed = 0

        if to_scrape:
            # Dedup URLs
            seen_urls: set[str] = set()
            deduped: list[dict[str, Any]] = []
            for u in to_scrape:
                if u["url"] not in seen_urls:
                    seen_urls.add(u["url"])
                    deduped.append(u)
            if len(deduped) < len(to_scrape):
                to_scrape = deduped

            total = len(to_scrape)
            stall_timeout = 30
            stream = scrape_stream(
                urls=[u["url"] for u in to_scrape],
                api_key=config.spider_api_key,
                configs=configs if configs else None,
                scraper_config=config.scraper_config(),
                usage=scraper_usage,
            )

            stall_strikes = 0
            max_stall_strikes = 3  # give up after 3 consecutive stalls

            with stage_progress(total, "Scraping", "pages") as (progress, task):
                try:
                    while True:
                        try:
                            page = await asyncio.wait_for(
                                stream.__anext__(), timeout=stall_timeout,
                            )
                        except StopAsyncIteration:
                            break
                        except (TimeoutError, asyncio.TimeoutError):
                            stall_strikes += 1
                            failed += 1
                            progress.advance(task)
                            if stall_strikes >= max_stall_strikes:
                                remaining = total - (succeeded + failed)
                                if remaining > 0:
                                    failed += remaining
                                show_warning(
                                    f"Stalled {max_stall_strikes}x — "
                                    f"skipping {remaining} remaining pages"
                                )
                                break
                            continue

                        stall_strikes = 0  # reset on any page received
                        if page.success and page.markdown:
                            succeeded += 1
                            pid, chash = save_scraped_page(
                                conn, issuer_id, page.url, page.markdown,
                                page.raw_html, page.signals, None,
                                stale_days=config.page_stale_days,
                            )
                            all_pages.append({
                                "page_id": pid, "url": page.url,
                                "markdown": page.markdown,
                                "raw_html": page.raw_html,
                                "signals": page.signals,
                                "content_hash": chash,
                            })
                        else:
                            failed += 1

                        progress.advance(task)
                except Exception as e:
                    show_warning(f"Stream error: {e}")

        if costs and scraper_usage.pages_scraped:
            costs.track_spider(scraper_usage.pages_scraped, cost_usd=scraper_usage.total_cost)

    finally:
        conn.close()

    # RAG ingestion — batch after scraping completes (faster than inline)
    if rag_store:
        new_pages = [p for p in all_pages if p.get("markdown") and p not in cached_pages]
        if new_pages:
            show_detail(f"Ingesting {len(new_pages)} pages into RAG...")
            rag_usage = None
            if rag_store:
                from rag import Usage as RAGUsage
                rag_usage = RAGUsage()
            docs = [
                {"id": p.get("page_id", ""), "content": p["markdown"],
                 "metadata": {"url": p["url"]}}
                for p in new_pages
            ]
            from ..display import show_spinner
            try:
                with show_spinner(f"Ingesting {len(new_pages)} pages into RAG..."):
                    await rag_store.ingest(docs, namespace=issuer_id, usage=rag_usage)
                if costs and rag_usage and rag_usage.embedding_tokens:
                    costs.track_embedding(rag_usage.embedding_tokens)
            except Exception as e:
                show_warning(f"RAG ingestion failed: {e}")

    # Footer
    console.print()
    elapsed = time.monotonic() - start
    mins, secs = divmod(int(elapsed), 60)
    time_str = f"{mins}m {secs:02d}s" if mins else f"{secs}s"
    total_new = succeeded
    total_all = len(all_pages)
    pct = (succeeded / (succeeded + failed) * 100) if (succeeded + failed) else 100

    footer = Text("  Done", style="bold green")
    footer.append("  ·  ", style="dim")
    if cached_pages:
        footer.append(f"{len(cached_pages)} cached + {total_new} scraped", style="bold")
    else:
        footer.append(f"{total_new} scraped", style="bold")
    footer.append(f" ({pct:.0f}%)", style="bold green" if pct >= 95 else "bold yellow")
    if failed:
        footer.append("  ·  ", style="dim")
        footer.append(f"{failed} failed", style="bold red")
    footer.append("  ·  ", style="dim")
    footer.append(time_str, style="bold")
    console.print(footer)
    console.print()

    return all_pages
