"""Tests for stages/scrape.py — config parsing and scrape orchestration."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from asset_search.stages.scrape import _config_from_notes, run_scrape
from asset_search.cost import CostTracker
from web_scraper import ScrapedPage, ScrapeConfig, Usage


# ── _config_from_notes ──────────────────────────────────────────────────────


def test_config_from_notes_empty():
    assert _config_from_notes(None) == ScrapeConfig()
    assert _config_from_notes("") == ScrapeConfig()


def test_config_from_notes_waf():
    cfg = _config_from_notes("waf_blocked")
    assert cfg.proxy_mode == "auto"


def test_config_from_notes_wait_for():
    cfg = _config_from_notes("wait_for:.locations-list")
    assert cfg.wait_for == ".locations-list"
    assert cfg.strategy == "browser"


def test_config_from_notes_combined():
    cfg = _config_from_notes("waf_blocked wait_for:.results")
    assert cfg.proxy_mode == "auto"
    assert cfg.wait_for == ".results"
    assert cfg.strategy == "browser"


def test_config_from_notes_ajax():
    cfg = _config_from_notes("ajax store_locator")
    assert cfg.strategy == "browser"


def test_config_from_notes_lazy_load():
    cfg = _config_from_notes("lazy_load")
    assert cfg.scan_full_page is True
    assert cfg.strategy == "browser"


def test_config_from_notes_screenshot():
    cfg = _config_from_notes("screenshot")
    assert cfg.screenshot is True


def test_config_from_notes_js_code():
    cfg = _config_from_notes("js_code:document.querySelector('.btn').click()")
    assert cfg.js_code == "document.querySelector('.btn').click()"
    assert cfg.strategy == "browser"


def test_config_from_notes_pdf():
    """PDF notes should not change strategy — HTTP is fine for PDFs."""
    cfg = _config_from_notes("pdf")
    assert cfg.strategy is None


# ── run_scrape ──────────────────────────────────────────────────────────────


def _make_page(url, md="# Page", html="<h1>Page</h1>"):
    return ScrapedPage(url=url, markdown=md, raw_html=html, success=True, status_code=200)


def _make_failed_page(url):
    return ScrapedPage(url=url, markdown="", raw_html="", success=False, status_code=0)


@pytest.mark.asyncio
@patch("asset_search.stages.scrape.get_connection")
@patch("asset_search.stages.scrape.scrape", new_callable=AsyncMock)
@patch("asset_search.stages.scrape.get_cached_page")
@patch("asset_search.stages.scrape.save_scraped_page")
@patch("asset_search.stages.scrape.show_stage")
async def test_run_scrape_cache_hit(mock_show, mock_save, mock_get_cached, mock_scrape, mock_get_conn):
    mock_get_conn.return_value = MagicMock()
    mock_get_cached.return_value = {"url": "https://a.com", "markdown": "# Cached", "page_id": "p1"}
    urls = [{"url": "https://a.com", "category": "facility_page"}]

    from asset_search.config import Config
    pages = await run_scrape("issuer-1", urls, Config())

    assert len(pages) == 1
    assert pages[0]["markdown"] == "# Cached"
    mock_scrape.assert_not_called()


@pytest.mark.asyncio
@patch("asset_search.stages.scrape.get_connection")
@patch("asset_search.stages.scrape.scrape", new_callable=AsyncMock)
@patch("asset_search.stages.scrape.get_cached_page", return_value=None)
@patch("asset_search.stages.scrape.save_scraped_page", return_value=("page-id", "content-hash"))
@patch("asset_search.stages.scrape.show_stage")
async def test_run_scrape_cache_miss(mock_show, mock_save, mock_get_cached, mock_scrape, mock_get_conn):
    mock_get_conn.return_value = MagicMock()
    mock_scrape.return_value = [_make_page("https://a.com")]
    urls = [{"url": "https://a.com", "category": "facility_page"}]

    from asset_search.config import Config
    pages = await run_scrape("issuer-1", urls, Config())

    assert len(pages) == 1
    mock_scrape.assert_called_once()
    mock_save.assert_called_once()


@pytest.mark.asyncio
@patch("asset_search.stages.scrape.get_connection")
@patch("asset_search.stages.scrape.scrape", new_callable=AsyncMock)
@patch("asset_search.stages.scrape.get_cached_page", return_value=None)
@patch("asset_search.stages.scrape.save_scraped_page", return_value=("p1", "ch1"))
@patch("asset_search.stages.scrape.show_stage")
async def test_run_scrape_cost_tracking(mock_show, mock_save, mock_get_cached, mock_scrape, mock_get_conn):
    mock_get_conn.return_value = MagicMock()

    # The scrape function creates its own ScraperUsage() internally and passes it
    # to web_scraper.scrape(). We need the mock to update that usage object.
    async def scrape_side_effect(*args, **kwargs):
        usage = kwargs.get("usage")
        if usage:
            usage.pages_crawled = 2
        return [_make_page("https://a.com"), _make_page("https://b.com")]

    mock_scrape.side_effect = scrape_side_effect
    urls = [
        {"url": "https://a.com", "category": "facility_page"},
        {"url": "https://b.com", "category": "facility_page"},
    ]
    costs = CostTracker()
    from asset_search.config import Config
    await run_scrape("issuer-1", urls, Config(), costs=costs)
    assert costs.crawl4ai_pages == 2


@pytest.mark.asyncio
@patch("asset_search.stages.scrape.get_connection")
@patch("asset_search.stages.scrape.scrape", new_callable=AsyncMock)
@patch("asset_search.stages.scrape.get_cached_page", return_value=None)
@patch("asset_search.stages.scrape.save_scraped_page")
@patch("asset_search.stages.scrape.show_stage")
async def test_run_scrape_failed_page_not_saved(mock_show, mock_save, mock_get_cached, mock_scrape, mock_get_conn):
    mock_get_conn.return_value = MagicMock()
    mock_scrape.return_value = [_make_failed_page("https://a.com")]
    urls = [{"url": "https://a.com", "category": "facility_page"}]

    from asset_search.config import Config
    pages = await run_scrape("issuer-1", urls, Config())

    assert len(pages) == 0  # failed pages not added
    mock_save.assert_not_called()


@pytest.mark.asyncio
@patch("asset_search.stages.scrape.get_connection")
@patch("asset_search.stages.scrape.scrape", new_callable=AsyncMock)
@patch("asset_search.stages.scrape.get_cached_page")
@patch("asset_search.stages.scrape.save_scraped_page", return_value=("p1", "ch1"))
@patch("asset_search.stages.scrape.show_stage")
async def test_run_scrape_mixed(mock_show, mock_save, mock_get_cached, mock_scrape, mock_get_conn):
    """Mix of 2 cached + 2 uncached URLs: all 4 in results, scraper called with only 2, 2 saves."""
    mock_get_conn.return_value = MagicMock()

    cached_urls = {"https://cached-a.com", "https://cached-b.com"}

    def get_cached_side_effect(conn, url):
        if url in cached_urls:
            return {"url": url, "markdown": "# Cached", "page_id": f"c-{url}"}
        return None

    mock_get_cached.side_effect = get_cached_side_effect
    mock_scrape.return_value = [
        _make_page("https://fresh-a.com"),
        _make_page("https://fresh-b.com"),
    ]

    urls = [
        {"url": "https://cached-a.com", "category": "facility_page"},
        {"url": "https://cached-b.com", "category": "facility_page"},
        {"url": "https://fresh-a.com", "category": "facility_page"},
        {"url": "https://fresh-b.com", "category": "facility_page"},
    ]

    from asset_search.config import Config
    pages = await run_scrape("issuer-1", urls, Config())

    assert len(pages) == 4
    mock_scrape.assert_called_once()
    scrape_urls = mock_scrape.call_args[1].get("urls") or mock_scrape.call_args[0][0]
    assert set(scrape_urls) == {"https://fresh-a.com", "https://fresh-b.com"}
    assert mock_save.call_count == 2
