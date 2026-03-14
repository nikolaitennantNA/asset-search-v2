"""Integration tests for scrape stage — real Postgres + mocked scraper.

Run: pytest -m integration tests/integration/test_scrape_live.py
"""

from unittest.mock import AsyncMock, patch

import pytest

from asset_discovery.cost import CostTracker
from asset_discovery.db import get_cached_page
from asset_discovery.stages.scrape import run_scrape
from web_scraper import ScrapedPage

pytestmark = pytest.mark.integration


def _make_page(url):
    return ScrapedPage(
        url=url, markdown="# Test Page", raw_html="<h1>Test Page</h1>",
        success=True, status_code=200,
    )


@patch("asset_discovery.stages.scrape.scrape", new_callable=AsyncMock)
async def test_run_scrape_end_to_end(mock_scrape, config, db_conn, test_issuer_id):
    mock_scrape.return_value = [_make_page("https://test-e2e.com")]
    urls = [{"url": "https://test-e2e.com", "category": "facility_page"}]

    pages = await run_scrape(test_issuer_id, urls, config)

    assert len(pages) == 1
    assert pages[0]["markdown"] == "# Test Page"
    # Verify it was saved to DB
    cached = get_cached_page(db_conn, "https://test-e2e.com")
    assert cached is not None


@patch("asset_discovery.stages.scrape.scrape", new_callable=AsyncMock)
async def test_run_scrape_cache_cycle(mock_scrape, config, db_conn, test_issuer_id):
    mock_scrape.return_value = [_make_page("https://test-cache.com")]
    urls = [{"url": "https://test-cache.com", "category": "facility_page"}]

    # First call — should scrape
    await run_scrape(test_issuer_id, urls, config)
    assert mock_scrape.call_count == 1

    # Second call — should use cache
    pages = await run_scrape(test_issuer_id, urls, config)
    assert mock_scrape.call_count == 1  # not called again
    assert len(pages) == 1


@patch("asset_discovery.stages.scrape.scrape", new_callable=AsyncMock)
async def test_run_scrape_cost_tracking_live(mock_scrape, config, db_conn, test_issuer_id):
    async def scrape_side_effect(*args, **kwargs):
        usage = kwargs.get("usage")
        if usage:
            usage.pages_crawled = 2
        return [
            _make_page("https://test-cost-a.com"),
            _make_page("https://test-cost-b.com"),
        ]

    mock_scrape.side_effect = scrape_side_effect
    urls = [
        {"url": "https://test-cost-a.com", "category": "facility_page"},
        {"url": "https://test-cost-b.com", "category": "facility_page"},
    ]
    costs = CostTracker()
    await run_scrape(test_issuer_id, urls, config, costs=costs)
    assert costs.crawl4ai_pages == 2
