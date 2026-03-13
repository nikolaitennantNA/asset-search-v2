"""Tests for discover/QA agent tools."""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from asset_search.stages import tools


@pytest.fixture(autouse=True)
def _init_tools():
    cfg = MagicMock()
    cfg.crawl4ai_api_key = "test-key"
    cfg.max_urls_per_run = 5000
    tools.init_tools(cfg, "issuer-1")


@pytest.mark.asyncio
@patch("asset_search.stages.tools._get_conn")
@patch("asset_search.stages.tools.get_discovered_urls", return_value=[])
@patch("asset_search.stages.tools.save_discovered_urls", return_value=1)
async def test_save_urls_passes_structured_fields(mock_save, mock_get, mock_conn):
    mock_conn.return_value = MagicMock()
    count = await tools.save_urls(urls=[{
        "url": "https://example.com/locations",
        "category": "facility_page",
        "notes": "JS-heavy SPA",
        "strategy": "browser",
        "proxy_mode": "auto",
        "wait_for": ".locations-list",
    }])
    assert count == 1
    saved = mock_save.call_args[0][2]  # third arg is the urls list
    assert saved[0]["strategy"] == "browser"
    assert saved[0]["proxy_mode"] == "auto"
    assert saved[0]["wait_for"] == ".locations-list"


@pytest.mark.asyncio
@patch("asset_search.stages.tools.httpx.AsyncClient")
async def test_crawl_page_default_uses_http(_mock_client_cls):
    mock_client = AsyncMock()
    _mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
    _mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
    mock_client.post.return_value = MagicMock(
        status_code=200,
        raise_for_status=MagicMock(),
        json=MagicMock(return_value={
            "success": True, "markdown": "# Page",
            "links": {"internal": [], "external": []}, "metadata": {},
        }),
    )
    result = await tools.crawl_page("https://example.com/page")
    payload = mock_client.post.call_args[1]["json"]
    assert payload["strategy"] == "http"
    assert result["markdown"] == "# Page"


@pytest.mark.asyncio
@patch("asset_search.stages.tools.httpx.AsyncClient")
async def test_crawl_page_browser_uses_browser_strategy(_mock_client_cls):
    mock_client = AsyncMock()
    _mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
    _mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
    mock_client.post.return_value = MagicMock(
        status_code=200,
        raise_for_status=MagicMock(),
        json=MagicMock(return_value={
            "success": True, "markdown": "# JS Page",
            "links": {"internal": [], "external": []}, "metadata": {},
        }),
    )
    result = await tools.crawl_page("https://example.com/page", browser=True)
    payload = mock_client.post.call_args[1]["json"]
    assert payload["strategy"] == "browser"
    assert result["markdown"] == "# JS Page"


@pytest.mark.asyncio
@patch("asset_search.stages.tools.httpx.AsyncClient")
async def test_probe_urls_returns_metadata(_mock_client_cls):
    mock_client = AsyncMock()
    _mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
    _mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
    responses = [
        MagicMock(
            status_code=200,
            headers={"content-type": "text/html; charset=utf-8", "content-length": "45000"},
            text="<html><head><title>Sydney Quarry - Boral</title></head><body>x</body></html>",
            content=b"x",
        ),
        MagicMock(
            status_code=403,
            headers={"content-type": "text/html", "server": "cloudflare"},
            text="", content=b"",
        ),
        MagicMock(
            status_code=200,
            headers={"content-type": "application/pdf", "content-length": "1200000"},
            text="", content=b"",
        ),
    ]
    mock_client.get.side_effect = responses
    results = await tools.probe_urls([
        "https://boral.com/locations/sydney",
        "https://blocked.com/page",
        "https://boral.com/reports/annual.pdf",
    ])
    assert len(results) == 3
    assert results[0]["status"] == 200
    assert results[0]["content_type"] == "text/html"
    assert results[0]["content_length"] == 45000
    assert results[0]["title"] == "Sydney Quarry - Boral"
    assert results[1]["status"] == 403
    assert results[1]["waf_blocked"] is True
    assert results[2]["content_type"] == "application/pdf"


@pytest.mark.asyncio
@patch("asset_search.stages.tools.httpx.AsyncClient")
async def test_probe_urls_handles_errors(_mock_client_cls):
    mock_client = AsyncMock()
    _mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
    _mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
    mock_client.get.side_effect = Exception("Connection refused")
    results = await tools.probe_urls(["https://down.com/page"])
    assert len(results) == 1
    assert results[0]["status"] == 0
    assert results[0]["error"] == "Connection refused"


@pytest.mark.asyncio
async def test_probe_urls_empty_list():
    results = await tools.probe_urls([])
    assert results == []
