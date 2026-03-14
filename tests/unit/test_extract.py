"""Tests for stages/extract.py — extraction orchestration with mocked deps."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from asset_discovery.models import Asset
from asset_discovery.cost import CostTracker
from asset_discovery.stages.extract import run_extract


def _mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


@pytest.mark.asyncio
@patch("asset_discovery.stages.extract.get_connection")
@patch("asset_discovery.stages.extract.get_extraction_result")
@patch("asset_discovery.stages.extract.extract", new_callable=AsyncMock)
@patch("asset_discovery.stages.extract.save_extraction_result")
@patch("asset_discovery.stages.extract.show_stage")
async def test_run_extract_cache_hit(mock_show, mock_save, mock_extract, mock_get_result, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_get_result.return_value = {
        "assets_json": [{"asset_name": "Quarry", "entity_name": "Boral"}]
    }
    pages = [{"url": "https://a.com", "page_id": "p1", "markdown": "# Page"}]
    from asset_discovery.config import Config
    assets = await run_extract("issuer-1", "Boral", pages, Config())
    assert len(assets) == 1
    assert assets[0].asset_name == "Quarry"
    mock_extract.assert_not_called()


@pytest.mark.asyncio
@patch("asset_discovery.stages.extract.get_connection")
@patch("asset_discovery.stages.extract.get_extraction_result")
@patch("asset_discovery.stages.extract.extract", new_callable=AsyncMock)
@patch("asset_discovery.stages.extract.save_extraction_result")
@patch("asset_discovery.stages.extract.show_stage")
async def test_run_extract_cache_dedup(mock_show, mock_save, mock_extract, mock_get_result, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    # Two pages with same cached asset — should dedup
    cached = {"assets_json": [{"asset_name": "Quarry", "entity_name": "Boral"}]}
    mock_get_result.return_value = cached
    pages = [
        {"url": "https://a.com", "page_id": "p1", "markdown": "# A"},
        {"url": "https://b.com", "page_id": "p2", "markdown": "# B"},
    ]
    from asset_discovery.config import Config
    assets = await run_extract("issuer-1", "Boral", pages, Config())
    assert len(assets) == 1  # deduped by (name, entity)


@pytest.mark.asyncio
@patch("asset_discovery.stages.extract.get_connection")
@patch("asset_discovery.stages.extract.get_extraction_result", return_value=None)
@patch("asset_discovery.stages.extract.extract", new_callable=AsyncMock)
@patch("asset_discovery.stages.extract.save_extraction_result")
@patch("asset_discovery.stages.extract.show_stage")
async def test_run_extract_calls_extractor(mock_show, mock_save, mock_extract, mock_get_result, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_extract.return_value = [Asset(asset_name="Plant", entity_name="Boral")]
    pages = [{"url": "https://a.com", "page_id": "p1", "markdown": "# Content", "content_hash": "h1"}]
    from asset_discovery.config import Config
    assets = await run_extract("issuer-1", "Boral", pages, Config())
    assert len(assets) == 1
    # Verify prompt contains company name
    call_kwargs = mock_extract.call_args[1]
    assert "Boral" in call_kwargs["prompt"]


@pytest.mark.asyncio
@patch("asset_discovery.stages.extract.get_connection")
@patch("asset_discovery.stages.extract.get_extraction_result", return_value=None)
@patch("asset_discovery.stages.extract.extract", new_callable=AsyncMock)
@patch("asset_discovery.stages.extract.save_extraction_result")
@patch("asset_discovery.stages.extract.show_stage")
async def test_run_extract_prompt_includes_ald_summary(mock_show, mock_save, mock_extract, mock_get_result, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_extract.return_value = []
    pages = [{"url": "https://a.com", "page_id": "p1", "markdown": "# Content", "content_hash": "h1"}]
    from asset_discovery.config import Config
    await run_extract("issuer-1", "Boral", pages, Config(), existing_assets_summary="Known: Quarry X")
    call_kwargs = mock_extract.call_args[1]
    assert "Known: Quarry X" in call_kwargs["prompt"]


@pytest.mark.asyncio
@patch("asset_discovery.stages.extract.get_connection")
@patch("asset_discovery.stages.extract.get_extraction_result", return_value=None)
@patch("asset_discovery.stages.extract.extract", new_callable=AsyncMock)
@patch("asset_discovery.stages.extract.save_extraction_result")
@patch("asset_discovery.stages.extract.show_stage")
async def test_run_extract_saves_per_page(mock_show, mock_save, mock_extract, mock_get_result, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_extract.return_value = [Asset(asset_name="Plant", entity_name="Boral")]
    pages = [
        {"url": "https://a.com", "page_id": "p1", "markdown": "# A", "content_hash": "h1"},
        {"url": "https://b.com", "page_id": "p2", "markdown": "# B", "content_hash": "h2"},
    ]
    from asset_discovery.config import Config
    await run_extract("issuer-1", "Boral", pages, Config())
    assert mock_save.call_count == 2  # saved once per page


@pytest.mark.asyncio
@patch("asset_discovery.stages.extract.get_connection")
@patch("asset_discovery.stages.extract.get_extraction_result", return_value=None)
@patch("asset_discovery.stages.extract.extract", new_callable=AsyncMock)
@patch("asset_discovery.stages.extract.save_extraction_result")
@patch("asset_discovery.stages.extract.show_stage")
async def test_run_extract_cost_tracking(mock_show, mock_save, mock_extract, mock_get_result, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()

    # Mock extract to populate the usage object with token counts
    async def extract_side_effect(*args, **kwargs):
        usage = kwargs.get("usage")
        if usage:
            usage.input_tokens = 500
            usage.output_tokens = 200
        return []

    mock_extract.side_effect = extract_side_effect
    pages = [{"url": "https://a.com", "page_id": "p1", "markdown": "# A", "content_hash": "h1"}]
    costs = CostTracker()
    from asset_discovery.config import Config
    await run_extract("issuer-1", "Boral", pages, Config(), costs=costs)
    assert "extract" in costs.tokens_by_stage
    assert costs.tokens_by_stage["extract"]["input"] == 500


@pytest.mark.asyncio
@patch("asset_discovery.stages.extract.get_connection")
@patch("asset_discovery.stages.extract.show_stage")
async def test_run_extract_empty_pages(mock_show, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    from asset_discovery.config import Config
    assets = await run_extract("issuer-1", "Boral", [], Config())
    assert assets == []
