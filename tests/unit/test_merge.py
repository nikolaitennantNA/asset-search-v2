"""Tests for stages/merge.py — dedup, classification, error handling."""

import json
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from asset_discovery.models import Asset
from asset_discovery.cost import CostTracker
from asset_discovery.stages.merge import run_merge


def _mock_conn(existing_assets=None):
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.return_value = existing_assets or []
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


def _make_llm_response(assets_data):
    """Create a mock litellm response returning JSON asset data."""
    content = json.dumps({"assets": assets_data})
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=content))],
        usage=SimpleNamespace(prompt_tokens=100, completion_tokens=50),
    )


def _make_asset(name="Quarry", entity="Boral", asset_id=""):
    return Asset(asset_name=name, entity_name=entity, asset_id=asset_id)


@pytest.mark.asyncio
@patch("asset_discovery.stages.merge.get_connection")
@patch("asset_discovery.stages.merge.show_stage")
async def test_run_merge_empty(mock_show, mock_get_conn):
    from asset_discovery.config import Config
    result = await run_merge("issuer-1", [], Config())
    assert result == []
    mock_get_conn.assert_not_called()


@pytest.mark.asyncio
@patch("asset_discovery.stages.merge.get_connection")
@patch("asset_discovery.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_discovery.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_discovery.stages.merge.get_gics_mapping")
@patch("asset_discovery.stages.merge.litellm")
@patch("asset_discovery.stages.merge.show_stage")
async def test_run_merge_assigns_uuid(mock_show, mock_litellm, mock_gics, mock_save, mock_get_assets, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_gics.return_value = MagicMock(lookup=MagicMock(return_value=None))
    asset_data = [{"asset_name": "Quarry", "entity_name": "Boral", "asset_id": ""}]
    mock_litellm.acompletion = AsyncMock(return_value=_make_llm_response(asset_data))
    assets = [_make_asset()]
    from asset_discovery.config import Config
    result = await run_merge("issuer-1", assets, Config())
    assert len(result) >= 1
    assert result[0].asset_id != ""  # UUID assigned


@pytest.mark.asyncio
@patch("asset_discovery.stages.merge.get_connection")
@patch("asset_discovery.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_discovery.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_discovery.stages.merge.get_gics_mapping")
@patch("asset_discovery.stages.merge.litellm")
@patch("asset_discovery.stages.merge.show_stage")
async def test_run_merge_sets_metadata(mock_show, mock_litellm, mock_gics, mock_save, mock_get_assets, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_gics.return_value = MagicMock(lookup=MagicMock(return_value=None))
    asset_data = [{"asset_name": "Quarry", "entity_name": "Boral"}]
    mock_litellm.acompletion = AsyncMock(return_value=_make_llm_response(asset_data))
    assets = [_make_asset()]
    from asset_discovery.config import Config
    result = await run_merge("issuer-1", assets, Config(), industry_code="B0810")
    assert result[0].industry_code == "B0810"
    assert result[0].attribution_source == "asset_discovery"
    assert result[0].date_researched == date.today().isoformat()


@pytest.mark.asyncio
@patch("asset_discovery.stages.merge.get_connection")
@patch("asset_discovery.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_discovery.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_discovery.stages.merge.get_gics_mapping")
@patch("asset_discovery.stages.merge.litellm")
@patch("asset_discovery.stages.merge.show_stage")
async def test_run_merge_batching(mock_show, mock_litellm, mock_gics, mock_save, mock_get_assets, mock_get_conn):
    """51+ assets should trigger 2 _merge_batch calls (batch_size=50)."""
    mock_get_conn.return_value = _mock_conn()
    mock_gics.return_value = MagicMock(lookup=MagicMock(return_value=None))
    asset_data = [{"asset_name": f"Asset {i}", "entity_name": "Boral"} for i in range(51)]
    mock_litellm.acompletion = AsyncMock(return_value=_make_llm_response(asset_data[:50]))
    assets = [_make_asset(name=f"Asset {i}") for i in range(51)]
    from asset_discovery.config import Config
    await run_merge("issuer-1", assets, Config())
    # 2 batch calls + 1 final dedup = 3 total acompletion calls
    assert mock_litellm.acompletion.call_count >= 2


@pytest.mark.asyncio
@patch("asset_discovery.stages.merge.get_connection")
@patch("asset_discovery.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_discovery.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_discovery.stages.merge.get_gics_mapping")
@patch("asset_discovery.stages.merge._final_dedup", new_callable=AsyncMock)
@patch("asset_discovery.stages.merge._merge_batch", new_callable=AsyncMock)
@patch("asset_discovery.stages.merge.show_stage")
async def test_run_merge_final_dedup_called(mock_show, mock_merge_batch, mock_final_dedup, mock_gics, mock_save, mock_get_assets, mock_get_conn):
    """_final_dedup is called when result has >1 asset."""
    mock_get_conn.return_value = _mock_conn()
    mock_gics.return_value = MagicMock(lookup=MagicMock(return_value=None))
    mock_merge_batch.return_value = [_make_asset(name="A"), _make_asset(name="B")]
    mock_final_dedup.return_value = [_make_asset(name="A"), _make_asset(name="B")]
    assets = [_make_asset(name="A"), _make_asset(name="B")]
    from asset_discovery.config import Config
    await run_merge("issuer-1", assets, Config())
    mock_final_dedup.assert_called_once()


@pytest.mark.asyncio
@patch("asset_discovery.stages.merge.get_connection")
@patch("asset_discovery.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_discovery.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_discovery.stages.merge.get_gics_mapping")
@patch("asset_discovery.stages.merge.litellm")
@patch("asset_discovery.stages.merge.show_stage")
async def test_run_merge_dedup_by_id(mock_show, mock_litellm, mock_gics, mock_save, mock_get_assets, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_gics.return_value = MagicMock(lookup=MagicMock(return_value=None))
    # LLM returns same asset_id in both calls — should dedup
    asset_data = [{"asset_name": "Quarry", "entity_name": "Boral", "asset_id": "dup-id"}]
    mock_litellm.acompletion = AsyncMock(return_value=_make_llm_response(asset_data))
    # Create 51 assets to trigger 2 batches (batch_size=50)
    assets = [_make_asset(name=f"Asset {i}") for i in range(51)]
    from asset_discovery.config import Config
    result = await run_merge("issuer-1", assets, Config())
    # Despite 2 batches returning "dup-id", only 1 asset in final result
    id_count = sum(1 for a in result if a.asset_id == "dup-id")
    assert id_count == 1


@pytest.mark.asyncio
@patch("asset_discovery.stages.merge.get_connection")
@patch("asset_discovery.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_discovery.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_discovery.stages.merge.get_gics_mapping")
@patch("asset_discovery.stages.merge.litellm")
@patch("asset_discovery.stages.merge.show_stage")
async def test_run_merge_cost_tracking(mock_show, mock_litellm, mock_gics, mock_save, mock_get_assets, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_gics.return_value = MagicMock(lookup=MagicMock(return_value=None))
    asset_data = [{"asset_name": "Quarry", "entity_name": "Boral"}]
    mock_litellm.acompletion = AsyncMock(return_value=_make_llm_response(asset_data))
    assets = [_make_asset()]
    costs = CostTracker()
    from asset_discovery.config import Config
    await run_merge("issuer-1", assets, Config(), costs=costs)
    assert costs.tokens_by_stage.get("merge", {}).get("calls", 0) >= 1


@pytest.mark.asyncio
@patch("asset_discovery.stages.merge.get_connection")
@patch("asset_discovery.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_discovery.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_discovery.stages.merge.get_gics_mapping")
@patch("asset_discovery.stages.merge.litellm")
@patch("asset_discovery.stages.merge.show_stage")
async def test_run_merge_batch_llm_error_fallback(mock_show, mock_litellm, mock_gics, mock_save, mock_get_assets, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_gics.return_value = MagicMock(lookup=MagicMock(return_value=None))
    # LLM returns invalid JSON
    bad_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="not json"))],
        usage=SimpleNamespace(prompt_tokens=0, completion_tokens=0),
    )
    mock_litellm.acompletion = AsyncMock(return_value=bad_response)
    assets = [_make_asset(name="Plant")]
    from asset_discovery.config import Config
    result = await run_merge("issuer-1", assets, Config())
    # Fallback: returns original batch unchanged
    assert len(result) >= 1
    assert result[0].asset_name == "Plant"


@pytest.mark.asyncio
@patch("asset_discovery.stages.merge.get_connection")
@patch("asset_discovery.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_discovery.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_discovery.stages.merge.get_gics_mapping")
@patch("asset_discovery.stages.merge.litellm")
@patch("asset_discovery.stages.merge.show_stage")
async def test_run_merge_final_dedup_error_fallback(mock_show, mock_litellm, mock_gics, mock_save, mock_get_assets, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_gics.return_value = MagicMock(lookup=MagicMock(return_value=None))
    # First call (merge batch) succeeds, second call (final dedup) returns bad JSON
    good_data = [
        {"asset_name": "A", "entity_name": "Boral"},
        {"asset_name": "B", "entity_name": "Boral"},
    ]
    good_response = _make_llm_response(good_data)
    bad_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="not json"))],
        usage=SimpleNamespace(prompt_tokens=0, completion_tokens=0),
    )
    mock_litellm.acompletion = AsyncMock(side_effect=[good_response, bad_response])
    assets = [_make_asset(name="A"), _make_asset(name="B")]
    from asset_discovery.config import Config
    result = await run_merge("issuer-1", assets, Config())
    # Final dedup fails gracefully — returns assets from merge batch
    assert len(result) == 2
