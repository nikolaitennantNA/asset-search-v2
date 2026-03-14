"""Tests for coordinate dedup in the extract stage.

Verifies that _dedup_by_coords() correctly merges assets with near-identical
coordinates (from signal injection double-counting) while preserving distinct
assets and those without coordinates.
"""

from asset_discovery.models import Asset
from asset_discovery.stages.extract import _dedup_by_coords


def _make_asset(name: str, lat: float | None = None, lng: float | None = None) -> Asset:
    return Asset(
        asset_name=name,
        entity_name="TestCo",
        latitude=lat,
        longitude=lng,
    )


def test_dedup_by_coords_removes_near_duplicates():
    """Assets within ~55m should be deduped, keeping the first."""
    assets = [
        _make_asset("Plant A", -33.868800, 151.209300),
        _make_asset("Plant A (dup)", -33.868810, 151.209310),  # ~1m away
        _make_asset("Plant B", 40.7128, -74.0060),
    ]
    result = _dedup_by_coords(assets)
    assert len(result) == 2
    assert result[0].asset_name == "Plant A"
    assert result[1].asset_name == "Plant B"


def test_dedup_by_coords_keeps_distant_assets():
    """Assets far apart should all be kept."""
    assets = [
        _make_asset("Sydney", -33.8688, 151.2093),
        _make_asset("Melbourne", -37.8136, 144.9631),
        _make_asset("Brisbane", -27.4698, 153.0251),
    ]
    result = _dedup_by_coords(assets)
    assert len(result) == 3


def test_dedup_by_coords_preserves_no_coord_assets():
    """Assets without coordinates should always be preserved."""
    assets = [
        _make_asset("With coords", -33.8688, 151.2093),
        _make_asset("No coords 1"),
        _make_asset("No coords 2"),
    ]
    result = _dedup_by_coords(assets)
    assert len(result) == 3


def test_dedup_by_coords_empty_list():
    assert _dedup_by_coords([]) == []


def test_dedup_by_coords_threshold_boundary():
    """Assets exactly at the threshold boundary should not be deduped."""
    # 0.0005 degrees ~= 55m. Place assets just outside the threshold.
    assets = [
        _make_asset("A", 0.0, 0.1),
        _make_asset("B", 0.0006, 0.1),  # 0.0006 > 0.0005 threshold
    ]
    result = _dedup_by_coords(assets)
    assert len(result) == 2
