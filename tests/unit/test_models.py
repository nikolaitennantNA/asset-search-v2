"""Tests for models.py — Asset, QAReport, CoverageFlag."""

import pytest
from pydantic import ValidationError

from asset_search.models import Asset, QAReport, CoverageFlag


def test_asset_required_fields():
    with pytest.raises(ValidationError):
        Asset()  # missing asset_name, entity_name


def test_asset_defaults():
    a = Asset(asset_name="Quarry", entity_name="Boral")
    assert a.entity_isin == ""
    assert a.latitude is None
    assert a.capacity is None
    assert a.supplementary_details == {}
    assert a.asset_id == ""
    assert a.source_url == ""


def test_asset_roundtrip():
    data = {
        "asset_name": "Quarry",
        "entity_name": "Boral",
        "latitude": -33.8688,
        "longitude": 151.2093,
        "status": "Operating",
        "asset_type_raw": "quarry",
    }
    a = Asset(**data)
    dumped = a.model_dump()
    restored = Asset(**dumped)
    assert restored == a


def test_qa_report_defaults():
    r = QAReport()
    assert r.quality_score == 0.0
    assert r.missing_types == []
    assert r.missing_regions == []
    assert r.issues == []
    assert r.should_enrich is False
    assert r.coverage_flags == []


def test_coverage_flag():
    f = CoverageFlag(flag_type="missing_region", description="No assets in QLD", severity="high")
    assert f.flag_type == "missing_region"
    assert f.severity == "high"


from asset_search.models import DiscoveredUrl


def test_discovered_url_minimal():
    """Only url and category are required."""
    u = DiscoveredUrl(url="https://example.com/page", category="facility_page")
    assert u.url == "https://example.com/page"
    assert u.strategy is None
    assert u.proxy_mode is None
    assert u.notes == ""


def test_discovered_url_with_scrape_config():
    u = DiscoveredUrl(
        url="https://example.com/locations",
        category="facility_page",
        notes="React SPA, needs JS rendering",
        strategy="browser",
        proxy_mode="auto",
        wait_for=".locations-list",
    )
    assert u.strategy == "browser"
    assert u.proxy_mode == "auto"
    assert u.wait_for == ".locations-list"


def test_discovered_url_strategy_validation():
    """Invalid strategy should raise ValidationError."""
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        DiscoveredUrl(url="https://x.com", category="x", strategy="invalid")


def test_discovered_url_proxy_mode_validation():
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        DiscoveredUrl(url="https://x.com", category="x", proxy_mode="invalid")
