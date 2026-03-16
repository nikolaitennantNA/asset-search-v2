"""Integration tests for two-pass extraction (estimate → exhaustive loop).

These tests hit real LLM endpoints via Bedrock. Run with:
    uv run pytest tests/test_two_pass_extract.py -v -m integration
"""

from __future__ import annotations

import pytest

from asset_discovery.stages.extract import (
    _estimate_asset_count,
    _exhaustive_extract,
    _EXHAUSTIVE_THRESHOLD,
    EXTRACT_PROMPT_TEMPLATE,
    _format_already_found,
)
from asset_discovery.config import Config
from doc_extractor import Document


# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------

# Australian city + asset type combinations for realistic facility names
_CITIES = [
    "Wollongong", "Newcastle", "Geelong", "Townsville", "Cairns",
    "Toowoomba", "Ballarat", "Bendigo", "Mackay", "Rockhampton",
    "Bunbury", "Gladstone", "Wagga Wagga", "Albury", "Launceston",
    "Hervey Bay", "Mildura", "Shepparton", "Dubbo", "Tamworth",
    "Bathurst", "Orange", "Port Augusta", "Whyalla", "Mount Isa",
    "Kalgoorlie", "Geraldton", "Karratha", "Broome", "Darwin",
    "Alice Springs", "Katherine", "Emerald", "Moranbah", "Chinchilla",
    "Dalby", "Roma", "Kingaroy", "Gympie", "Caboolture",
    "Ipswich", "Logan", "Redland", "Beaudesert", "Warwick",
    "Stanthorpe", "Goondiwindi", "Moree", "Narrabri", "Gunnedah",
    "Mudgee", "Lithgow", "Cessnock", "Singleton", "Muswellbrook",
    "Armidale", "Coffs Harbour", "Lismore", "Grafton", "Taree",
    "Port Macquarie", "Forster", "Nowra", "Ulladulla", "Bega",
    "Queanbeyan", "Yass", "Young", "Cowra", "Parkes",
    "Forbes", "Condobolin", "Broken Hill", "Coober Pedy", "Ceduna",
    "Port Lincoln", "Murray Bridge", "Mount Gambier", "Horsham", "Hamilton",
    "Warrnambool", "Colac", "Sale", "Traralgon", "Morwell",
    "Wonthaggi", "Leongatha", "Korumburra", "Bairnsdale", "Orbost",
    "Maffra", "Seymour", "Benalla", "Wangaratta", "Wodonga",
    "Echuca", "Swan Hill", "Kerang", "Deniliquin", "Hay",
    "Griffith", "Leeton", "Narrandera", "Temora", "West Wyalong",
]

_ASSET_TYPES = [
    "Quarry", "Concrete Plant", "Asphalt Depot", "Cement Works",
    "Sand Mine", "Gravel Pit", "Crushing Facility", "Batching Plant",
    "Precast Yard", "Recycling Depot", "Lime Kiln", "Block Factory",
    "Fly Ash Terminal", "Aggregate Wharf", "Admixtures Plant",
    "Distribution Centre", "Ready-Mix Depot", "Slag Processing Facility",
    "Stone Cutting Works", "Polishing Plant",
]

# Base latitude/longitude ranges for realistic Australian coords
_BASE_LAT = -33.0
_BASE_LNG = 151.0


def _make_facility(index: int) -> tuple[str, float, float]:
    """Return (name, lat, lng) for a synthetic facility."""
    city = _CITIES[index % len(_CITIES)]
    asset_type = _ASSET_TYPES[index % len(_ASSET_TYPES)]
    name = f"{city} {asset_type}"
    # Spread coords across Australia
    lat = _BASE_LAT - (index * 0.05)
    lng = _BASE_LNG + (index * 0.03)
    return name, round(lat, 6), round(lng, 6)


def _build_page_markdown(facility_count: int) -> str:
    """Build a realistic scraped-page markdown with signal header + body."""
    facilities = [_make_facility(i) for i in range(facility_count)]

    signal_lines = []
    for name, lat, lng in facilities:
        signal_lines.append(f"- **{name}** — coordinates: ({lat}, {lng})")

    body_names = ", ".join(f[0] for f in facilities[:10])

    return (
        "## Pre-extracted Location Signals\n\n"
        "**Named facilities:**\n"
        + "\n".join(signal_lines)
        + "\n\n---\n\n"
        "# Granite Resources Australia — Operations Overview\n\n"
        "Granite Resources Australia (GRA) is a leading producer of construction "
        "materials across eastern and central Australia. The company operates "
        f"{facility_count} facilities including {body_names}, and many more.\n\n"
        "GRA's operations span quarrying, ready-mix concrete production, asphalt "
        "manufacturing, precast concrete, and recycled materials processing. The "
        "company supplies major infrastructure projects including highways, rail "
        "corridors, and urban development across New South Wales, Queensland, "
        "Victoria, South Australia, and Western Australia.\n\n"
        "All facilities are wholly owned and operated by Granite Resources "
        "Australia Pty Ltd, a subsidiary of Granite Resources Holdings Ltd.\n"
    )


COMPANY_NAME = "Granite Resources Australia"

# Pre-build the two test pages
PAGE_60 = _build_page_markdown(60)
PAGE_100 = _build_page_markdown(100)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def config() -> Config:
    return Config()


@pytest.fixture(scope="module")
def doc_60() -> Document:
    return Document(
        content=PAGE_60,
        metadata={"url": "https://example.com/gra-operations-60"},
    )


@pytest.fixture(scope="module")
def doc_100() -> Document:
    return Document(
        content=PAGE_100,
        metadata={"url": "https://example.com/gra-operations-100"},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_estimate_asset_count_60(config: Config, doc_60: Document):
    """Cheap model should estimate ~50-70 assets for a 60-asset page."""
    estimate = await _estimate_asset_count(doc_60, COMPANY_NAME, config)
    assert 50 <= estimate <= 70, (
        f"Expected estimate in [50, 70] for 60-asset page, got {estimate}"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_estimate_asset_count_100(config: Config, doc_100: Document):
    """Cheap model should estimate ~80-120 assets for a 100-asset page."""
    estimate = await _estimate_asset_count(doc_100, COMPANY_NAME, config)
    assert 80 <= estimate <= 120, (
        f"Expected estimate in [80, 120] for 100-asset page, got {estimate}"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routing_logic(config: Config, doc_60: Document, doc_100: Document):
    """Both pages should be routed to exhaustive extraction (count > threshold)."""
    estimate_60 = await _estimate_asset_count(doc_60, COMPANY_NAME, config)
    estimate_100 = await _estimate_asset_count(doc_100, COMPANY_NAME, config)

    assert estimate_60 > _EXHAUSTIVE_THRESHOLD, (
        f"60-asset page estimate ({estimate_60}) should exceed threshold "
        f"({_EXHAUSTIVE_THRESHOLD})"
    )
    assert estimate_100 > _EXHAUSTIVE_THRESHOLD, (
        f"100-asset page estimate ({estimate_100}) should exceed threshold "
        f"({_EXHAUSTIVE_THRESHOLD})"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_exhaustive_extract_breaks_ceiling(config: Config, doc_100: Document):
    """Exhaustive extraction on a 100-asset page should find well over 20 assets.

    The whole point of looped extraction is to break past the ~20-asset ceiling
    that single-pass extraction hits. We verify we get significantly more.
    """
    estimate = await _estimate_asset_count(doc_100, COMPANY_NAME, config)
    assets = await _exhaustive_extract(
        doc_100, COMPANY_NAME, estimate, config,
    )

    asset_names = [a.asset_name for a in assets]
    unique_names = set(a.strip().lower() for a in asset_names)

    assert len(assets) > 20, (
        f"Exhaustive extraction should break past 20-asset ceiling, "
        f"got only {len(assets)} assets"
    )
    # Should also have reasonable dedup (no massive over-count)
    assert len(unique_names) > 20, (
        f"Expected >20 unique asset names, got {len(unique_names)}"
    )
    print(f"\n  Exhaustive extraction result: {len(assets)} total assets, "
          f"{len(unique_names)} unique names (target ~{estimate})")
    for name in sorted(unique_names)[:10]:
        print(f"    - {name}")
    if len(unique_names) > 10:
        print(f"    ... and {len(unique_names) - 10} more")
