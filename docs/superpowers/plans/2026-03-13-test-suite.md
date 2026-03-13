# Test Suite Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build proper pytest test suites for asset-search-v2 and web-scraper with unit + integration coverage.

**Architecture:** Two repos get `tests/unit/` and `tests/integration/` directories. Unit tests mock all external dependencies (HTTP, DB, LLM). Integration tests use `@pytest.mark.integration` marker and hit real Crawl4AI (web-scraper) or real Postgres (asset-search-v2). web-scraper owns Crawl4AI API testing; asset-search-v2 mocks the scraper and tests its own DB orchestration.

**Tech Stack:** pytest, pytest-asyncio, respx (HTTP mocking), unittest.mock (DB/LLM mocking)

**Spec:** `docs/superpowers/specs/2026-03-13-test-suite-design.md`

---

## Chunk 1: Infrastructure + web-scraper unit tests

### Task 1: web-scraper — test infrastructure and directory restructure

**Files:**
- Modify: `/Users/nikolai.tennant/Documents/GitHub/web-scraper/pyproject.toml`
- Create: `/Users/nikolai.tennant/Documents/GitHub/web-scraper/tests/conftest.py`
- Create: `/Users/nikolai.tennant/Documents/GitHub/web-scraper/tests/unit/__init__.py`
- Create: `/Users/nikolai.tennant/Documents/GitHub/web-scraper/tests/unit/conftest.py`
- Create: `/Users/nikolai.tennant/Documents/GitHub/web-scraper/tests/integration/__init__.py`
- Create: `/Users/nikolai.tennant/Documents/GitHub/web-scraper/tests/integration/conftest.py`
- Move: existing `tests/test_scraper.py` → `tests/unit/test_scraper.py`
- Move: existing `tests/test_models.py` → `tests/unit/test_models.py`
- Move: existing `tests/test_signals.py` → `tests/unit/test_signals.py`

- [ ] **Step 1: Update pyproject.toml with marker config**

In `/Users/nikolai.tennant/Documents/GitHub/web-scraper/pyproject.toml`, add the integration marker:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
markers = ["integration: hits real external services (Crawl4AI API)"]
```

- [ ] **Step 2: Create directory structure and move existing tests**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/web-scraper
mkdir -p tests/unit tests/integration
touch tests/unit/__init__.py tests/integration/__init__.py
mv tests/test_scraper.py tests/unit/test_scraper.py
mv tests/test_models.py tests/unit/test_models.py
mv tests/test_signals.py tests/unit/test_signals.py
```

- [ ] **Step 2b: Remove duplicate `mock_crawl4ai` fixture from moved test_scraper.py**

After moving, `tests/unit/test_scraper.py` has a local `mock_crawl4ai` fixture that duplicates the one in `tests/unit/conftest.py`. Remove the fixture and its imports from the test file (keep the imports used by tests, remove `respx` and `httpx` since the fixture is now in conftest):

In `tests/unit/test_scraper.py`, delete this block:
```python
@pytest.fixture
def mock_crawl4ai():
    with respx.mock(base_url=CRAWL4AI_BASE) as m:
        yield m
```

And remove `import respx` and `import httpx` from the test file (they're only needed by the fixture, which is now in conftest).

- [ ] **Step 3: Create tests/conftest.py (root)**

```python
"""Shared pytest configuration for web-scraper."""
```

- [ ] **Step 4: Create tests/unit/conftest.py**

```python
"""Unit test fixtures for web-scraper."""

import pytest
import respx
import httpx

CRAWL4AI_BASE = "https://api.crawl4ai.com/v1"


@pytest.fixture
def mock_crawl4ai():
    with respx.mock(base_url=CRAWL4AI_BASE) as m:
        yield m
```

- [ ] **Step 5: Create tests/integration/conftest.py**

```python
"""Integration test fixtures for web-scraper."""

import csv
import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Load .env from asset-search-v2 (sibling repo has the API key)
_asset_search_root = Path(__file__).resolve().parents[3] / "asset-search-v2"
load_dotenv(_asset_search_root / ".env")


@pytest.fixture
def crawl4ai_api_key():
    key = os.environ.get("CRAWL4AI_API_KEY", "")
    if not key:
        pytest.skip("CRAWL4AI_API_KEY not set")
    return key


@pytest.fixture
def boral_urls():
    csv_path = _asset_search_root / "output/boral-ltd/2026-03-13T02-16-31/discovered_urls.csv"
    if not csv_path.exists():
        pytest.skip(f"Boral URL CSV not found at {csv_path}")
    with open(csv_path) as f:
        return [row["url"] for row in csv.DictReader(f)]


@pytest.fixture
def integration_url_count():
    return int(os.environ.get("INTEGRATION_TEST_URLS", "5"))
```

- [ ] **Step 6: Run existing tests from new location to verify move**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/web-scraper && uv run pytest tests/unit/ -v`
Expected: All 11 existing tests PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/web-scraper
git add tests/ pyproject.toml
git commit -m "test: restructure into unit/integration directories, add marker config"
```

### Task 2: web-scraper — usage tracking tests (new)

**Files:**
- Modify: `/Users/nikolai.tennant/Documents/GitHub/web-scraper/tests/unit/test_scraper.py`

- [ ] **Step 1: Add usage tracking tests to test_scraper.py**

Append to the existing file:

```python
@pytest.mark.asyncio
async def test_usage_tracking_success(mock_crawl4ai):
    mock_crawl4ai.post("/crawl").mock(return_value=httpx.Response(200, json=_SINGLE_RESPONSE))
    usage = Usage()
    await scrape(urls=["https://example.com"], api_key="test-key", usage=usage)
    assert usage.pages_crawled == 1
    assert usage.pages_failed == 0


@pytest.mark.asyncio
async def test_usage_tracking_failure(mock_crawl4ai):
    mock_crawl4ai.post("/crawl").mock(return_value=httpx.Response(200, json={"success": False}))
    usage = Usage()
    await scrape(urls=["https://example.com/fail"], api_key="test-key", usage=usage)
    assert usage.pages_crawled == 0
    assert usage.pages_failed == 1


@pytest.mark.asyncio
async def test_usage_tracking_mixed(mock_crawl4ai):
    """Batch with mix of success/fail pages."""
    batch_response = [
        {**_BATCH_RESPONSE[0]},
        {"success": False, "url": "https://example.com/fail", "error": "Timeout"},
    ]
    mock_crawl4ai.post("/crawl/batch").mock(
        return_value=httpx.Response(200, json=batch_response)
    )
    usage = Usage()
    await scrape(
        urls=["https://example.com/a", "https://example.com/fail"],
        api_key="test-key",
        usage=usage,
    )
    assert usage.pages_crawled == 1
    assert usage.pages_failed == 1
```

Also add `Usage` to the imports at the top of the file:

```python
from web_scraper import scrape, ScrapeConfig, Usage
```

- [ ] **Step 2: Run tests**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/web-scraper && uv run pytest tests/unit/test_scraper.py -v`
Expected: All 14 tests PASS (11 existing + 3 new).

- [ ] **Step 3: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/web-scraper
git add tests/unit/test_scraper.py
git commit -m "test: add usage tracking tests for scraper"
```

### Task 3: web-scraper — expanded signal extraction tests

**Files:**
- Rewrite: `/Users/nikolai.tennant/Documents/GitHub/web-scraper/tests/unit/test_signals.py`

- [ ] **Step 1: Write the expanded test_signals.py**

```python
"""Tests for HTML signal extraction (coordinates, addresses, structured data)."""

from web_scraper.signals import (
    extract_signals,
    inject_signals,
    _is_valid_coord,
    _dedup_coords,
    _html_to_text,
    _extract_jsonld_locations,
    _extract_google_maps_coords,
    _extract_data_attr_coords,
    _extract_meta_geo,
    _extract_inline_js_coords,
    _extract_html_coordinates,
    _extract_embedded_json,
)


# ── Existing tests ──────────────────────────────────────────────────────────


def test_extract_data_attributes():
    html = '<div data-lat="-33.8688" data-lng="151.2093"></div>'
    signals = extract_signals(html)
    assert len(signals.get("coordinates", [])) > 0


def test_inject_empty_signals():
    md = "# Page Title"
    result = inject_signals(md, {})
    assert result == md


# ── JSON-LD ─────────────────────────────────────────────────────────────────


def test_jsonld_with_graph():
    html = """<script type="application/ld+json">{
        "@graph": [{
            "@type": "Place",
            "name": "Boral Quarry",
            "address": {
                "@type": "PostalAddress",
                "streetAddress": "123 Quarry Rd",
                "addressLocality": "Sydney",
                "addressRegion": "NSW",
                "postalCode": "2000",
                "addressCountry": "AU"
            },
            "geo": {"latitude": -33.8688, "longitude": 151.2093}
        }]
    }</script>"""
    locations = _extract_jsonld_locations(html)
    assert len(locations) >= 1
    loc = locations[0]
    assert loc["name"] == "Boral Quarry"
    assert "123 Quarry Rd" in loc["address"]
    assert loc["lat"] == -33.8688
    assert loc["lng"] == 151.2093


def test_jsonld_nested_locations():
    html = """<script type="application/ld+json">{
        "@type": "Organization",
        "name": "Boral",
        "location": {
            "@type": "Place",
            "name": "Head Office",
            "address": "Level 3, 40 Mount St, North Sydney"
        }
    }</script>"""
    locations = _extract_jsonld_locations(html)
    names = [loc["name"] for loc in locations]
    assert "Head Office" in names


def test_jsonld_string_address():
    html = """<script type="application/ld+json">{
        "@type": "Place",
        "name": "Depot",
        "address": "42 Industrial Ave, Melbourne VIC 3000"
    }</script>"""
    locations = _extract_jsonld_locations(html)
    assert any("42 Industrial Ave" in loc["address"] for loc in locations)


# ── Google Maps ─────────────────────────────────────────────────────────────


def test_google_maps_at_sign():
    html = '<a href="https://www.google.com/maps/@-33.8688,151.2093,15z">Map</a>'
    coords = _extract_google_maps_coords(html)
    assert len(coords) >= 1
    assert coords[0]["lat"] == -33.8688
    assert coords[0]["lng"] == 151.2093


def test_google_maps_query_param():
    html = '<a href="https://maps.google.com/maps?q=-33.8688,151.2093">Map</a>'
    coords = _extract_google_maps_coords(html)
    assert len(coords) >= 1
    assert abs(coords[0]["lat"] - (-33.8688)) < 0.001


def test_google_maps_place():
    html = '<a href="https://www.google.com/maps/place/-33.8688,151.2093">Map</a>'
    coords = _extract_google_maps_coords(html)
    assert len(coords) >= 1


# ── Data attributes ─────────────────────────────────────────────────────────


def test_data_attr_with_name_address():
    html = '<div data-lat="-33.8688" data-lng="151.2093" data-name="Quarry" data-address="123 Rd"></div>'
    locations, coords = _extract_data_attr_coords(html)
    assert len(locations) >= 1
    assert locations[0]["name"] == "Quarry"
    assert locations[0]["address"] == "123 Rd"


def test_data_attr_without_name():
    html = '<div data-lat="-33.8688" data-lng="151.2093"></div>'
    locations, coords = _extract_data_attr_coords(html)
    assert len(coords) >= 1
    assert coords[0]["lat"] == -33.8688


def test_data_attr_reverse_order():
    html = '<div data-lng="151.2093" data-latitude="-33.8688"></div>'
    locations, coords = _extract_data_attr_coords(html)
    assert len(locations) + len(coords) >= 1


# ── Meta tags ───────────────────────────────────────────────────────────────


def test_meta_geo_position():
    html = '<meta name="geo.position" content="-33.8688;151.2093">'
    coords = _extract_meta_geo(html)
    assert len(coords) >= 1
    assert abs(coords[0]["lat"] - (-33.8688)) < 0.001


def test_meta_og_latlong():
    html = """
    <meta property="og:latitude" content="-33.8688">
    <meta property="og:longitude" content="151.2093">
    """
    coords = _extract_meta_geo(html)
    assert len(coords) >= 1
    assert abs(coords[0]["lat"] - (-33.8688)) < 0.001


# ── Inline JS ───────────────────────────────────────────────────────────────


def test_inline_js_latlng_constructor():
    html = "<script>var pos = new google.maps.LatLng(-33.8688, 151.2093);</script>"
    coords = _extract_inline_js_coords(html)
    assert len(coords) >= 1
    assert coords[0]["lat"] == -33.8688


def test_inline_js_object_notation():
    html = '<script>var loc = {"lat": -33.8688, "lng": 151.2093};</script>'
    coords = _extract_inline_js_coords(html)
    assert len(coords) >= 1


def test_inline_js_array():
    html = "<script>var coords = [-33.8688, 151.2093];</script>"
    coords = _extract_inline_js_coords(html)
    assert len(coords) >= 1
    assert abs(coords[0]["lat"] - (-33.8688)) < 0.001


# ── Embedded JSON ───────────────────────────────────────────────────────────


def test_embedded_geojson_point():
    html = """<script type="application/json">{
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [151.2093, -33.8688]},
        "properties": {}
    }</script>"""
    locations, coords = _extract_embedded_json(html)
    assert len(coords) >= 1
    # GeoJSON is [lng, lat]
    assert abs(coords[0]["lat"] - (-33.8688)) < 0.001
    assert abs(coords[0]["lng"] - 151.2093) < 0.001


def test_embedded_geojson_with_properties():
    html = """<script type="application/json">{
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [151.2093, -33.8688]},
        "properties": {"tooltip": "Boral Quarry"}
    }</script>"""
    locations, coords = _extract_embedded_json(html)
    assert len(locations) >= 1
    assert locations[0]["name"] == "Boral Quarry"


# ── Raw HTML patterns ──────────────────────────────────────────────────────


def test_html_coordinate_pattern():
    html = "<p>Located at -33.8688, 151.2093 in Sydney</p>"
    coords = _extract_html_coordinates(html)
    assert len(coords) >= 1
    assert abs(coords[0]["lat"] - (-33.8688)) < 0.001


def test_html_to_text_address_tag():
    html = "<address>123 Quarry Rd<br>Sydney NSW 2000</address>"
    text = _html_to_text(html)
    assert "123 Quarry Rd" in text
    assert "Sydney" in text


# ── SVG / dedup / validation ───────────────────────────────────────────────


def test_svg_stripping():
    """SVG path data should not be matched as coordinates."""
    html = '<svg viewBox="0 0 100 100"><path d="M-33.8688,151.2093 L10,20"/></svg>'
    signals = extract_signals(html)
    assert len(signals["coordinates"]) == 0


def test_coord_dedup_within_threshold():
    """Coords within _COORD_DEDUP_THRESHOLD (0.0005, ~55m) should be deduplicated."""
    coords = [
        {"lat": -33.8688, "lng": 151.2093, "source": "a"},
        {"lat": -33.8689, "lng": 151.2094, "source": "b"},  # ~15m away
    ]
    result = _dedup_coords(coords)
    assert len(result) == 1


def test_coord_dedup_preserves_distant():
    """Coords further apart than 0.0005 should be kept separate."""
    coords = [
        {"lat": -33.8688, "lng": 151.2093, "source": "a"},
        {"lat": -33.8800, "lng": 151.2200, "source": "b"},  # ~1.5km away
    ]
    result = _dedup_coords(coords)
    assert len(result) == 2


def test_is_valid_coord_rejects_origin():
    assert _is_valid_coord(0.001, 0.001) is False


def test_is_valid_coord_rejects_out_of_range():
    assert _is_valid_coord(91.0, 0.0) is False
    assert _is_valid_coord(0.0, 181.0) is False
    assert _is_valid_coord(-91.0, 0.0) is False


def test_is_valid_coord_accepts_valid():
    assert _is_valid_coord(-33.8688, 151.2093) is True
    assert _is_valid_coord(51.5074, -0.1278) is True  # London


# ── inject_signals ──────────────────────────────────────────────────────────


def test_inject_signals_with_coords_and_addresses():
    signals = {
        "coordinates": [(-33.8688, 151.2093)],
        "addresses": ["123 Quarry Rd, Sydney"],
    }
    result = inject_signals("# Page", signals)
    assert "## Extracted Location Signals" in result
    assert "**Addresses:**" in result
    assert "123 Quarry Rd" in result
    assert "**Coordinates:**" in result
    assert "-33.868800" in result
    assert "# Page" in result


def test_inject_signals_coords_only():
    signals = {"coordinates": [(-33.8688, 151.2093)], "addresses": []}
    result = inject_signals("# Page", signals)
    assert "**Coordinates:**" in result
    assert "**Addresses:**" not in result


# ── extract_signals top-level ───────────────────────────────────────────────


def test_extract_signals_empty_html():
    result = extract_signals("")
    assert result == {"coordinates": [], "addresses": []}


def test_extract_signals_combined_sources():
    """Multiple signal types extracted from one HTML document."""
    html = """
    <meta name="geo.position" content="-33.8688;151.2093">
    <script type="application/ld+json">{
        "@type": "Place",
        "name": "Quarry",
        "address": "123 Quarry Rd, Sydney"
    }</script>
    <div data-lat="-27.4698" data-lng="153.0251"></div>
    """
    signals = extract_signals(html)
    assert len(signals["coordinates"]) >= 2
    assert len(signals["addresses"]) >= 1
```

- [ ] **Step 2: Run tests**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/web-scraper && uv run pytest tests/unit/test_signals.py -v`
Expected: All ~30 tests PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/web-scraper
git add tests/unit/test_signals.py
git commit -m "test: expand signal extraction tests to ~30 covering all extraction paths"
```

---

## Chunk 2: asset-search-v2 infrastructure + pure-logic unit tests

### Task 4: asset-search-v2 — test infrastructure

**Files:**
- Modify: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/pyproject.toml`
- Rewrite: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/conftest.py`
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/unit/__init__.py`
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/unit/conftest.py`
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/integration/__init__.py`
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/integration/conftest.py`
- Delete: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/test_pipeline.py`
- Delete: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/scripts/test_scrape_standalone.py`
- Delete: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/scripts/test_scrape_pipeline.py`

- [ ] **Step 1: Update pyproject.toml**

Add test deps and pytest config:

```toml
[project.optional-dependencies]
test = ["pytest>=8.0", "pytest-asyncio>=0.25.0", "respx>=0.22.0"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
markers = ["integration: hits real external services (Postgres, APIs)"]
```

- [ ] **Step 2: Create directory structure, remove stubs and ad-hoc scripts**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2
mkdir -p tests/unit tests/integration
touch tests/unit/__init__.py tests/integration/__init__.py
rm tests/test_pipeline.py
rm -f scripts/test_scrape_standalone.py scripts/test_scrape_pipeline.py
```

- [ ] **Step 3: Write tests/conftest.py (root)**

```python
"""Shared pytest configuration for asset-search-v2."""
```

- [ ] **Step 4: Write tests/unit/conftest.py**

```python
"""Unit test fixtures — all external dependencies mocked."""

from unittest.mock import MagicMock, AsyncMock

import pytest


@pytest.fixture
def mock_conn():
    """Mock psycopg.Connection with cursor context manager."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    conn._cursor = cursor  # easy access in tests
    return conn
```

- [ ] **Step 5: Write tests/integration/conftest.py**

```python
"""Integration test fixtures — real Postgres, test-scoped cleanup."""

import uuid

import psycopg
import pytest
from psycopg.rows import dict_row

from asset_search.config import Config


@pytest.fixture
def config():
    return Config()


@pytest.fixture
def db_conn(config):
    conn = psycopg.connect(config.corpgraph_db_url, row_factory=dict_row)
    yield conn
    conn.close()


@pytest.fixture
def test_issuer_id(db_conn):
    """Unique issuer_id for this test. Cleaned up on teardown."""
    issuer_id = f"test-{uuid.uuid4()}"
    yield issuer_id
    # Cleanup: delete all rows with this issuer_id from all tables.
    # Order matters due to FK: extraction_results → scraped_pages.
    with db_conn.cursor() as cur:
        for table in [
            "extraction_results",
            "scraped_pages",
            "discovered_urls",
            "discovered_assets",
            "qa_results",
        ]:
            cur.execute(f"DELETE FROM {table} WHERE issuer_id = %s", (issuer_id,))
    db_conn.commit()
```

- [ ] **Step 6: Verify empty test run**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2 && uv run pytest tests/ -v`
Expected: `no tests ran` (0 collected, no errors).

- [ ] **Step 7: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2
git add tests/ pyproject.toml
git rm -f scripts/test_scrape_standalone.py scripts/test_scrape_pipeline.py 2>/dev/null; true
git commit -m "test: set up unit/integration directory structure, add pytest config"
```

### Task 5: asset-search-v2 — test_config.py

**Files:**
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/unit/test_config.py`

- [ ] **Step 1: Write test_config.py**

```python
"""Tests for config.py — model conversion, resolution helpers, sub-module builders."""

import os
from unittest.mock import patch

import pytest

from asset_search.config import (
    _to_pydantic_ai_model,
    _resolve_str,
    _resolve_int,
    _resolve_float,
    _resolve_bool,
)


# ── _to_pydantic_ai_model ──────────────────────────────────────────────────


def test_to_pydantic_ai_model_bedrock():
    assert _to_pydantic_ai_model("bedrock/us.anthropic.claude-opus-4-6-v1") == "bedrock:us.anthropic.claude-opus-4-6-v1"


def test_to_pydantic_ai_model_openai():
    assert _to_pydantic_ai_model("openai/gpt-5") == "openai:gpt-5"


def test_to_pydantic_ai_model_anthropic():
    assert _to_pydantic_ai_model("anthropic/claude-opus-4-6") == "anthropic:claude-opus-4-6"


def test_to_pydantic_ai_model_litellm_fallback():
    assert _to_pydantic_ai_model("groq/llama-3-70b") == "litellm:groq/llama-3-70b"


def test_to_pydantic_ai_model_already_native():
    assert _to_pydantic_ai_model("bedrock:us.anthropic.claude-opus-4-6-v1") == "bedrock:us.anthropic.claude-opus-4-6-v1"


def test_to_pydantic_ai_model_bare_string():
    assert _to_pydantic_ai_model("gpt-5") == "gpt-5"


# ── _resolve_str ────────────────────────────────────────────────────────────


def test_resolve_str_env_wins():
    with patch.dict(os.environ, {"MY_KEY": "from_env"}):
        result = _resolve_str("MY_KEY", {"key": "from_toml"}, "key", "default")
        assert result == "from_env"


def test_resolve_str_toml_wins():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("MY_KEY", None)
        result = _resolve_str("MY_KEY", {"key": "from_toml"}, "key", "default")
        assert result == "from_toml"


def test_resolve_str_default_fallback():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("MY_KEY", None)
        result = _resolve_str("MY_KEY", {}, "key", "my_default")
        assert result == "my_default"


# ── _resolve_int / _resolve_float ───────────────────────────────────────────


def test_resolve_int_env_wins():
    with patch.dict(os.environ, {"MY_INT": "42"}):
        result = _resolve_int("MY_INT", {"val": 10}, "val", 1)
        assert result == 42


def test_resolve_float_env_wins():
    with patch.dict(os.environ, {"MY_FLOAT": "3.14"}):
        result = _resolve_float("MY_FLOAT", {"val": 1.0}, "val", 0.0)
        assert result == pytest.approx(3.14)


# ── _resolve_bool ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("val", ["true", "1", "yes", "True", "YES"])
def test_resolve_bool_true_strings(val):
    with patch.dict(os.environ, {"MY_BOOL": val}):
        assert _resolve_bool("MY_BOOL", {}, "b", False) is True


def test_resolve_bool_toml_layer():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("MY_BOOL", None)
        assert _resolve_bool("MY_BOOL", {"b": True}, "b", False) is True
        assert _resolve_bool("MY_BOOL", {"b": False}, "b", True) is False


# ── Sub-module config builders ──────────────────────────────────────────────


def test_scraper_config_builder():
    from asset_search.config import Config
    cfg = Config()
    sc = cfg.scraper_config()
    assert sc.base_url == cfg.scraper_base_url
    assert sc.batch_limit == cfg.scraper_batch_limit
    assert sc.strategy == cfg.scraper_strategy


def test_extractor_config_builder():
    from asset_search.config import Config
    cfg = Config()
    ec = cfg.extractor_config()
    assert ec.max_batch_tokens == cfg.extractor_max_batch_tokens
    assert ec.max_page_tokens == cfg.extractor_max_page_tokens
    assert ec.max_retries == cfg.extractor_max_retries
```

- [ ] **Step 2: Run tests**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2 && uv run pytest tests/unit/test_config.py -v`
Expected: All 15 tests PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2
git add tests/unit/test_config.py
git commit -m "test: add config resolution and model conversion tests"
```

### Task 6: asset-search-v2 — test_cost.py

**Files:**
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/unit/test_cost.py`

- [ ] **Step 1: Write test_cost.py**

```python
"""Tests for cost.py — model prefix stripping, cost tracking, pricing math."""

from types import SimpleNamespace

import pytest

from asset_search.cost import _strip_model_prefix, CostTracker


# ── _strip_model_prefix ────────────────────────────────────────────────────


def test_strip_model_prefix_bedrock():
    assert _strip_model_prefix("bedrock/us.anthropic.claude-opus-4-6-20250219-v1:0") == "anthropic.claude-opus-4-6-20250219-v1:0"


def test_strip_model_prefix_openai():
    assert _strip_model_prefix("openai/gpt-5") == "gpt-5"


def test_strip_model_prefix_no_prefix():
    assert _strip_model_prefix("gpt-5-mini") == "gpt-5-mini"


@pytest.mark.parametrize("region", ["us.", "global.", "eu.", "jp.", "apac."])
def test_strip_model_prefix_all_regions(region):
    model = f"bedrock/{region}anthropic.claude-opus-4-6-v1"
    assert _strip_model_prefix(model) == "anthropic.claude-opus-4-6-v1"


# ── track_llm ───────────────────────────────────────────────────────────────


def test_track_llm_accumulates():
    ct = CostTracker()
    ct.track_llm("model-a", 100, 50, stage="discover")
    ct.track_llm("model-a", 200, 100, stage="discover")
    assert ct.tokens_by_model["model-a"] == {"input": 300, "output": 150}
    assert ct.tokens_by_stage["discover"]["calls"] == 2
    assert ct.total_input_tokens == 300
    assert ct.total_output_tokens == 150


def test_track_llm_no_stage():
    ct = CostTracker()
    ct.track_llm("model-a", 100, 50)
    assert ct.tokens_by_model["model-a"] == {"input": 100, "output": 50}
    assert ct.tokens_by_stage == {}


# ── track_pydantic_ai ──────────────────────────────────────────────────────


def test_track_pydantic_ai_input_output():
    ct = CostTracker()
    usage = SimpleNamespace(input_tokens=100, output_tokens=50)
    ct.track_pydantic_ai(usage, model="m", stage="qa")
    assert ct.total_input_tokens == 100
    assert ct.total_output_tokens == 50


def test_track_pydantic_ai_request_response_fallback():
    ct = CostTracker()
    usage = SimpleNamespace(request_tokens=200, response_tokens=80)
    ct.track_pydantic_ai(usage, model="m", stage="qa")
    assert ct.total_input_tokens == 200
    assert ct.total_output_tokens == 80


def test_track_pydantic_ai_none_usage():
    ct = CostTracker()
    ct.track_pydantic_ai(None, model="m")
    assert ct.total_input_tokens == 0


# ── track_litellm ──────────────────────────────────────────────────────────


def test_track_litellm_with_usage():
    ct = CostTracker()
    resp = SimpleNamespace(usage=SimpleNamespace(prompt_tokens=300, completion_tokens=120))
    ct.track_litellm(resp, model="m", stage="merge")
    assert ct.total_input_tokens == 300
    assert ct.total_output_tokens == 120


def test_track_litellm_no_usage():
    ct = CostTracker()
    resp = SimpleNamespace()  # no .usage attribute
    ct.track_litellm(resp, model="m")
    assert ct.total_input_tokens == 0


# ── Cost calculations ──────────────────────────────────────────────────────


def test_llm_cost_usd_known_model():
    """Opus: $15/1M input, $75/1M output."""
    ct = CostTracker()
    ct.track_llm("bedrock/us.anthropic.claude-opus-4-6-20250219-v1:0", 1_000_000, 1_000_000)
    assert ct.llm_cost_usd() == pytest.approx(15.0 + 75.0)


def test_llm_cost_usd_unknown_model():
    """Unknown model falls back to hardcoded (0.25, 2.00) per 1M."""
    ct = CostTracker()
    ct.track_llm("unknown/mystery-model", 1_000_000, 1_000_000)
    assert ct.llm_cost_usd() == pytest.approx(0.25 + 2.00)


def test_api_cost_usd():
    ct = CostTracker()
    ct.track_crawl4ai(100)
    ct.track_exa(10)
    ct.track_embedding(1_000_000)
    ct.track_cohere_rerank(5)
    ct.track_firecrawl(20)
    expected = (
        100 * 0.001      # crawl4ai
        + 10 * 0.015     # exa
        + 1.0 * 0.02     # embedding (1M tokens)
        + 5 * 0.002      # cohere
        + 20 * 0.001     # firecrawl
    )
    assert ct.api_cost_usd() == pytest.approx(expected)


def test_track_firecrawl():
    ct = CostTracker()
    ct.track_firecrawl(5)
    assert ct.firecrawl_credits == 5
    ct.track_firecrawl(3)
    assert ct.firecrawl_credits == 8


def test_total_cost_usd():
    ct = CostTracker()
    ct.track_llm("gpt-5-mini", 1_000_000, 1_000_000)
    ct.track_crawl4ai(10)
    assert ct.total_cost_usd() == pytest.approx(ct.llm_cost_usd() + ct.api_cost_usd())


def test_total_cost_gbp():
    ct = CostTracker()
    ct.track_llm("gpt-5-mini", 1_000_000, 0)
    assert ct.total_cost_gbp() == pytest.approx(ct.total_cost_usd() * 0.741)


def test_summary_dict_keys():
    ct = CostTracker()
    summary = ct.summary()
    expected_keys = {
        "total_usd", "total_gbp", "llm_usd", "api_usd",
        "total_input_tokens", "total_output_tokens",
        "by_model", "by_stage",
        "crawl4ai_pages", "exa_searches", "embedding_tokens",
        "cohere_rerank_calls", "firecrawl_credits",
    }
    assert set(summary.keys()) == expected_keys
```

- [ ] **Step 2: Run tests**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2 && uv run pytest tests/unit/test_cost.py -v`
Expected: All 18 tests PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2
git add tests/unit/test_cost.py
git commit -m "test: add cost tracking and pricing math tests"
```

### Task 7: asset-search-v2 — test_models.py + test_db.py

**Files:**
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/unit/test_models.py`
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/unit/test_db.py`

- [ ] **Step 1: Write test_models.py**

```python
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
```

- [ ] **Step 2: Write test_db.py**

```python
"""Tests for db.py — hash functions and mocked DB operations."""

from unittest.mock import MagicMock, patch, call

import pytest

from asset_search.db import (
    url_hash,
    extraction_id,
    save_discovered_urls,
    get_cached_page,
    save_scraped_page,
    save_extraction_result,
    save_qa_report,
    save_discovered_assets,
)


# ── Hash functions ──────────────────────────────────────────────────────────


def test_url_hash_deterministic():
    h1 = url_hash("https://example.com/page")
    h2 = url_hash("https://example.com/page")
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex


def test_url_hash_different_urls():
    assert url_hash("https://a.com") != url_hash("https://b.com")


def test_extraction_id_deterministic():
    eid1 = extraction_id("page1", "gpt-5")
    eid2 = extraction_id("page1", "gpt-5")
    assert eid1 == eid2


def test_extraction_id_different_inputs():
    assert extraction_id("page1", "gpt-5") != extraction_id("page2", "gpt-5")
    assert extraction_id("page1", "gpt-5") != extraction_id("page1", "opus")


# ── save_discovered_urls ────────────────────────────────────────────────────


def test_save_discovered_urls_params(mock_conn):
    urls = [{"url": "https://example.com", "category": "facility_page", "notes": "test"}]
    count = save_discovered_urls(mock_conn, "issuer-1", urls)
    assert count == 1
    mock_conn.commit.assert_called_once()


def test_save_discovered_urls_empty(mock_conn):
    count = save_discovered_urls(mock_conn, "issuer-1", [])
    assert count == 0
    mock_conn.commit.assert_not_called()


# ── get_cached_page ─────────────────────────────────────────────────────────


def test_get_cached_page_fresh(mock_conn):
    row = {"page_id": "abc", "markdown": "# Hello", "url": "https://example.com"}
    mock_conn._cursor.fetchone.return_value = row
    result = get_cached_page(mock_conn, "https://example.com")
    assert result == row


def test_get_cached_page_miss(mock_conn):
    mock_conn._cursor.fetchone.return_value = None
    result = get_cached_page(mock_conn, "https://example.com")
    assert result is None


# ── save_scraped_page ───────────────────────────────────────────────────────


def test_save_scraped_page_returns_ids(mock_conn):
    pid, chash = save_scraped_page(
        mock_conn, "issuer-1", "https://example.com",
        "# Hello", "<h1>Hello</h1>", {"coordinates": []}, None,
    )
    assert pid == url_hash("https://example.com")
    assert len(chash) == 64
    mock_conn.commit.assert_called_once()


# ── save_extraction_result ──────────────────────────────────────────────────


def test_save_extraction_result_params(mock_conn):
    assets = [{"asset_name": "Quarry", "entity_name": "Boral"}]
    save_extraction_result(mock_conn, "page1", "issuer-1", "hash1", "gpt-5", assets)
    mock_conn.commit.assert_called_once()
    # Verify asset_count is len(assets)
    execute_call = mock_conn._cursor.execute.call_args
    params = execute_call[0][1]
    assert params[-1] == 1  # asset_count = len(assets)


# ── save_qa_report ──────────────────────────────────────────────────────────


def test_save_qa_report_params(mock_conn):
    report = {"quality_score": 0.85, "issues": []}
    save_qa_report(mock_conn, "issuer-1", report)
    # Should call CREATE TABLE and then INSERT
    mock_conn.execute.assert_called_once()  # CREATE TABLE
    mock_conn.commit.assert_called_once()


# ── save_discovered_assets ──────────────────────────────────────────────────


def test_save_discovered_assets_with_geom(mock_conn):
    assets = [{"asset_name": "Quarry", "latitude": -33.8688, "longitude": 151.2093, "asset_id": "a1"}]
    count = save_discovered_assets(mock_conn, "issuer-1", assets)
    assert count == 1
    sql = mock_conn._cursor.execute.call_args[0][0]
    assert "ST_MakePoint" in sql


def test_save_discovered_assets_without_geom(mock_conn):
    assets = [{"asset_name": "Office", "asset_id": "a2"}]
    count = save_discovered_assets(mock_conn, "issuer-1", assets)
    assert count == 1
    sql = mock_conn._cursor.execute.call_args[0][0]
    assert "NULL" in sql
```

- [ ] **Step 3: Run tests**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2 && uv run pytest tests/unit/test_models.py tests/unit/test_db.py -v`
Expected: All ~18 tests PASS.

- [ ] **Step 4: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2
git add tests/unit/test_models.py tests/unit/test_db.py
git commit -m "test: add model and DB helper unit tests"
```

---

## Chunk 3: asset-search-v2 stage unit tests

### Task 8: asset-search-v2 — test_scrape.py (unit)

**Files:**
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/unit/test_scrape.py`

- [ ] **Step 1: Write test_scrape.py**

```python
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
    assert cfg.use_proxy is True


def test_config_from_notes_wait_for():
    cfg = _config_from_notes("wait_for:.locations-list")
    assert cfg.wait_for == ".locations-list"


def test_config_from_notes_combined():
    cfg = _config_from_notes("waf_blocked wait_for:.results")
    assert cfg.use_proxy is True
    assert cfg.wait_for == ".results"


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
```

- [ ] **Step 2: Run tests**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2 && uv run pytest tests/unit/test_scrape.py -v`
Expected: All 9 tests PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2
git add tests/unit/test_scrape.py
git commit -m "test: add scrape stage unit tests with mocked DB and scraper"
```

### Task 9: asset-search-v2 — test_extract.py (unit)

**Files:**
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/unit/test_extract.py`

- [ ] **Step 1: Write test_extract.py**

```python
"""Tests for stages/extract.py — extraction orchestration with mocked deps."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from asset_search.models import Asset
from asset_search.cost import CostTracker
from asset_search.stages.extract import run_extract


def _mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


@pytest.mark.asyncio
@patch("asset_search.stages.extract.get_connection")
@patch("asset_search.stages.extract.get_extraction_result")
@patch("asset_search.stages.extract.extract", new_callable=AsyncMock)
@patch("asset_search.stages.extract.save_extraction_result")
@patch("asset_search.stages.extract.show_stage")
async def test_run_extract_cache_hit(mock_show, mock_save, mock_extract, mock_get_result, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_get_result.return_value = {
        "assets_json": [{"asset_name": "Quarry", "entity_name": "Boral"}]
    }
    pages = [{"url": "https://a.com", "page_id": "p1", "markdown": "# Page"}]
    from asset_search.config import Config
    assets = await run_extract("issuer-1", "Boral", pages, Config())
    assert len(assets) == 1
    assert assets[0].asset_name == "Quarry"
    mock_extract.assert_not_called()


@pytest.mark.asyncio
@patch("asset_search.stages.extract.get_connection")
@patch("asset_search.stages.extract.get_extraction_result")
@patch("asset_search.stages.extract.extract", new_callable=AsyncMock)
@patch("asset_search.stages.extract.save_extraction_result")
@patch("asset_search.stages.extract.show_stage")
async def test_run_extract_cache_dedup(mock_show, mock_save, mock_extract, mock_get_result, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    # Two pages with same cached asset — should dedup
    cached = {"assets_json": [{"asset_name": "Quarry", "entity_name": "Boral"}]}
    mock_get_result.return_value = cached
    pages = [
        {"url": "https://a.com", "page_id": "p1", "markdown": "# A"},
        {"url": "https://b.com", "page_id": "p2", "markdown": "# B"},
    ]
    from asset_search.config import Config
    assets = await run_extract("issuer-1", "Boral", pages, Config())
    assert len(assets) == 1  # deduped by (name, entity)


@pytest.mark.asyncio
@patch("asset_search.stages.extract.get_connection")
@patch("asset_search.stages.extract.get_extraction_result", return_value=None)
@patch("asset_search.stages.extract.extract", new_callable=AsyncMock)
@patch("asset_search.stages.extract.save_extraction_result")
@patch("asset_search.stages.extract.show_stage")
async def test_run_extract_calls_extractor(mock_show, mock_save, mock_extract, mock_get_result, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_extract.return_value = [Asset(asset_name="Plant", entity_name="Boral")]
    pages = [{"url": "https://a.com", "page_id": "p1", "markdown": "# Content", "content_hash": "h1"}]
    from asset_search.config import Config
    assets = await run_extract("issuer-1", "Boral", pages, Config())
    assert len(assets) == 1
    # Verify prompt contains company name
    call_kwargs = mock_extract.call_args[1]
    assert "Boral" in call_kwargs["prompt"]


@pytest.mark.asyncio
@patch("asset_search.stages.extract.get_connection")
@patch("asset_search.stages.extract.get_extraction_result", return_value=None)
@patch("asset_search.stages.extract.extract", new_callable=AsyncMock)
@patch("asset_search.stages.extract.save_extraction_result")
@patch("asset_search.stages.extract.show_stage")
async def test_run_extract_prompt_includes_ald_summary(mock_show, mock_save, mock_extract, mock_get_result, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_extract.return_value = []
    pages = [{"url": "https://a.com", "page_id": "p1", "markdown": "# Content", "content_hash": "h1"}]
    from asset_search.config import Config
    await run_extract("issuer-1", "Boral", pages, Config(), existing_assets_summary="Known: Quarry X")
    call_kwargs = mock_extract.call_args[1]
    assert "Known: Quarry X" in call_kwargs["prompt"]


@pytest.mark.asyncio
@patch("asset_search.stages.extract.get_connection")
@patch("asset_search.stages.extract.get_extraction_result", return_value=None)
@patch("asset_search.stages.extract.extract", new_callable=AsyncMock)
@patch("asset_search.stages.extract.save_extraction_result")
@patch("asset_search.stages.extract.show_stage")
async def test_run_extract_saves_per_page(mock_show, mock_save, mock_extract, mock_get_result, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    mock_extract.return_value = [Asset(asset_name="Plant", entity_name="Boral")]
    pages = [
        {"url": "https://a.com", "page_id": "p1", "markdown": "# A", "content_hash": "h1"},
        {"url": "https://b.com", "page_id": "p2", "markdown": "# B", "content_hash": "h2"},
    ]
    from asset_search.config import Config
    await run_extract("issuer-1", "Boral", pages, Config())
    assert mock_save.call_count == 2  # saved once per page


@pytest.mark.asyncio
@patch("asset_search.stages.extract.get_connection")
@patch("asset_search.stages.extract.get_extraction_result", return_value=None)
@patch("asset_search.stages.extract.extract", new_callable=AsyncMock)
@patch("asset_search.stages.extract.save_extraction_result")
@patch("asset_search.stages.extract.show_stage")
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
    from asset_search.config import Config
    await run_extract("issuer-1", "Boral", pages, Config(), costs=costs)
    assert "extract" in costs.tokens_by_stage
    assert costs.tokens_by_stage["extract"]["input"] == 500


@pytest.mark.asyncio
@patch("asset_search.stages.extract.get_connection")
@patch("asset_search.stages.extract.show_stage")
async def test_run_extract_empty_pages(mock_show, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    from asset_search.config import Config
    assets = await run_extract("issuer-1", "Boral", [], Config())
    assert assets == []
```

- [ ] **Step 2: Run tests**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2 && uv run pytest tests/unit/test_extract.py -v`
Expected: All 7 tests PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2
git add tests/unit/test_extract.py
git commit -m "test: add extract stage unit tests with mocked DB and extractor"
```

### Task 10: asset-search-v2 — test_merge.py (unit)

**Files:**
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/unit/test_merge.py`

- [ ] **Step 1: Write test_merge.py**

```python
"""Tests for stages/merge.py — dedup, classification, error handling."""

import json
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from asset_search.models import Asset
from asset_search.cost import CostTracker
from asset_search.stages.merge import run_merge


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
@patch("asset_search.stages.merge.get_connection")
@patch("asset_search.stages.merge.show_stage")
async def test_run_merge_empty(mock_show, mock_get_conn):
    from asset_search.config import Config
    result = await run_merge("issuer-1", [], Config())
    assert result == []
    mock_get_conn.assert_not_called()


@pytest.mark.asyncio
@patch("asset_search.stages.merge.get_connection")
@patch("asset_search.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_search.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_search.stages.merge.litellm")
@patch("asset_search.stages.merge.show_stage")
async def test_run_merge_assigns_uuid(mock_show, mock_litellm, mock_save, mock_get_assets, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    asset_data = [{"asset_name": "Quarry", "entity_name": "Boral", "asset_id": ""}]
    mock_litellm.acompletion = AsyncMock(return_value=_make_llm_response(asset_data))
    assets = [_make_asset()]
    from asset_search.config import Config
    result = await run_merge("issuer-1", assets, Config())
    assert len(result) >= 1
    assert result[0].asset_id != ""  # UUID assigned


@pytest.mark.asyncio
@patch("asset_search.stages.merge.get_connection")
@patch("asset_search.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_search.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_search.stages.merge.litellm")
@patch("asset_search.stages.merge.show_stage")
async def test_run_merge_sets_metadata(mock_show, mock_litellm, mock_save, mock_get_assets, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    asset_data = [{"asset_name": "Quarry", "entity_name": "Boral"}]
    mock_litellm.acompletion = AsyncMock(return_value=_make_llm_response(asset_data))
    assets = [_make_asset()]
    from asset_search.config import Config
    result = await run_merge("issuer-1", assets, Config(), industry_code="B0810")
    assert result[0].industry_code == "B0810"
    assert result[0].attribution_source == "asset_search"
    assert result[0].date_researched == date.today().isoformat()


@pytest.mark.asyncio
@patch("asset_search.stages.merge.get_connection")
@patch("asset_search.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_search.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_search.stages.merge.litellm")
@patch("asset_search.stages.merge.show_stage")
async def test_run_merge_batching(mock_show, mock_litellm, mock_save, mock_get_assets, mock_get_conn):
    """51+ assets should trigger 2 _merge_batch calls (batch_size=50)."""
    mock_get_conn.return_value = _mock_conn()
    asset_data = [{"asset_name": f"Asset {i}", "entity_name": "Boral"} for i in range(51)]
    mock_litellm.acompletion = AsyncMock(return_value=_make_llm_response(asset_data[:50]))
    assets = [_make_asset(name=f"Asset {i}") for i in range(51)]
    from asset_search.config import Config
    await run_merge("issuer-1", assets, Config())
    # 2 batch calls + 1 final dedup = 3 total acompletion calls
    assert mock_litellm.acompletion.call_count >= 2


@pytest.mark.asyncio
@patch("asset_search.stages.merge.get_connection")
@patch("asset_search.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_search.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_search.stages.merge._final_dedup", new_callable=AsyncMock)
@patch("asset_search.stages.merge._merge_batch", new_callable=AsyncMock)
@patch("asset_search.stages.merge.show_stage")
async def test_run_merge_final_dedup_called(mock_show, mock_merge_batch, mock_final_dedup, mock_save, mock_get_assets, mock_get_conn):
    """_final_dedup is called when result has >1 asset."""
    mock_get_conn.return_value = _mock_conn()
    mock_merge_batch.return_value = [_make_asset(name="A"), _make_asset(name="B")]
    mock_final_dedup.return_value = [_make_asset(name="A"), _make_asset(name="B")]
    assets = [_make_asset(name="A"), _make_asset(name="B")]
    from asset_search.config import Config
    await run_merge("issuer-1", assets, Config())
    mock_final_dedup.assert_called_once()


@pytest.mark.asyncio
@patch("asset_search.stages.merge.get_connection")
@patch("asset_search.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_search.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_search.stages.merge.litellm")
@patch("asset_search.stages.merge.show_stage")
async def test_run_merge_dedup_by_id(mock_show, mock_litellm, mock_save, mock_get_assets, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    # LLM returns same asset_id in both calls — should dedup
    asset_data = [{"asset_name": "Quarry", "entity_name": "Boral", "asset_id": "dup-id"}]
    mock_litellm.acompletion = AsyncMock(return_value=_make_llm_response(asset_data))
    # Create 51 assets to trigger 2 batches (batch_size=50)
    assets = [_make_asset(name=f"Asset {i}") for i in range(51)]
    from asset_search.config import Config
    result = await run_merge("issuer-1", assets, Config())
    # Despite 2 batches returning "dup-id", only 1 asset in final result
    id_count = sum(1 for a in result if a.asset_id == "dup-id")
    assert id_count == 1


@pytest.mark.asyncio
@patch("asset_search.stages.merge.get_connection")
@patch("asset_search.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_search.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_search.stages.merge.litellm")
@patch("asset_search.stages.merge.show_stage")
async def test_run_merge_cost_tracking(mock_show, mock_litellm, mock_save, mock_get_assets, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    asset_data = [{"asset_name": "Quarry", "entity_name": "Boral"}]
    mock_litellm.acompletion = AsyncMock(return_value=_make_llm_response(asset_data))
    assets = [_make_asset()]
    costs = CostTracker()
    from asset_search.config import Config
    await run_merge("issuer-1", assets, Config(), costs=costs)
    assert costs.tokens_by_stage.get("merge", {}).get("calls", 0) >= 1


@pytest.mark.asyncio
@patch("asset_search.stages.merge.get_connection")
@patch("asset_search.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_search.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_search.stages.merge.litellm")
@patch("asset_search.stages.merge.show_stage")
async def test_run_merge_batch_llm_error_fallback(mock_show, mock_litellm, mock_save, mock_get_assets, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
    # LLM returns invalid JSON
    bad_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="not json"))],
        usage=SimpleNamespace(prompt_tokens=0, completion_tokens=0),
    )
    mock_litellm.acompletion = AsyncMock(return_value=bad_response)
    assets = [_make_asset(name="Plant")]
    from asset_search.config import Config
    result = await run_merge("issuer-1", assets, Config())
    # Fallback: returns original batch unchanged
    assert len(result) >= 1
    assert result[0].asset_name == "Plant"


@pytest.mark.asyncio
@patch("asset_search.stages.merge.get_connection")
@patch("asset_search.stages.merge.get_discovered_assets", return_value=[])
@patch("asset_search.stages.merge.save_discovered_assets", return_value=1)
@patch("asset_search.stages.merge.litellm")
@patch("asset_search.stages.merge.show_stage")
async def test_run_merge_final_dedup_error_fallback(mock_show, mock_litellm, mock_save, mock_get_assets, mock_get_conn):
    mock_get_conn.return_value = _mock_conn()
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
    from asset_search.config import Config
    result = await run_merge("issuer-1", assets, Config())
    # Final dedup fails gracefully — returns assets from merge batch
    assert len(result) == 2
```

- [ ] **Step 2: Run tests**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2 && uv run pytest tests/unit/test_merge.py -v`
Expected: All 8 tests PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2
git add tests/unit/test_merge.py
git commit -m "test: add merge stage unit tests with mocked LLM and DB"
```

---

## Chunk 4: Integration tests (both repos) + code change

### Task 11: asset-search-v2 — add `get_qa_report` to db.py

**Files:**
- Modify: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/src/asset_search/db.py`

- [ ] **Step 1: Add get_qa_report function**

Add after `save_qa_report` in `db.py` (after line 131):

```python
def get_qa_report(conn: psycopg.Connection, issuer_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM qa_results WHERE issuer_id = %s", (issuer_id,))
        return cur.fetchone()
```

- [ ] **Step 2: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2
git add src/asset_search/db.py
git commit -m "feat: add get_qa_report DB helper for test round-trip coverage"
```

### Task 12: web-scraper — integration tests (real Crawl4AI API)

**Files:**
- Create: `/Users/nikolai.tennant/Documents/GitHub/web-scraper/tests/integration/test_scraper_live.py`

- [ ] **Step 1: Write test_scraper_live.py**

```python
"""Integration tests — real Crawl4AI API with Boral URLs.

Run: CRAWL4AI_API_KEY=... pytest -m integration tests/integration/
Override URL count: INTEGRATION_TEST_URLS=20 pytest -m integration
"""

import pytest

from web_scraper import scrape, ScraperConfig, Usage


pytestmark = pytest.mark.integration


async def test_scrape_single_url_live(crawl4ai_api_key, boral_urls):
    url = boral_urls[0]
    pages = await scrape(urls=[url], api_key=crawl4ai_api_key)
    assert len(pages) == 1
    page = pages[0]
    assert page.success is True
    assert len(page.markdown) > 0
    assert len(page.raw_html) > 0
    assert page.status_code == 200


async def test_scrape_batch_live(crawl4ai_api_key, boral_urls, integration_url_count):
    n = min(integration_url_count, 10, len(boral_urls))  # batch endpoint handles up to 10
    urls = boral_urls[:n]
    pages = await scrape(urls=urls, api_key=crawl4ai_api_key)
    assert len(pages) == n
    success_count = sum(1 for p in pages if p.success)
    assert success_count > 0


async def test_scrape_async_chunk_live(crawl4ai_api_key, boral_urls, integration_url_count):
    """Force async path by setting batch_limit=2."""
    n = min(max(integration_url_count, 5), len(boral_urls))
    urls = boral_urls[:n]
    config = ScraperConfig(batch_limit=2)
    pages = await scrape(urls=urls, api_key=crawl4ai_api_key, scraper_config=config)
    assert len(pages) == n
    success_count = sum(1 for p in pages if p.success)
    assert success_count > 0


async def test_scrape_signals_extracted(crawl4ai_api_key, boral_urls):
    """At least one Boral page should yield coordinates or addresses."""
    urls = boral_urls[:3]
    pages = await scrape(urls=urls, api_key=crawl4ai_api_key)
    has_signals = any(
        p.signals.get("coordinates") or p.signals.get("addresses")
        for p in pages if p.success
    )
    assert has_signals, "Expected at least one page to have extracted signals"


async def test_scrape_usage_tracking(crawl4ai_api_key, boral_urls):
    urls = boral_urls[:3]
    usage = Usage()
    pages = await scrape(urls=urls, api_key=crawl4ai_api_key, usage=usage)
    expected = sum(1 for p in pages if p.success)
    assert usage.pages_crawled == expected


async def test_scrape_failure_handling(crawl4ai_api_key):
    pages = await scrape(
        urls=["https://this-domain-does-not-exist-abc123.com"],
        api_key=crawl4ai_api_key,
    )
    assert len(pages) == 1
    assert pages[0].success is False
```

- [ ] **Step 2: Run integration tests (requires CRAWL4AI_API_KEY)**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/web-scraper && uv run pytest tests/integration/ -v -m integration`
Expected: All 6 tests PASS (may take 30-60s).

- [ ] **Step 3: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/web-scraper
git add tests/integration/test_scraper_live.py
git commit -m "test: add Crawl4AI integration tests with real Boral URLs"
```

### Task 13: asset-search-v2 — integration tests (real Postgres)

**Files:**
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/integration/test_db_live.py`
- Create: `/Users/nikolai.tennant/Documents/GitHub/asset-search-v2/tests/integration/test_scrape_live.py`

- [ ] **Step 1: Write test_db_live.py**

```python
"""Integration tests for db.py — real Postgres round-trips.

Requires: local Postgres with asset_search DB and tables initialized.
Run: pytest -m integration tests/integration/test_db_live.py
"""

import time

import pytest

from asset_search.db import (
    url_hash,
    save_discovered_urls,
    get_discovered_urls,
    save_scraped_page,
    get_cached_page,
    save_extraction_result,
    get_extraction_result,
    save_discovered_assets,
    get_discovered_assets,
    save_qa_report,
    get_qa_report,
)

pytestmark = pytest.mark.integration


def test_save_and_get_discovered_urls(db_conn, test_issuer_id):
    urls = [
        {"url": "https://test-a.com", "category": "facility_page", "notes": "test"},
        {"url": "https://test-b.com", "category": "corporate_page"},
    ]
    count = save_discovered_urls(db_conn, test_issuer_id, urls)
    assert count == 2

    rows = get_discovered_urls(db_conn, test_issuer_id)
    assert len(rows) == 2
    saved_urls = {r["url"] for r in rows}
    assert "https://test-a.com" in saved_urls
    assert "https://test-b.com" in saved_urls


def test_save_and_get_scraped_page(db_conn, test_issuer_id):
    url = "https://test-scrape.com"
    pid, chash = save_scraped_page(
        db_conn, test_issuer_id, url,
        "# Hello", "<h1>Hello</h1>", {"coordinates": []}, None,
        stale_days=30,
    )
    assert pid == url_hash(url)

    cached = get_cached_page(db_conn, url)
    assert cached is not None
    assert cached["markdown"] == "# Hello"


def test_scraped_page_staleness(db_conn, test_issuer_id):
    url = "https://test-stale.com"
    save_scraped_page(
        db_conn, test_issuer_id, url,
        "# Stale", "<h1>Stale</h1>", None, None,
        stale_days=0,  # expires immediately
    )
    time.sleep(0.1)
    cached = get_cached_page(db_conn, url)
    assert cached is None


def test_save_and_get_extraction_result(db_conn, test_issuer_id):
    # Need a scraped page first (FK constraint)
    url = "https://test-extract.com"
    pid, chash = save_scraped_page(
        db_conn, test_issuer_id, url,
        "# Content", "<h1>Content</h1>", None, None,
    )

    assets = [{"asset_name": "Quarry", "entity_name": "Boral"}]
    save_extraction_result(db_conn, pid, test_issuer_id, chash, "test-model", assets)

    result = get_extraction_result(db_conn, pid, "test-model")
    assert result is not None
    assert result["assets_json"] == assets


def test_extraction_cache_invalidation(db_conn, test_issuer_id):
    url = "https://test-invalidate.com"
    pid, chash1 = save_scraped_page(
        db_conn, test_issuer_id, url,
        "# V1", "<h1>V1</h1>", None, None,
    )
    save_extraction_result(db_conn, pid, test_issuer_id, chash1, "test-model", [])

    # Update the page content → new content_hash
    save_scraped_page(
        db_conn, test_issuer_id, url,
        "# V2", "<h1>V2</h1>", None, None,
    )

    # Extraction cache should miss (content_hash changed)
    result = get_extraction_result(db_conn, pid, "test-model")
    assert result is None


def test_save_and_get_discovered_assets(db_conn, test_issuer_id):
    assets = [{
        "asset_id": "test-asset-1",
        "asset_name": "Test Quarry",
        "entity_name": "Boral",
        "address": "123 Test Rd",
        "latitude": -33.8688,
        "longitude": 151.2093,
        "asset_type_raw": "quarry",
        "status": "Operating",
    }]
    count = save_discovered_assets(db_conn, test_issuer_id, assets)
    assert count == 1

    rows = get_discovered_assets(db_conn, test_issuer_id)
    assert len(rows) == 1
    assert rows[0]["asset_name"] == "Test Quarry"


def test_save_and_get_qa_report(db_conn, test_issuer_id):
    report = {"quality_score": 0.85, "issues": ["Missing QLD assets"]}
    save_qa_report(db_conn, test_issuer_id, report)

    result = get_qa_report(db_conn, test_issuer_id)
    assert result is not None
    assert result["report"]["quality_score"] == 0.85


def test_upsert_overwrites(db_conn, test_issuer_id):
    urls = [{"url": "https://test-upsert.com", "category": "facility_page"}]
    save_discovered_urls(db_conn, test_issuer_id, urls)

    # Upsert with new category
    urls[0]["category"] = "corporate_page"
    save_discovered_urls(db_conn, test_issuer_id, urls)

    rows = get_discovered_urls(db_conn, test_issuer_id)
    assert len(rows) == 1
    assert rows[0]["category"] == "corporate_page"
```

- [ ] **Step 2: Write test_scrape_live.py**

```python
"""Integration tests for scrape stage — real Postgres + mocked scraper.

Run: pytest -m integration tests/integration/test_scrape_live.py
"""

from unittest.mock import AsyncMock, patch

import pytest

from asset_search.cost import CostTracker
from asset_search.db import get_cached_page
from asset_search.stages.scrape import run_scrape
from web_scraper import ScrapedPage

pytestmark = pytest.mark.integration


def _make_page(url):
    return ScrapedPage(
        url=url, markdown="# Test Page", raw_html="<h1>Test Page</h1>",
        success=True, status_code=200,
    )


@patch("asset_search.stages.scrape.scrape", new_callable=AsyncMock)
async def test_run_scrape_end_to_end(mock_scrape, config, db_conn, test_issuer_id):
    mock_scrape.return_value = [_make_page("https://test-e2e.com")]
    urls = [{"url": "https://test-e2e.com", "category": "facility_page"}]

    pages = await run_scrape(test_issuer_id, urls, config)

    assert len(pages) == 1
    assert pages[0]["markdown"] == "# Test Page"
    # Verify it was saved to DB
    cached = get_cached_page(db_conn, "https://test-e2e.com")
    assert cached is not None


@patch("asset_search.stages.scrape.scrape", new_callable=AsyncMock)
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


@patch("asset_search.stages.scrape.scrape", new_callable=AsyncMock)
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
```

- [ ] **Step 3: Run integration tests (requires local Postgres)**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2 && uv run pytest tests/integration/ -v -m integration`
Expected: All 11 tests PASS.

- [ ] **Step 4: Commit**

```bash
cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2
git add tests/integration/test_db_live.py tests/integration/test_scrape_live.py
git commit -m "test: add Postgres integration tests for DB helpers and scrape stage"
```

### Task 14: Final verification — run all tests in both repos

- [ ] **Step 1: Run all web-scraper tests**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/web-scraper && uv run pytest tests/ -v -m "not integration"`
Expected: All unit tests PASS (~44 tests).

- [ ] **Step 2: Run all asset-search-v2 unit tests**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2 && uv run pytest tests/unit/ -v`
Expected: All unit tests PASS (~57 tests).

- [ ] **Step 3: Run asset-search-v2 integration tests (if DB is available)**

Run: `cd /Users/nikolai.tennant/Documents/GitHub/asset-search-v2 && uv run pytest tests/integration/ -v -m integration`
Expected: All 11 integration tests PASS.

- [ ] **Step 4: Commit any fixes needed**
