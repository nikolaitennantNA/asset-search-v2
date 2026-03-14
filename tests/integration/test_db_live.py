"""Integration tests for db.py — real Postgres round-trips.

Requires: local Postgres with asset_discovery DB and tables initialized.
Run: pytest -m integration tests/integration/test_db_live.py
"""

import time

import pytest

from asset_discovery.db import (
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
    # Use issuer_id in URLs to guarantee uniqueness across test runs
    urls = [
        {"url": f"https://{test_issuer_id}-a.com", "category": "facility_page", "notes": "test"},
        {"url": f"https://{test_issuer_id}-b.com", "category": "corporate_page"},
    ]
    count = save_discovered_urls(db_conn, test_issuer_id, urls)
    assert count == 2

    rows = get_discovered_urls(db_conn, test_issuer_id)
    assert len(rows) == 2
    saved_urls = {r["url"] for r in rows}
    assert f"https://{test_issuer_id}-a.com" in saved_urls
    assert f"https://{test_issuer_id}-b.com" in saved_urls


def test_save_and_get_scraped_page(db_conn, test_issuer_id):
    url = f"https://{test_issuer_id}-scrape.com"
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
    url = f"https://{test_issuer_id}-stale.com"
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
    url = f"https://{test_issuer_id}-extract.com"
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
    url = f"https://{test_issuer_id}-invalidate.com"
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
        "asset_id": f"{test_issuer_id}-asset-1",
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
    urls = [{"url": f"https://{test_issuer_id}-upsert.com", "category": "facility_page"}]
    save_discovered_urls(db_conn, test_issuer_id, urls)

    # Upsert with new category
    urls[0]["category"] = "corporate_page"
    save_discovered_urls(db_conn, test_issuer_id, urls)

    rows = get_discovered_urls(db_conn, test_issuer_id)
    assert len(rows) == 1
    assert rows[0]["category"] == "corporate_page"
