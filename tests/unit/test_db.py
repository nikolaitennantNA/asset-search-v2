"""Tests for db.py — hash functions and mocked DB operations."""

from unittest.mock import MagicMock, patch, call

import pytest

from asset_discovery.db import (
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
    urls = [
        {"url": "https://a.com", "category": "facility_page",
         "notes": "React SPA", "strategy": "browser", "proxy_mode": "auto"},
        {"url": "https://b.com", "category": "pdf", "notes": "annual report"},
    ]
    count = save_discovered_urls(mock_conn, "issuer-1", urls)
    assert count == 2
    assert mock_conn.cursor().__enter__().execute.call_count == 2


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
