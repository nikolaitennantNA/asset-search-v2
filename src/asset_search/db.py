"""Psycopg helpers for the 4 asset-search cache tables in corp-graph Postgres."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

from .config import Config


def get_connection(config: Config | None = None) -> psycopg.Connection:
    cfg = config or Config()
    return psycopg.connect(cfg.corpgraph_db_url, row_factory=dict_row)


def url_hash(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()


def extraction_id(page_id: str, model: str) -> str:
    return hashlib.sha256(f"{page_id}:{model}".encode()).hexdigest()


def get_discovered_urls(conn: psycopg.Connection, issuer_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM discovered_urls WHERE issuer_id = %s", (issuer_id,))
        return cur.fetchall()


def save_discovered_urls(conn: psycopg.Connection, issuer_id: str, urls: list[dict[str, Any]]) -> int:
    if not urls:
        return 0
    with conn.cursor() as cur:
        for u in urls:
            cur.execute(
                """INSERT INTO discovered_urls (url_hash, url, issuer_id, category, notes)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (url_hash) DO UPDATE SET category = EXCLUDED.category, notes = EXCLUDED.notes""",
                (url_hash(u["url"]), u["url"], issuer_id, u["category"], u.get("notes")),
            )
    conn.commit()
    return len(urls)


def get_cached_page(conn: psycopg.Connection, url: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM scraped_pages WHERE page_id = %s AND stale_after > NOW()", (url_hash(url),))
        return cur.fetchone()


def save_scraped_page(
    conn: psycopg.Connection, issuer_id: str, url: str,
    markdown: str, raw_html: str, signals: dict | None, tokens: int | None,
    stale_days: int = 30,
) -> tuple[str, str]:
    """Save a scraped page. Returns (page_id, content_hash)."""
    pid = url_hash(url)
    content_hash = hashlib.sha256(markdown.encode()).hexdigest()
    stale_after = datetime.now(timezone.utc) + timedelta(days=stale_days)
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO scraped_pages
               (page_id, url, issuer_id, content_hash, markdown, raw_html, signals, tokens, scraped_at, stale_after)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
               ON CONFLICT (page_id) DO UPDATE SET
                 content_hash = EXCLUDED.content_hash, markdown = EXCLUDED.markdown,
                 raw_html = EXCLUDED.raw_html, signals = EXCLUDED.signals,
                 tokens = EXCLUDED.tokens, scraped_at = EXCLUDED.scraped_at,
                 stale_after = EXCLUDED.stale_after""",
            (pid, url, issuer_id, content_hash, markdown, raw_html,
             psycopg.types.json.Json(signals) if signals else None, tokens, stale_after),
        )
    conn.commit()
    return pid, content_hash


def get_extraction_result(conn: psycopg.Connection, page_id: str, model: str) -> dict[str, Any] | None:
    eid = extraction_id(page_id, model)
    with conn.cursor() as cur:
        cur.execute(
            """SELECT er.* FROM extraction_results er
               JOIN scraped_pages sp ON er.page_id = sp.page_id
               WHERE er.extraction_id = %s AND er.content_hash = sp.content_hash""",
            (eid,),
        )
        return cur.fetchone()


def save_extraction_result(
    conn: psycopg.Connection, page_id: str, issuer_id: str,
    content_hash: str, model: str, assets_json: list[dict],
) -> None:
    eid = extraction_id(page_id, model)
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO extraction_results
               (extraction_id, page_id, issuer_id, content_hash, model, assets_json, asset_count, extracted_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
               ON CONFLICT (extraction_id) DO UPDATE SET
                 content_hash = EXCLUDED.content_hash, assets_json = EXCLUDED.assets_json,
                 asset_count = EXCLUDED.asset_count, extracted_at = EXCLUDED.extracted_at""",
            (eid, page_id, issuer_id, content_hash, model,
             psycopg.types.json.Json(assets_json), len(assets_json)),
        )
    conn.commit()


def save_qa_report(
    conn: psycopg.Connection, issuer_id: str, report_json: dict[str, Any],
) -> None:
    """Upsert QA report (including coverage flags) for an issuer."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS qa_results (
               issuer_id TEXT PRIMARY KEY,
               report JSONB NOT NULL,
               created_at TIMESTAMPTZ DEFAULT NOW()
           )""",
    )
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO qa_results (issuer_id, report, created_at)
               VALUES (%s, %s, NOW())
               ON CONFLICT (issuer_id) DO UPDATE SET
                 report = EXCLUDED.report, created_at = EXCLUDED.created_at""",
            (issuer_id, psycopg.types.json.Json(report_json)),
        )
    conn.commit()


def get_qa_report(conn: psycopg.Connection, issuer_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM qa_results WHERE issuer_id = %s", (issuer_id,))
        return cur.fetchone()


def get_discovered_assets(conn: psycopg.Connection, issuer_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM discovered_assets WHERE issuer_id = %s", (issuer_id,))
        return cur.fetchall()


def save_discovered_assets(conn: psycopg.Connection, issuer_id: str, assets: list[dict[str, Any]]) -> int:
    if not assets:
        return 0
    with conn.cursor() as cur:
        for a in assets:
            lat, lon = a.get("latitude"), a.get("longitude")
            geom_expr = "ST_SetSRID(ST_MakePoint(%s, %s), 4326)" if lat and lon else "NULL"
            geom_params = (lon, lat) if lat and lon else ()
            cur.execute(
                f"""INSERT INTO discovered_assets
                   (asset_id, issuer_id, asset_name, entity_name, entity_isin,
                    parent_name, parent_isin, address, latitude, longitude,
                    geom, asset_type_raw, naturesense_asset_type, industry_code,
                    status, capacity, capacity_units, entity_stake_pct,
                    source_url, domain_source, supplementary_details,
                    date_researched, attribution_source, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    {geom_expr}, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                   ON CONFLICT (asset_id) DO UPDATE SET
                     asset_name=EXCLUDED.asset_name, address=EXCLUDED.address,
                     latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,
                     geom=EXCLUDED.geom, asset_type_raw=EXCLUDED.asset_type_raw,
                     naturesense_asset_type=EXCLUDED.naturesense_asset_type,
                     status=EXCLUDED.status, updated_at=NOW()""",
                (a.get("asset_id"), issuer_id, a["asset_name"], a.get("entity_name"),
                 a.get("entity_isin"), a.get("parent_name"), a.get("parent_isin"),
                 a.get("address"), lat, lon, *geom_params,
                 a.get("asset_type_raw"), a.get("naturesense_asset_type"),
                 a.get("industry_code"), a.get("status"), a.get("capacity"),
                 a.get("capacity_units"), a.get("entity_stake_pct"),
                 a.get("source_url"), a.get("domain_source"),
                 psycopg.types.json.Json(a.get("supplementary_details")),
                 a.get("date_researched"), a.get("attribution_source")),
            )
    conn.commit()
    return len(assets)
