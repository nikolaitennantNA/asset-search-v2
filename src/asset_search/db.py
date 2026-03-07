"""Psycopg connection + cache table helpers for the asset discovery pipeline."""

from __future__ import annotations

from typing import Any

import psycopg

from .config import Config


def get_connection(config: Config | None = None) -> psycopg.Connection:
    """Return a psycopg connection to the corpgraph database."""
    cfg = config or Config()
    return psycopg.connect(cfg.corpgraph_db_url)


def get_discovered_urls(
    conn: psycopg.Connection, issuer_id: str
) -> list[dict[str, Any]]:
    """Read discovered URLs for an issuer from the discovered_urls table."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT * FROM discovered_urls WHERE issuer_id = %s",
            (issuer_id,),
        )
        return cur.fetchall()


def save_discovered_urls(
    conn: psycopg.Connection, urls: list[dict[str, Any]]
) -> int:
    """Write discovered URLs to the discovered_urls table. Returns count."""
    if not urls:
        return 0
    with conn.cursor() as cur:
        for url in urls:
            cur.execute(
                """INSERT INTO discovered_urls
                   (issuer_id, url, url_hash, domain, domain_source,
                    page_type, priority_tier)
                   VALUES (%(issuer_id)s, %(url)s, %(url_hash)s, %(domain)s,
                           %(domain_source)s, %(page_type)s, %(priority_tier)s)
                   ON CONFLICT (url_hash) DO NOTHING""",
                url,
            )
    conn.commit()
    return len(urls)


def get_cached_page(
    conn: psycopg.Connection, url_hash: str
) -> dict[str, Any] | None:
    """Read a cached scraped page by URL hash."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT * FROM scraped_pages WHERE url_hash = %s",
            (url_hash,),
        )
        return cur.fetchone()


def save_scraped_page(
    conn: psycopg.Connection, page: dict[str, Any]
) -> None:
    """Write a scraped page to the scraped_pages table."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO scraped_pages
               (url_hash, url, html, markdown, content_hash,
                scraped_at, status_code)
               VALUES (%(url_hash)s, %(url)s, %(html)s, %(markdown)s,
                       %(content_hash)s, %(scraped_at)s, %(status_code)s)
               ON CONFLICT (url_hash) DO UPDATE SET
                 html = EXCLUDED.html,
                 markdown = EXCLUDED.markdown,
                 content_hash = EXCLUDED.content_hash,
                 scraped_at = EXCLUDED.scraped_at,
                 status_code = EXCLUDED.status_code""",
            page,
        )
    conn.commit()


def get_extraction_result(
    conn: psycopg.Connection, page_id: str, content_hash: str
) -> dict[str, Any] | None:
    """Check extraction cache for a page+content combination."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """SELECT * FROM extraction_results
               WHERE page_id = %s AND content_hash = %s""",
            (page_id, content_hash),
        )
        return cur.fetchone()


def save_extraction_result(
    conn: psycopg.Connection, result: dict[str, Any]
) -> None:
    """Write an extraction result to the extraction_results table."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO extraction_results
               (page_id, content_hash, assets_json, model, extracted_at)
               VALUES (%(page_id)s, %(content_hash)s, %(assets_json)s,
                       %(model)s, %(extracted_at)s)
               ON CONFLICT (page_id, content_hash) DO UPDATE SET
                 assets_json = EXCLUDED.assets_json,
                 model = EXCLUDED.model,
                 extracted_at = EXCLUDED.extracted_at""",
            result,
        )
    conn.commit()


def save_discovered_assets(
    conn: psycopg.Connection, assets: list[dict[str, Any]]
) -> int:
    """Write discovered assets to the discovered_assets table. Returns count."""
    if not assets:
        return 0
    with conn.cursor() as cur:
        for asset in assets:
            cur.execute(
                """INSERT INTO discovered_assets
                   (issuer_id, asset_name, entity_name, entity_isin,
                    parent_name, parent_isin, address, latitude, longitude,
                    asset_type, status, capacity, capacity_units,
                    ownership_pct, source_url, domain_source,
                    supplementary_details)
                   VALUES (%(issuer_id)s, %(asset_name)s, %(entity_name)s,
                           %(entity_isin)s, %(parent_name)s, %(parent_isin)s,
                           %(address)s, %(latitude)s, %(longitude)s,
                           %(asset_type)s, %(status)s, %(capacity)s,
                           %(capacity_units)s, %(ownership_pct)s,
                           %(source_url)s, %(domain_source)s,
                           %(supplementary_details)s)
                   ON CONFLICT DO NOTHING""",
                asset,
            )
    conn.commit()
    return len(assets)
