"""Integration test fixtures — real Postgres, test-scoped cleanup."""

import uuid

import psycopg
import pytest
from psycopg.rows import dict_row

from asset_discovery.config import Config


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
    for table in [
        "extraction_results",
        "scraped_pages",
        "discovered_urls",
        "discovered_assets",
        "qa_results",
    ]:
        try:
            with db_conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table} WHERE issuer_id = %s", (issuer_id,))
            db_conn.commit()
        except psycopg.errors.UndefinedTable:
            db_conn.rollback()
