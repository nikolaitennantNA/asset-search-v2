-- Minimal Postgres setup for asset-search-v2 cache tables.
-- Run this if you don't have the full corp-graph schema.
--
-- Usage:
--   createdb asset_search
--   psql asset_search -f scripts/init_cache_db.sql
--
-- Then set: CORPGRAPH_DB_URL=postgresql://localhost/asset_search

-- Extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS vector;

-- Discovered URLs: persist forever, tagged by the discover agent
CREATE TABLE IF NOT EXISTS discovered_urls (
    url_hash        TEXT PRIMARY KEY,
    url             TEXT NOT NULL,
    issuer_id       TEXT,
    category        TEXT NOT NULL,
    notes           TEXT,
    strategy        TEXT,
    proxy_mode      TEXT,
    wait_for        TEXT,
    js_code         TEXT,
    scan_full_page  BOOLEAN DEFAULT FALSE,
    screenshot      BOOLEAN DEFAULT FALSE,
    discovered_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_discovered_urls_issuer ON discovered_urls (issuer_id);

-- Migration: add scrape config columns if missing
DO $$ BEGIN
    ALTER TABLE discovered_urls ADD COLUMN IF NOT EXISTS strategy TEXT;
    ALTER TABLE discovered_urls ADD COLUMN IF NOT EXISTS proxy_mode TEXT;
    ALTER TABLE discovered_urls ADD COLUMN IF NOT EXISTS wait_for TEXT;
    ALTER TABLE discovered_urls ADD COLUMN IF NOT EXISTS js_code TEXT;
    ALTER TABLE discovered_urls ADD COLUMN IF NOT EXISTS scan_full_page BOOLEAN DEFAULT FALSE;
    ALTER TABLE discovered_urls ADD COLUMN IF NOT EXISTS screenshot BOOLEAN DEFAULT FALSE;
END $$;

-- Scraped pages: cached markdown + raw HTML, stale after 30 days
CREATE TABLE IF NOT EXISTS scraped_pages (
    page_id         TEXT PRIMARY KEY,
    url             TEXT NOT NULL,
    issuer_id       TEXT,
    content_hash    TEXT,
    markdown        TEXT,
    raw_html        TEXT,
    signals         JSONB,
    tokens          INTEGER,
    scraped_at      TIMESTAMPTZ DEFAULT NOW(),
    stale_after     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_scraped_pages_issuer ON scraped_pages (issuer_id);

-- Extraction results: invalidated when page content changes
CREATE TABLE IF NOT EXISTS extraction_results (
    extraction_id   TEXT PRIMARY KEY,
    page_id         TEXT REFERENCES scraped_pages(page_id),
    issuer_id       TEXT,
    content_hash    TEXT,
    model           TEXT,
    assets_json     JSONB,
    asset_count     INTEGER,
    extracted_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_extraction_results_issuer ON extraction_results (issuer_id);
CREATE INDEX IF NOT EXISTS idx_extraction_results_page ON extraction_results (page_id);

-- Discovered assets: final output, accumulates across runs
CREATE TABLE IF NOT EXISTS discovered_assets (
    asset_id        TEXT PRIMARY KEY,
    issuer_id       TEXT,
    asset_name      TEXT NOT NULL,
    entity_name     TEXT,
    entity_isin     TEXT,
    parent_name     TEXT,
    parent_isin     TEXT,
    address         TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    geom            GEOMETRY(Point, 4326),
    asset_type_raw  TEXT,
    naturesense_asset_type TEXT,
    industry_code   TEXT,
    status          TEXT,
    capacity        DOUBLE PRECISION,
    capacity_units  TEXT,
    entity_stake_pct REAL,
    source_url      TEXT,
    domain_source   TEXT,
    supplementary_details JSONB,
    discovery_source TEXT NOT NULL DEFAULT 'asset_search',
    source_dataset  TEXT,
    date_researched TEXT,
    attribution_source TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_discovered_assets_geom ON discovered_assets USING gist (geom);
CREATE INDEX IF NOT EXISTS idx_discovered_assets_issuer ON discovered_assets (issuer_id);
