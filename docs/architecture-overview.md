# Asset Discovery Platform — Architecture Overview

## Problem

We need to find every physical asset (facilities, plants, mines, offices, warehouses, stores, etc.) owned or operated by a company and its subsidiaries. No single data source is complete — company websites miss coordinates, maps miss ownership context, store locators only cover retail-heavy companies. The solution is three independent discovery channels feeding into a shared merge + enrichment pipeline.

## Discovery Sources

### 1. Web Search (asset-search-v2)

**Status: Built and working.**

Discovers assets by scraping company websites, regulatory filings, and external databases. Uses an LLM agent to find relevant URLs, Crawl4AI to scrape them, and structured extraction to pull asset records from page content.

**Pipeline:** Profile → Discover → Scrape → Extract

**Strengths:**
- Finds named facilities with context (capacity, status, ownership, type)
- Catches regulatory filings (NPI, NGER, SEC) with facility-level detail
- Gets subsidiary and joint venture info from corporate pages
- Signal extraction pulls coordinates from HTML (JSON-LD, Google Maps embeds, data attributes)

**Weaknesses:**
- Coordinates often missing — depends on what the page contains
- Addresses may be incomplete or unstructured
- Can't verify that a location physically exists

### 2. Store Locator Extraction (planned)

**Status: Not yet built. Will integrate with asset-search-v2.**

Many companies with hundreds of locations (retail, branches, distribution) expose them through store locator pages that load data via JavaScript/AJAX. The web scraper already detects these and flags them in discover notes (`wait_for:.css-selector`). This module would go further.

**Approach:**
1. During scrape, detect store locator pages (AJAX-loaded, map widgets, `/api/locations` endpoints)
2. LLM analyzes the page structure and writes a targeted extraction script from pre-built templates
3. Script executes to pull all locations (often hundreds) in one pass
4. Results tagged as `source=store_locator` for downstream tracking

**Strengths:**
- Captures hundreds of locations in one shot for retail/branch-heavy companies
- Often includes structured data (lat/lng, address, phone, hours)
- More reliable than scraping individual location pages

**Weaknesses:**
- Complex to build — each store locator is different
- Requires LLM to reverse-engineer the page/API structure
- Only relevant for companies with store locator pages

### 3. Map Data (asset-locator replacement)

**Status: Existing asset-locator repo uses SerpAPI/Google Maps. To be replaced with Overture Maps + Google Places fallback.**

Discovers assets by searching map/POI databases for company name matches. Returns locations with validated coordinates and addresses.

**Data sources (priority order):**
1. **Overture Maps Places** — free, bulk, no rate limits. POI data from Meta + Microsoft + others
2. **OpenStreetMap** — community-sourced, good for industrial/infrastructure
3. **Google Places API** — most complete, but paid. Use as fallback for gaps

**Approach:**
1. Search Overture Places for company name + subsidiary names
2. Filter by geography (from profile's operating countries)
3. Supplement with OSM where Overture has gaps (industrial facilities, mines)
4. Google Places API as final fallback for high-priority missing assets

**Strengths:**
- Pre-validated coordinates and addresses
- Building/place metadata (type, category)
- Bulk processing — no per-page scraping cost

**Weaknesses:**
- Doesn't know about ownership, capacity, status, or operational context
- May miss assets that aren't public-facing businesses (internal warehouses, mines)
- Name matching is fuzzy — "Boral Quarry Lysterfield" may not match "Lysterfield Quarry"

## Shared Pipeline

All three discovery sources produce asset records (name, entity, address, coords, type, source) that feed into the shared downstream stages.

### Merge + Dedup

Takes assets from all active sources. Deduplicates across sources using:
- Name + entity matching
- Coordinate proximity (assets within ~100m of each other)
- LLM arbitration for ambiguous matches

Each asset retains its `discovery_source` tag (web_search, store_locator, map_data) so we can track provenance. When merging duplicates found by multiple sources, keeps the richest metadata from each.

### Enrich

Post-merge validation and enrichment. This is where Overture gets used **bidirectionally**:

**Forward (discovery → Overture):**
Assets found via web search or store locators often have coordinates but no building confirmation. Enrich checks each coordinate against Overture data:
- **Building density check** — pre-computed raster from Overture Buildings layer. Sample at each asset's coordinates. Zero density = strong flag. Low density = soft flag.
- **Overture Places reverse lookup** — "we found an asset at these coordinates from web scraping, does Overture know this place?" Confirms the asset and adds Overture metadata (building footprint, category).

**Reverse (Overture → enrichment):**
Assets found via Overture/map data may be sparse (just a name + coords). Enrich can:
- Search scraped web content (via RAG) for additional context about that location
- Fill in capacity, status, ownership details from web-sourced data
- Resolve and standardize addresses via Google Address Validation API

**Additional enrichment:**
- **Coordinate QA** — ocean detection, isolation detection, bounds validation, lat/lng swap detection
- **Address resolution** — Google Places Text Search → Address Validation → Geocoding fallback for assets missing addresses
- **Cross-source confidence** — assets confirmed by 2+ sources get higher confidence scores

### Verification / QA

Final quality gate before output:
- **Coverage check** — asset count vs profile estimates, type distribution, geographic coverage
- **Confidence scoring** — each asset gets a score based on: number of confirming sources, building density at coords, address validation result, coordinate QA result
- **Flagging** — suspicious assets flagged for human review (no building, ocean coords, isolated, single-source only)
- **Gap identification** — missing types, missing geographies, unexpectedly low counts

### Output

Verified asset records in TREX ALD-aligned format, persisted to Postgres with PostGIS geometry. Each record includes discovery_source, confidence_score, and any verification flags.

## Module Boundaries

```
corp-profile        Company profiling from corp-graph + LLM enrichment
asset-search-v2     Web search discovery pipeline (profile → discover → scrape → extract)
asset-locator-v2    Map data discovery (Overture + OSM + Google Places fallback)
corp-enrich         Post-merge enrichment + validation (Overture buildings, address resolution, coord QA)
web-scraper         Crawl4AI Cloud API wrapper (used by asset-search-v2)
doc-extractor       LLM structured extraction via instructor (used by asset-search-v2)
rag                 pgvector ingest + Cohere rerank retrieval (used by asset-search-v2)
```

Each module is a separate repo with its own package. The merge stage and downstream (enrich, QA) live in whichever repo orchestrates the full pipeline — likely asset-search-v2 or a new top-level orchestrator.

## What's Built vs Planned

| Component | Status |
|---|---|
| corp-profile | Built |
| asset-search-v2 (web search pipeline) | Built, testing |
| web-scraper | Built |
| doc-extractor | Built |
| rag | Built |
| Store locator extraction | Not started |
| asset-locator-v2 (Overture + map data) | Not started (v1 exists with SerpAPI) |
| corp-enrich (validation + enrichment) | Not started |
| Cross-source merge | Partially built (single-source merge in asset-search-v2) |
| Building density raster | Not started |
| Confidence scoring | Not started |

## Key Design Principles

1. **Loosely coupled modules** — merge accepts assets from any combination of sources. You can run just web search for one company, all three for another.
2. **Source tracking** — every asset knows where it came from. Cross-source confirmation increases confidence.
3. **Overture is dual-purpose** — discovery source AND validation layer. Not redundant — serves different roles at different pipeline stages.
4. **Enrich is not discovery** — it validates and fills gaps, it doesn't find new assets. Discovery is done before merge.
5. **Idempotent caching** — all stages cache results in Postgres. Re-running skips already-processed data.
