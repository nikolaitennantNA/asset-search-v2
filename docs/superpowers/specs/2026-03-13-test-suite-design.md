# Test Suite Design — asset-search-v2 + web-scraper

**Date:** 2026-03-13
**Status:** Draft

## Overview

Build proper pytest test suites for both `asset-search-v2` and the `web-scraper` sibling package. Unit tests cover all pure logic with mocked external dependencies. Integration tests verify real Crawl4AI API calls (web-scraper) and real Postgres operations (asset-search-v2).

## Decisions

- **Test framework:** pytest + pytest-asyncio + respx (HTTP mocking)
- **Marker convention:** `@pytest.mark.integration` for tests requiring external services. Skipped by default in CI with `pytest -m "not integration"`.
- **Directory layout:** `tests/unit/` and `tests/integration/` in both repos.
- **DB mocking strategy:** Unit tests patch `asset_search.stages.<module>.get_connection` (the stage-level import) to return a `MagicMock` connection. This avoids needing a real Postgres for unit tests.
- **DB integration cleanup:** Integration tests use a test-specific `issuer_id` prefix (`test-<uuid>`) and DELETE matching rows in a pytest fixture finalizer. Simpler and more robust than transaction rollback across async tests.
- **Crawl4AI tests:** Only in web-scraper's integration tests. asset-search-v2 never directly tests the Crawl4AI API — it mocks `web_scraper.scrape`.
- **Integration URL count:** Configurable via `INTEGRATION_TEST_URLS` env var, default 5. Uses Boral URLs from `output/boral-ltd/2026-03-13T02-16-31/discovered_urls.csv`.

## Directory Structure

### web-scraper

```
tests/
  conftest.py                    # marker registration, shared fixtures
  unit/
    conftest.py
    test_scraper.py              # existing mocked Crawl4AI API tests (moved) + new usage tracking test
    test_models.py               # existing model tests (moved)
    test_signals.py              # existing 2 tests + expanded to ~30
  integration/
    conftest.py                  # API key fixture, URL loading
    test_scraper_live.py         # real Crawl4AI API
```

### asset-search-v2

```
tests/
  conftest.py                    # marker registration, shared fixtures
  unit/
    conftest.py                  # mocked DB connection fixture (patches get_connection)
    test_config.py
    test_cost.py
    test_models.py
    test_db.py
    test_scrape.py
    test_extract.py
    test_merge.py
  integration/
    conftest.py                  # real DB connection, test issuer_id, cleanup fixture
    test_db_live.py
    test_scrape_live.py
```

## Test Dependencies

Both repos add to `pyproject.toml`:

```toml
[project.optional-dependencies]
test = ["pytest>=8.0", "pytest-asyncio>=0.25.0", "respx>=0.22.0"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
markers = ["integration: hits real external services (Crawl4AI, Postgres)"]
```

### Run commands

```bash
uv run pytest                          # all tests
uv run pytest -m "not integration"     # unit only (fast, no services)
uv run pytest -m integration           # integration only
INTEGRATION_TEST_URLS=20 uv run pytest -m integration  # more URLs
```

## Unit Test Coverage

### web-scraper

#### `unit/test_scraper.py` (existing, moved + new)

Existing tests (moved from `tests/test_scraper.py`):
- Single URL uses `/crawl` endpoint with `url` key (not `urls`)
- Auth header is `X-API-Key`
- Request body includes `strategy: "browser"`
- 2-10 URLs use `/crawl/batch` with `urls` list
- API failure returns `ScrapedPage(success=False)`
- Empty URL list returns `[]`
- `use_proxy=True` sets `proxy.use_proxy` in payload
- No proxy by default

New tests to add:
- `test_usage_tracking_success` — `Usage.pages_crawled` incremented for successful pages
- `test_usage_tracking_failure` — `Usage.pages_failed` incremented for failed pages
- `test_usage_tracking_mixed` — mix of success/fail pages tracked correctly

#### `unit/test_models.py` (existing, moved)

- `ScrapeConfig` defaults (use_proxy=False, timeout_ms=30000, etc.)
- Removed fields not present
- `ScrapedPage` minimal construction
- `ScrapedPage` failure state

#### `unit/test_signals.py` (existing 2 + expanded)

| Test | What it verifies |
|---|---|
| **Existing** | |
| `test_extract_data_attributes` | data-lat/data-lng extraction |
| `test_inject_empty_signals` | no-op on empty signals |
| **JSON-LD** | |
| `test_jsonld_with_graph` | JSON-LD `@graph` with nested geo + address |
| `test_jsonld_nested_locations` | Recursive location extraction from nested items |
| `test_jsonld_string_address` | Address as plain string (not PostalAddress object) |
| **Google Maps** | |
| `test_google_maps_at_sign` | `@lat,lng` pattern in Maps URL |
| `test_google_maps_query_param` | `?q=lat,lng` and `&ll=lat,lng` patterns |
| `test_google_maps_place` | `/place/lat,lng` pattern |
| **Data attributes** | |
| `test_data_attr_with_name_address` | data-lat + data-name + data-address → location |
| `test_data_attr_without_name` | data-lat/lng only → coordinate (not location) |
| `test_data_attr_reverse_order` | data-lng before data-lat |
| **Meta tags** | |
| `test_meta_geo_position` | `<meta name="geo.position">` extraction |
| `test_meta_og_latlong` | `og:latitude` + `og:longitude` meta tags |
| **Inline JS** | |
| `test_inline_js_latlng_constructor` | `new google.maps.LatLng(lat, lng)` |
| `test_inline_js_object_notation` | `{lat: -33.8, lng: 151.2}` |
| `test_inline_js_array` | `[-33.8688, 151.2093]` |
| **Embedded JSON** | |
| `test_embedded_geojson_point` | GeoJSON Feature with Point geometry |
| `test_embedded_geojson_with_properties` | GeoJSON with tooltip/name in properties → location |
| **Raw HTML patterns** | |
| `test_html_coordinate_pattern` | Raw `"-33.8688, 151.2093"` text pattern extraction |
| `test_html_to_text_address_tag` | `_html_to_text` extracts text from `<address>` tags |
| **SVG / dedup / validation** | |
| `test_svg_stripping` | SVG path data not falsely matched as coordinates |
| `test_coord_dedup_within_threshold` | Coords within `_COORD_DEDUP_THRESHOLD` (0.0005, ~55m) deduplicated |
| `test_coord_dedup_preserves_distant` | Coords further apart than 0.0005 kept separate |
| `test_is_valid_coord_rejects_origin` | (0.001, 0.001) rejected (abs < 0.01) |
| `test_is_valid_coord_rejects_out_of_range` | lat>90 or lng>180 rejected |
| `test_is_valid_coord_accepts_valid` | e.g. (-33.8688, 151.2093) passes |
| **inject_signals** | |
| `test_inject_signals_with_coords_and_addresses` | Header injected with Addresses + Coordinates sections |
| `test_inject_signals_coords_only` | Header with only Coordinates section |
| **extract_signals top-level** | |
| `test_extract_signals_empty_html` | Returns `{"coordinates": [], "addresses": []}` |
| `test_extract_signals_combined_sources` | Multiple signal types extracted from one HTML document |

### asset-search-v2

#### `unit/test_config.py`

| Test | What it verifies |
|---|---|
| `test_to_pydantic_ai_model_bedrock` | `bedrock/us.model` → `bedrock:us.model` |
| `test_to_pydantic_ai_model_openai` | `openai/gpt-5` → `openai:gpt-5` |
| `test_to_pydantic_ai_model_anthropic` | `anthropic/claude-...` → `anthropic:claude-...` |
| `test_to_pydantic_ai_model_litellm_fallback` | `groq/llama-...` → `litellm:groq/llama-...` |
| `test_to_pydantic_ai_model_already_native` | `bedrock:us.model` passes through unchanged |
| `test_to_pydantic_ai_model_bare_string` | `"gpt-5"` (no provider prefix) passes through unchanged |
| `test_resolve_str_env_wins` | Env var takes priority over toml and default |
| `test_resolve_str_toml_wins` | Toml takes priority over default when no env var |
| `test_resolve_str_default_fallback` | Default used when no env var or toml |
| `test_resolve_int_env_wins` | Env var int conversion takes priority |
| `test_resolve_float_env_wins` | Env var float conversion takes priority |
| `test_resolve_bool_true_strings` | "true", "1", "yes" all resolve to True |
| `test_resolve_bool_toml_layer` | Toml bool value used when no env var set |
| `test_scraper_config_builder` | `Config.scraper_config()` produces correct `ScraperConfig` |
| `test_extractor_config_builder` | `Config.extractor_config()` produces correct `ExtractorConfig` |

#### `unit/test_cost.py`

| Test | What it verifies |
|---|---|
| `test_strip_model_prefix_bedrock` | `bedrock/us.anthropic.claude-...` → `anthropic.claude-...` |
| `test_strip_model_prefix_openai` | `openai/gpt-5` → `gpt-5` |
| `test_strip_model_prefix_no_prefix` | Bare model string passes through |
| `test_strip_model_prefix_all_regions` | All region prefixes stripped: `us.`, `global.`, `eu.`, `jp.`, `apac.` |
| `test_track_llm_accumulates` | Multiple calls accumulate per-model and per-stage |
| `test_track_llm_no_stage` | Call without stage only updates model totals, not `tokens_by_stage` |
| `test_track_pydantic_ai_input_output` | `track_pydantic_ai` with usage having `input_tokens`/`output_tokens` |
| `test_track_pydantic_ai_request_response_fallback` | `track_pydantic_ai` falls back to `request_tokens`/`response_tokens` |
| `test_track_pydantic_ai_none_usage` | `track_pydantic_ai(None)` is a no-op |
| `test_track_litellm_with_usage` | `track_litellm` extracts `prompt_tokens`/`completion_tokens` from response |
| `test_track_litellm_no_usage` | `track_litellm` with response missing `.usage` is a no-op |
| `test_llm_cost_usd_known_model` | Known model pricing math is correct (e.g. opus at $15/$75 per 1M) |
| `test_llm_cost_usd_unknown_model` | Unknown model falls back to hardcoded `(0.25, 2.00)` per 1M pricing |
| `test_api_cost_usd` | Crawl4AI + Exa + embedding + Cohere + Firecrawl costs all sum correctly |
| `test_track_firecrawl` | `track_firecrawl` increments `firecrawl_credits` |
| `test_total_cost_usd` | LLM + API costs combined |
| `test_total_cost_gbp` | USD → GBP conversion at `_USD_TO_GBP` rate |
| `test_summary_dict_keys` | `.summary()` returns dict with all 13 keys: `total_usd`, `total_gbp`, `llm_usd`, `api_usd`, `total_input_tokens`, `total_output_tokens`, `by_model`, `by_stage`, `crawl4ai_pages`, `exa_searches`, `embedding_tokens`, `cohere_rerank_calls`, `firecrawl_credits` |

#### `unit/test_models.py`

| Test | What it verifies |
|---|---|
| `test_asset_required_fields` | `asset_name` and `entity_name` required |
| `test_asset_defaults` | Optional fields have correct defaults |
| `test_asset_roundtrip` | `model_dump()` → `Asset(**data)` preserves all fields |
| `test_qa_report_defaults` | Empty QAReport has correct zero/empty defaults |
| `test_coverage_flag` | Construction with all fields |

#### `unit/test_db.py`

All tests use pure functions or mock `psycopg.Connection`.

| Test | What it verifies |
|---|---|
| `test_url_hash_deterministic` | Same URL → same hash |
| `test_url_hash_different_urls` | Different URLs → different hashes |
| `test_extraction_id_deterministic` | Same (page_id, model) → same hash |
| `test_extraction_id_different_inputs` | Different (page_id, model) → different hashes |
| `test_save_discovered_urls_params` | Correct SQL params and commit called |
| `test_save_discovered_urls_empty` | Empty list returns 0, no SQL executed |
| `test_get_cached_page_fresh` | Mock cursor returns row → function returns that row |
| `test_get_cached_page_miss` | Mock cursor returns None → function returns None |
| `test_save_scraped_page_returns_ids` | Returns (page_id, content_hash) tuple |
| `test_save_extraction_result_params` | Correct SQL params including `asset_count = len(assets_json)` |
| `test_save_qa_report_params` | Correct SQL params, includes CREATE TABLE IF NOT EXISTS |
| `test_save_discovered_assets_with_geom` | PostGIS `ST_MakePoint` expression when lat/lon present |
| `test_save_discovered_assets_without_geom` | `NULL` geometry when no coordinates |

#### `unit/test_scrape.py`

| Test | What it verifies |
|---|---|
| `test_config_from_notes_empty` | `None` / `""` → default `ScrapeConfig` |
| `test_config_from_notes_waf` | `"waf_blocked"` → `ScrapeConfig(use_proxy=True)` |
| `test_config_from_notes_wait_for` | `"wait_for:.locations-list"` → `ScrapeConfig(wait_for=".locations-list")` |
| `test_config_from_notes_combined` | `"waf_blocked wait_for:.selector"` → both fields set |
| `test_run_scrape_cache_hit` | Cached page returned in results; `web_scraper.scrape` not called |
| `test_run_scrape_cache_miss` | Uncached URL calls `web_scraper.scrape`, result saved via `save_scraped_page` |
| `test_run_scrape_cost_tracking` | `CostTracker.crawl4ai_pages` incremented by successful page count |
| `test_run_scrape_failed_page_not_saved` | `ScrapedPage(success=False)` not passed to `save_scraped_page` |
| `test_run_scrape_mixed` | Mix of 2 cached + 2 uncached URLs: all 4 in results, scraper called with only 2 URLs, 2 saves to DB |

#### `unit/test_extract.py`

| Test | What it verifies |
|---|---|
| `test_run_extract_cache_hit` | Cached extraction skips `doc_extractor.extract` call |
| `test_run_extract_cache_dedup` | Cached assets deduplicated by `(asset_name, entity_name)` tuple |
| `test_run_extract_calls_extractor` | Uncached pages passed to `extract()`. Captured `prompt` arg contains company name. |
| `test_run_extract_prompt_includes_ald_summary` | When `existing_assets_summary` provided, prompt includes it |
| `test_run_extract_saves_per_page` | Extraction results saved against every page in batch (same `all_dumped` for each) |
| `test_run_extract_cost_tracking` | LLM costs tracked with model name and stage="extract" |
| `test_run_extract_empty_pages` | Empty page list returns empty assets, no extractor call |

#### `unit/test_merge.py`

| Test | What it verifies |
|---|---|
| `test_run_merge_empty` | Empty asset list returns `[]`, no LLM call |
| `test_run_merge_assigns_uuid` | Assets without `asset_id` get UUID assigned |
| `test_run_merge_batching` | 51+ assets split into multiple `_merge_batch` calls (batch_size=50) |
| `test_run_merge_dedup_by_id` | Duplicate `asset_id` across batches deduplicated via `seen_ids` |
| `test_run_merge_final_dedup_called` | `_final_dedup` called when result has >1 asset |
| `test_run_merge_sets_metadata` | `industry_code`, `attribution_source="asset_search"`, `date_researched=today` set on each asset |
| `test_run_merge_cost_tracking` | LLM costs tracked with model name and stage="merge" |
| `test_run_merge_batch_llm_error_fallback` | `_merge_batch` JSON parse failure returns original batch unchanged |
| `test_run_merge_final_dedup_error_fallback` | `_final_dedup` JSON parse failure returns assets unchanged |

## Integration Test Coverage

### web-scraper `integration/test_scraper_live.py`

| Test | What it verifies |
|---|---|
| `test_scrape_single_url_live` | 1 Boral URL → `ScrapedPage` with non-empty markdown, raw_html, status_code=200 |
| `test_scrape_batch_live` | 2-10 URLs via batch endpoint → all pages returned with success=True |
| `test_scrape_async_chunk_live` | Uses `ScraperConfig(batch_limit=2)` to force async path with 5+ URLs → all pages returned |
| `test_scrape_signals_extracted` | At least one page has non-empty `signals["coordinates"]` or `signals["addresses"]` |
| `test_scrape_usage_tracking` | `Usage.pages_crawled` matches count of pages with `success=True` |
| `test_scrape_failure_handling` | Invalid URL (e.g. `https://this-domain-does-not-exist-abc123.com`) returns `success=False` gracefully |

All tests use `@pytest.mark.integration` and load API key from `.env`. URL count configurable via `INTEGRATION_TEST_URLS` (default 5). The async chunk test overrides `batch_limit` to avoid needing >10 URLs.

### asset-search-v2 `integration/test_db_live.py`

| Test | What it verifies |
|---|---|
| `test_save_and_get_discovered_urls` | Round-trip write/read for `discovered_urls` table. Verifies `get_discovered_urls` returns saved rows. |
| `test_save_and_get_scraped_page` | Write page, read back via `get_cached_page` within stale window |
| `test_scraped_page_staleness` | Page saved with `stale_days=0` not returned by `get_cached_page` after a brief delay |
| `test_save_and_get_extraction_result` | Round-trip write/read with content_hash matching between scraped_pages and extraction_results |
| `test_extraction_cache_invalidation` | Update scraped_page with new content_hash → `get_extraction_result` returns None |
| `test_save_and_get_discovered_assets` | Round-trip write/read for `discovered_assets` |
| `test_save_and_get_qa_report` | Round-trip write/read for `qa_results` using `save_qa_report` and `get_qa_report` |
| `test_upsert_overwrites` | Second `save_discovered_urls` with same URL updates category/notes |

All tests use `@pytest.mark.integration`, connect to real Postgres via `Config().corpgraph_db_url`. Each test uses a unique `issuer_id` (e.g. `test-<uuid4>`) and a fixture finalizer DELETEs all rows matching that issuer_id from all tables.

### asset-search-v2 `integration/test_scrape_live.py`

| Test | What it verifies |
|---|---|
| `test_run_scrape_end_to_end` | `run_scrape()` with real DB + mocked `web_scraper.scrape` (returns canned `ScrapedPage` objects): pages saved to Postgres, returned list has correct shape |
| `test_run_scrape_cache_cycle` | First call saves to DB; second call with same URLs returns cached pages — mock scraper only called once |
| `test_run_scrape_cost_tracking_live` | `CostTracker.crawl4ai_pages` reflects correct count after real DB run |

Uses real Postgres + `unittest.mock.patch("asset_search.stages.scrape.scrape")` returning canned `ScrapedPage` objects.

## Code Changes (non-test)

- Add `get_qa_report(conn, issuer_id) -> dict | None` to `db.py` — follows the same pattern as `get_discovered_urls`, `get_cached_page`, etc.

## Cleanup

- Delete `scripts/test_scrape_standalone.py` and `scripts/test_scrape_pipeline.py` (ad-hoc scripts replaced by proper tests)
- Move existing web-scraper tests from `tests/` into `tests/unit/` subdirectory
