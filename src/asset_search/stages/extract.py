"""Stage 4: Extract — cache check → doc-extractor → save to Postgres."""

from __future__ import annotations

from typing import Any

from doc_extractor import extract, Document

from ..config import Config
from ..db import get_connection, get_extraction_result, save_extraction_result, url_hash
from ..display import show_stage
from ..models import Asset


EXTRACT_PROMPT_TEMPLATE = """\
Extract all physical assets belonging to {company} and its subsidiaries from the
documents below. Physical assets include: facilities, plants, factories, mines,
quarries, offices, warehouses, data centers, stores, properties, wind/solar farms,
pipelines, terminals, refineries, and any other permanent physical infrastructure.

For each asset, extract:
- asset_name (required): specific name of the facility/site
- entity_name (required): who owns or operates it
- entity_isin / parent_name / parent_isin: ownership chain if mentioned
- entity_stake_pct: ownership percentage 0-100 if mentioned
- address: full address text if available
- latitude / longitude: coordinates if available
- status: Operating / Construction / Planned / Cancelled
- capacity / capacity_units: numeric capacity and units (e.g., 500 MW)
- asset_type_raw: free text description of asset type
- supplementary_details: anything extra (fuel type, year built, etc.)

Only extract assets you're confident are real physical locations.
{ald_summary}
"""


async def run_extract(
    issuer_id: str, company_name: str, pages: list[dict[str, Any]],
    config: Config, existing_assets_summary: str | None = None,
) -> list[Asset]:
    """Extract assets from scraped pages, skipping cached extractions."""
    show_stage(4, "Extracting assets")

    conn = get_connection(config)
    all_assets: list[Asset] = []

    try:
        to_extract: list[dict[str, Any]] = []
        for page in pages:
            pid = page.get("page_id") or url_hash(page["url"])
            cached = get_extraction_result(conn, pid, config.extract_model)
            if cached:
                for ad in (cached.get("assets_json") or []):
                    all_assets.append(Asset(**ad))
            else:
                to_extract.append(page)

        if not to_extract:
            return all_assets

        documents = [
            Document(
                content=p["markdown"],
                metadata={"url": p["url"], "page_id": p.get("page_id") or url_hash(p["url"])},
            )
            for p in to_extract if p.get("markdown")
        ]

        ald_summary = ""
        if existing_assets_summary:
            ald_summary = (
                f"\nThis company has existing known assets:\n{existing_assets_summary}\n"
                "Extract all assets you find -- dedup happens later."
            )

        prompt = EXTRACT_PROMPT_TEMPLATE.format(company=company_name, ald_summary=ald_summary)

        new_assets = await extract(
            documents=documents, schema=Asset, prompt=prompt,
            model=config.extract_model, max_concurrency=config.max_extract_concurrency,
        )

        # Save per-page extraction results
        by_page: dict[str, list[Asset]] = {}
        for asset in new_assets:
            pid = url_hash(asset.source_url) if asset.source_url else "unknown"
            by_page.setdefault(pid, []).append(asset)

        for page in to_extract:
            pid = page.get("page_id") or url_hash(page["url"])
            page_assets = by_page.get(pid, [])
            save_extraction_result(
                conn, pid, issuer_id, page.get("content_hash", ""),
                config.extract_model, [a.model_dump() for a in page_assets],
            )

        all_assets.extend(new_assets)
    finally:
        conn.close()

    return all_assets
