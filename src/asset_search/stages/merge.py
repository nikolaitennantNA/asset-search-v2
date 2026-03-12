"""Stage 5: Merge — cross-batch dedup, ALD dedup, naturesense classification."""

from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any

import litellm

from ..config import Config
from ..db import get_connection, get_discovered_assets, save_discovered_assets
from ..display import show_stage
from ..models import Asset

NATURESENSE_TYPES = [
    "Agricultural & Food Production", "Electricity Distribution", "Energy Production",
    "Heavy Industrial & Manufacturing", "IT Facility/Data Center", "Mining Operations",
    "Office/Housing", "Oil & Gas Facilities",
    "Other (5km buffer area of influence)", "Other (10km buffer area of influence)",
    "Other (20km buffer area of influence)", "Other (50km buffer area of influence)",
    "R&D Facility", "Retail", "Transportation and Logistics Facility", "Warehouse",
]

MERGE_PROMPT = """\
You are deduplicating and classifying physical assets.

Given a batch of newly extracted assets and existing known assets:
1. Check if each new asset matches an existing one (same facility, different name).
2. If match: set matched_asset_id to the existing asset_id. Merge: keep richer data.
3. If new: set matched_asset_id to null.
4. Classify naturesense_asset_type from: {types}

Return JSON array of objects with all Asset fields plus matched_asset_id.
"""


async def run_merge(
    issuer_id: str, extracted_assets: list[Asset], config: Config, industry_code: str = "",
) -> list[Asset]:
    """Dedup extracted assets against each other + existing ALD assets."""
    show_stage(5, "Merging and deduplicating")
    if not extracted_assets:
        return []

    conn = get_connection(config)
    try:
        existing = get_discovered_assets(conn, issuer_id)

        batch_size = 50
        final_assets: list[Asset] = []
        seen_ids: set[str] = set()

        for i in range(0, len(extracted_assets), batch_size):
            batch = extracted_assets[i : i + batch_size]
            merged = await _merge_batch(batch, existing, final_assets, config.merge_model)

            for asset in merged:
                if not asset.asset_id:
                    asset.asset_id = str(uuid.uuid4())
                if asset.asset_id in seen_ids:
                    continue
                seen_ids.add(asset.asset_id)
                asset.industry_code = industry_code
                asset.attribution_source = "asset_search"
                asset.date_researched = date.today().isoformat()
                final_assets.append(asset)

        save_discovered_assets(conn, issuer_id, [a.model_dump() for a in final_assets])
    finally:
        conn.close()

    return final_assets


async def _merge_batch(
    batch: list[Asset], existing: list[dict[str, Any]],
    prior_merged: list[Asset], model: str,
) -> list[Asset]:
    batch_json = json.dumps([a.model_dump() for a in batch], default=str)
    existing_summary = json.dumps(
        [
            {
                "asset_id": e["asset_id"], "asset_name": e["asset_name"],
                "address": e.get("address", ""), "asset_type_raw": e.get("asset_type_raw", ""),
            }
            for e in existing[:200]
        ],
        default=str,
    )
    prior_summary = json.dumps(
        [
            {"asset_id": a.asset_id, "asset_name": a.asset_name, "address": a.address}
            for a in prior_merged[-100:]
        ],
        default=str,
    )
    prompt = MERGE_PROMPT.format(types=", ".join(NATURESENSE_TYPES))

    response = await litellm.acompletion(
        model=model,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": (
                f"## New assets\n{batch_json}\n\n"
                f"## Existing known\n{existing_summary}\n\n"
                f"## Already merged\n{prior_summary}"
            )},
        ],
        response_format={"type": "json_object"},
    )

    try:
        result = json.loads(response.choices[0].message.content)
        assets_data = result if isinstance(result, list) else result.get("assets", [])
        merged: list[Asset] = []
        for item in assets_data:
            matched_id = item.pop("matched_asset_id", None)
            asset = Asset(**{k: v for k, v in item.items() if k in Asset.model_fields})
            if matched_id:
                asset.asset_id = matched_id
            merged.append(asset)
        return merged
    except Exception:
        return batch
