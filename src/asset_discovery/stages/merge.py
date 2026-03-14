"""Stage 5: Merge — cross-batch dedup, ALD dedup, GICS classification."""

from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any

import litellm

from ..config import Config
from ..cost import CostTracker
from ..db import get_connection, get_discovered_assets, save_discovered_assets
from ..display import show_stage
from ..gics import GICSMapping, get_gics_mapping
from ..models import Asset

MERGE_PROMPT = """\
You are deduplicating physical assets.

Given a batch of newly extracted assets and existing known assets:
1. Check if each new asset matches an existing one (same facility, different name).
2. If match: set matched_asset_id to the existing asset_id. Merge: keep richer data.
3. If new: set matched_asset_id to null.

Do NOT set naturesense_asset_type or industry_code — those are assigned post-merge.

Return JSON array of objects with all Asset fields plus matched_asset_id.
"""

FINAL_DEDUP_PROMPT = """\
You are doing a FINAL deduplication pass on a merged asset list.

These assets were merged in batches — some cross-batch duplicates may remain.
Identify duplicates: same physical facility appearing with different names, \
slight address variations, or different levels of detail.

For each group of duplicates, COMBINE all information into one record — merge \
the richest name, most complete address, all available coordinates, capacity data, \
and supplementary details from every duplicate. Preserve the asset_id from the \
version that has one (or the first one if multiple do).
Return JSON array of the deduplicated assets (Asset fields only, no matched_asset_id).
"""


async def run_merge(
    issuer_id: str, extracted_assets: list[Asset], config: Config, industry_code: str = "",
    costs: CostTracker | None = None,
) -> list[Asset]:
    """Dedup extracted assets against each other + existing ALD assets."""
    show_stage(5, "Merging and deduplicating")
    if not extracted_assets:
        return []

    gics = get_gics_mapping()

    conn = get_connection(config)
    try:
        existing = get_discovered_assets(conn, issuer_id)

        batch_size = 50
        final_assets: list[Asset] = []
        seen_ids: set[str] = set()

        for i in range(0, len(extracted_assets), batch_size):
            batch = extracted_assets[i : i + batch_size]
            merged = await _merge_batch(batch, existing, final_assets, config.merge_model, costs)

            for asset in merged:
                if not asset.asset_id:
                    asset.asset_id = str(uuid.uuid4())
                if asset.asset_id in seen_ids:
                    continue
                seen_ids.add(asset.asset_id)

                # GICS lookup: per-asset naturesense + industry code from mapping CSV.
                # Falls back to LLM-assigned naturesense and company-level industry code.
                _apply_gics(asset, gics, industry_code)

                asset.attribution_source = "asset_discovery"
                asset.date_researched = date.today().isoformat()
                final_assets.append(asset)

        # Final cross-batch dedup pass (spec §8: catch remaining duplicates)
        if len(final_assets) > 1:
            final_assets = await _final_dedup(final_assets, config.merge_model, costs)

        save_discovered_assets(conn, issuer_id, [a.model_dump() for a in final_assets])
    finally:
        conn.close()

    return final_assets


async def _merge_batch(
    batch: list[Asset], existing: list[dict[str, Any]],
    prior_merged: list[Asset], model: str,
    costs: CostTracker | None = None,
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
    prompt = MERGE_PROMPT

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

    if costs:
        costs.track_litellm(response, model, "merge")

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


def _apply_gics(asset: Asset, gics: GICSMapping, fallback_industry_code: str) -> None:
    """Apply GICS mapping to a single asset, falling back to LLM-assigned values."""
    match = gics.lookup(asset.asset_type_raw)
    if match:
        # Deterministic naturesense from the mapping CSV (overrides LLM classification)
        asset.naturesense_asset_type = match.naturesense_asset_type
        # Per-asset industry code from GICS mapping
        asset.industry_code = match.industry_code if match.industry_code else fallback_industry_code
    else:
        # No mapping match — keep LLM-assigned naturesense, use company-level industry code
        asset.industry_code = fallback_industry_code


async def _final_dedup(
    assets: list[Asset], model: str, costs: CostTracker | None = None,
) -> list[Asset]:
    """Final cross-batch dedup pass on the full merged asset list."""
    assets_json = json.dumps([a.model_dump() for a in assets], default=str)

    response = await litellm.acompletion(
        model=model,
        messages=[
            {"role": "system", "content": FINAL_DEDUP_PROMPT},
            {"role": "user", "content": f"## All merged assets ({len(assets)} total)\n{assets_json}"},
        ],
        response_format={"type": "json_object"},
    )

    if costs:
        costs.track_litellm(response, model, "merge")

    try:
        result = json.loads(response.choices[0].message.content)
        assets_data = result if isinstance(result, list) else result.get("assets", [])
        return [
            Asset(**{k: v for k, v in item.items() if k in Asset.model_fields})
            for item in assets_data
        ]
    except Exception:
        return assets
