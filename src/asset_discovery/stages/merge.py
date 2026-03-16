"""Stage 5: Merge — cross-batch dedup and ALD dedup."""

from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any

import litellm

from ..config import Config
from ..cost import CostTracker
from ..display import show_detail, show_spinner, show_stage
from ..models import Asset

MERGE_PROMPT = """\
You are merging and deduplicating physical assets.

Given a batch of newly extracted assets and existing known assets:
1. Check if each new asset matches an existing one (same physical facility, \
possibly different name or level of detail).
2. If match: set matched_asset_id to the existing asset_id. Combine ALL data \
from both records — keep the richer value for each field. If one has an address \
and the other has coordinates, keep both. If one has capacity data the other \
lacks, keep it. Always prefer more complete, more specific information.
3. If new: set matched_asset_id to null.

Return JSON array of objects with all Asset fields plus matched_asset_id.
"""

FINAL_DEDUP_PROMPT = """\
You are doing a FINAL deduplication pass on a merged asset list.

These assets were merged in batches — some cross-batch duplicates may remain.
Identify duplicates: same physical facility appearing with different names, \
slight address variations, or different levels of detail.

For each group of duplicates, COMBINE all information into one record — merge \
the richest name, most complete address, all available coordinates, capacity data, \
and supplementary details from every duplicate.
Return JSON array of the deduplicated assets (Asset fields only, no matched_asset_id).
"""


async def run_merge(
    issuer_id: str, extracted_assets: list[Asset], config: Config,
    costs: CostTracker | None = None,
) -> list[Asset]:
    """Dedup extracted assets against each other."""
    show_stage(5, "Merging and deduplicating")
    if not extracted_assets:
        return []

    import asyncio

        batch_size = 50
        batches = [
            extracted_assets[i : i + batch_size]
            for i in range(0, len(extracted_assets), batch_size)
        ]
        show_detail(f"Merging {len(extracted_assets)} assets in {len(batches)} concurrent batches...")

        async def _merge_one(batch: list[Asset], batch_num: int) -> list[Asset]:
            merged = await _merge_batch(batch, [], [], config.merge_model, costs)
            if len(merged) < len(batch) // 2:
                show_detail(f"Batch {batch_num}: merge returned {len(merged)}/{len(batch)} — keeping originals")
                return batch
            show_detail(f"Batch {batch_num}: {len(batch)} → {len(merged)} after dedup")
            return merged

        with show_spinner(f"Merging {len(batches)} batches concurrently..."):
            batch_results = await asyncio.gather(*[
                _merge_one(batch, i + 1) for i, batch in enumerate(batches)
            ])

        final_assets: list[Asset] = []
        for batch_result in batch_results:
            final_assets.extend(batch_result)

        # Final cross-batch dedup pass
        if len(final_assets) > 1:
            with show_spinner(f"Final dedup across {len(final_assets)} assets..."):
                final_assets = await _final_dedup(final_assets, config.merge_model, costs)
            show_detail(f"Final: {len(final_assets)} unique assets")

    # Assign asset IDs and pipeline metadata after all dedup is done
    today = date.today().isoformat()
    for asset in final_assets:
        asset.asset_id = str(uuid.uuid4())
        asset.attribution_source = "asset_discovery"
        asset.date_researched = today

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
        return [
            Asset(**{
                k: v for k, v in item.items()
                if k in Asset.model_fields and k != "matched_asset_id"
            })
            for item in assets_data
        ]
    except Exception:
        return batch


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
