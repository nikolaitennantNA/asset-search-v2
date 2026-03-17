"""Stage 5: Merge — concurrent batch dedup, write to file, final review pass."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from datetime import date
from pathlib import Path
from typing import Any

import litellm
from rich.panel import Panel
from rich.text import Text

from ..config import Config
from ..cost import CostTracker
from ..display import console, show_detail, show_spinner
from ..models import Asset

MERGE_PROMPT = """\
You are deduplicating physical assets. You receive a batch of assets (each with
an "idx" field) that may contain duplicates — the same physical facility extracted
from different pages with slightly different names, addresses, or detail levels.

For each group of duplicates:
1. MERGE all data into the entry with the lowest idx — combine the richest name,
   most complete address, all coordinates, capacity data, and supplementary details.
2. Add the OTHER indices to "remove_indices".

Return a JSON object with:
- "merged": list of objects like {"idx": 3, "updates": {"address": "...", "latitude": ...}}
  Only include fields that changed from merging. Skip entries that weren't merged.
- "remove_indices": list of indices to drop (the duplicates that were merged INTO another).

If there are no duplicates, return {"merged": [], "remove_indices": []}.
"""

FINAL_REVIEW_PROMPT = """\
You are reviewing a complete asset list for cross-batch duplicates.

These assets were deduplicated in separate batches — duplicates that span
batch boundaries may remain. Scan the full list and identify any remaining
duplicates (same facility, similar name/address, near-identical coordinates).

Return a JSON object with "remove_indices" — 0-based indices to remove.
Keep the entry with the richest data. If no duplicates, return {"remove_indices": []}.
"""


async def run_merge(
    issuer_id: str, extracted_assets: list[Asset], config: Config,
    costs: CostTracker | None = None,
    run_dir: Path | None = None,
) -> list[Asset]:
    """Dedup extracted assets in concurrent batches, write results to file."""
    start = time.monotonic()

    header = Text()
    header.append("[5/6]", style="bold cyan")
    header.append(" Merging & deduplicating", style="bold")
    header.append("  ·  ", style="dim")
    header.append(f"{len(extracted_assets)} assets")
    console.print(Panel(header, border_style="dim", padding=(0, 1)))

    if not extracted_assets:
        return []

    batch_size = 50
    batches = [
        extracted_assets[i : i + batch_size]
        for i in range(0, len(extracted_assets), batch_size)
    ]

    # Each batch deduplicates and writes its results to the shared list
    all_assets: list[Asset] = []
    lock = asyncio.Lock()

    async def _merge_batch(batch: list[Asset], batch_num: int) -> None:
        """Dedup one batch and append results to all_assets."""
        batch_json = json.dumps(
            [{"idx": i, **a.model_dump()} for i, a in enumerate(batch)],
            default=str,
        )

        try:
            response = await litellm.acompletion(
                model=config.merge_model,
                messages=[
                    {"role": "system", "content": MERGE_PROMPT},
                    {"role": "user", "content": batch_json},
                ],
                response_format={"type": "json_object"},
            )
            if costs:
                costs.track_litellm(response, config.merge_model, "merge")

            result = json.loads(response.choices[0].message.content)

            # Apply merge updates to the kept entries
            for update in result.get("merged", []):
                idx = update.get("idx")
                updates = update.get("updates", {})
                if idx is not None and 0 <= idx < len(batch) and updates:
                    for field, value in updates.items():
                        if field in Asset.model_fields and value is not None:
                            setattr(batch[idx], field, value)

            remove = set(result.get("remove_indices", []))
            kept = [a for i, a in enumerate(batch) if i not in remove]
            removed = len(batch) - len(kept)
        except Exception:
            kept = batch
            removed = 0

        async with lock:
            all_assets.extend(kept)

        if removed:
            show_detail(f"  Batch {batch_num}: {len(batch)} → {len(kept)} ({removed} dupes)")
        else:
            show_detail(f"  Batch {batch_num}: {len(batch)} — no dupes")

    # Run all batches concurrently
    show_detail(f"Deduplicating in {len(batches)} concurrent batches...")
    await asyncio.gather(*[
        _merge_batch(batch, i + 1) for i, batch in enumerate(batches)
    ])

    # Final cross-batch review — just flags indices to remove
    if len(all_assets) > batch_size:
        show_detail(f"Final review across {len(all_assets)} assets...")
        # Send compact version: just index + name + address + coords
        compact = json.dumps([
            {"idx": i, "name": a.asset_name, "address": a.address,
             "lat": a.latitude, "lon": a.longitude}
            for i, a in enumerate(all_assets)
        ], default=str)

        try:
            response = await litellm.acompletion(
                model=config.merge_model,
                messages=[
                    {"role": "system", "content": FINAL_REVIEW_PROMPT},
                    {"role": "user", "content": compact},
                ],
                response_format={"type": "json_object"},
            )
            if costs:
                costs.track_litellm(response, config.merge_model, "merge")

            result = json.loads(response.choices[0].message.content)
            remove = set(result.get("remove_indices", []))
            if remove:
                all_assets = [a for i, a in enumerate(all_assets) if i not in remove]
                show_detail(f"  Removed {len(remove)} cross-batch duplicates")
            else:
                show_detail(f"  No cross-batch duplicates")
        except Exception:
            show_detail(f"  Final review failed — keeping all")

    # Assign asset IDs and metadata
    today = date.today().isoformat()
    for asset in all_assets:
        asset.asset_id = str(uuid.uuid4())
        asset.attribution_source = "asset_discovery"
        asset.date_researched = today

    # Footer
    from ..display import show_done
    elapsed = time.monotonic() - start
    show_done([f"{len(all_assets)} unique assets"], elapsed=elapsed)

    return all_assets
