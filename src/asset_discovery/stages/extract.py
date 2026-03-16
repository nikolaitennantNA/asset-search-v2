"""Stage 4: Extract — cache check → doc-extractor → save to Postgres.

Includes a two-pass extraction mode for pages with many assets:
  Pass 1 (cheap model): estimate asset count per page.
  Pass 2 (extraction model): if count > threshold, extract in a loop,
    feeding already-found assets back each iteration until exhausted.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field

from doc_extractor import extract, Document, Usage as ExtractorUsage

from ..config import Config
from ..cost import CostTracker
from ..db import get_connection, get_extraction_result, save_extraction_result, url_hash
from ..display import show_stage
from ..models import Asset

log = logging.getLogger(__name__)


_COORD_DEDUP_THRESHOLD = 0.0005  # ~55m at equator — matches signals.py
_EXHAUSTIVE_THRESHOLD = 40       # pages with more assets than this trigger looped extraction
_MAX_EXTRACT_ROUNDS = 10         # safety cap on extraction loop iterations


def _dedup_by_coords(assets: list[Asset], threshold: float = _COORD_DEDUP_THRESHOLD) -> list[Asset]:
    """Deduplicate assets whose coordinates are within ~55m of each other.

    Keeps the first occurrence (which typically has richer metadata from signal
    injection). This prevents the LLM from double-counting coordinates that
    appear both in the signal header and in the page body.
    """
    result: list[Asset] = []
    seen_coords: list[tuple[float, float]] = []
    for asset in assets:
        if asset.latitude is not None and asset.longitude is not None:
            is_dup = False
            for elat, elng in seen_coords:
                if abs(elat - asset.latitude) < threshold and abs(elng - asset.longitude) < threshold:
                    is_dup = True
                    break
            if is_dup:
                continue
            seen_coords.append((asset.latitude, asset.longitude))
        result.append(asset)
    return result


EXTRACT_PROMPT_TEMPLATE = """\
Extract all physical assets belonging to {company} and its subsidiaries from the
documents below. Only extract assets owned or operated by {company}.

Physical assets include: facilities, plants, factories, mines, quarries, offices,
warehouses, data centers, stores, properties, wind/solar farms, pipelines,
terminals, refineries, and any other permanent physical infrastructure.
{ald_summary}
"""

EXHAUSTIVE_PROMPT_TEMPLATE = """\
Extract all physical assets belonging to {company} and its subsidiaries from the
document below. Only extract assets owned or operated by {company}.

Physical assets include: facilities, plants, factories, mines, quarries, offices,
warehouses, data centers, stores, properties, wind/solar farms, pipelines,
terminals, refineries, and any other permanent physical infrastructure.

IMPORTANT: This page contains approximately {estimated_count} assets. You have
already extracted {found_so_far} assets listed below. Extract ONLY assets that
are NOT in this list. Do not re-extract any asset already found.

Already extracted:
{already_found}
"""


# ---------------------------------------------------------------------------
# Pass 1: asset counting (cheap model)
# ---------------------------------------------------------------------------


class _PageAssetEstimate(BaseModel):
    """Cheap model output: estimated number of distinct assets on a page."""
    estimated_count: int = Field(
        description="Number of distinct physical assets (facilities, plants, "
        "offices, mines, etc.) mentioned on this page. Count each unique "
        "facility once even if mentioned multiple times.",
    )


async def _estimate_asset_count(
    document: Document,
    company_name: str,
    config: Config,
    costs: CostTracker | None = None,
) -> int:
    """Use a cheap model to estimate how many assets are on a page."""
    prompt = (
        f"Count the number of distinct physical assets belonging to "
        f"{company_name} mentioned in this document. Physical assets include "
        f"facilities, plants, factories, mines, quarries, offices, warehouses, "
        f"stores, wind/solar farms, pipelines, terminals, refineries, etc. "
        f"Count each unique facility once."
    )
    usage = ExtractorUsage()
    try:
        results = await extract(
            documents=[document],
            schema=_PageAssetEstimate,
            prompt=prompt,
            model=config.count_model,
            config=config.extractor_config(),
            usage=usage,
        )
        if costs:
            costs.track_llm(
                config.count_model, usage.input_tokens, usage.output_tokens, "count",
            )
        if results:
            return results[0].estimated_count
    except Exception as e:
        log.warning("Asset count estimation failed: %s", e)
    return 0


# ---------------------------------------------------------------------------
# Pass 2: exhaustive extraction loop for high-count pages
# ---------------------------------------------------------------------------


def _format_already_found(assets: list[Asset]) -> str:
    """Format already-extracted assets as a compact list for the prompt."""
    lines = []
    for a in assets:
        parts = [a.asset_name]
        if a.asset_type_raw:
            parts.append(f"[{a.asset_type_raw}]")
        if a.latitude is not None and a.longitude is not None:
            parts.append(f"({a.latitude:.4f}, {a.longitude:.4f})")
        lines.append("- " + " ".join(parts))
    return "\n".join(lines)


async def _exhaustive_extract(
    document: Document,
    company_name: str,
    estimated_count: int,
    config: Config,
    costs: CostTracker | None = None,
) -> list[Asset]:
    """Extract assets from a page in a loop until no new assets are found.

    Used for pages where the cheap counting pass estimated more assets than
    _EXHAUSTIVE_THRESHOLD. Each round passes previously found assets so the
    model skips them and extracts the next batch.
    """
    all_assets: list[Asset] = []
    seen_names: set[str] = set()

    for round_num in range(_MAX_EXTRACT_ROUNDS):
        if round_num == 0:
            prompt = EXTRACT_PROMPT_TEMPLATE.format(
                company=company_name,
                ald_summary=(
                    f"\nIMPORTANT: This page contains approximately "
                    f"{estimated_count} assets. Extract ALL of them. "
                    f"Do not stop early.\n"
                ),
            )
        else:
            prompt = EXHAUSTIVE_PROMPT_TEMPLATE.format(
                company=company_name,
                estimated_count=estimated_count,
                found_so_far=len(all_assets),
                already_found=_format_already_found(all_assets),
            )

        usage = ExtractorUsage()
        try:
            new_assets = await extract(
                documents=[document],
                schema=Asset,
                prompt=prompt,
                model=config.extract_model,
                config=config.extractor_config(),
                usage=usage,
            )
        except Exception as e:
            log.warning("Exhaustive extract round %d failed: %s", round_num + 1, e)
            break

        if costs:
            costs.track_llm(
                config.extract_model, usage.input_tokens, usage.output_tokens, "extract",
            )

        # Deduplicate against what we already have
        truly_new = []
        for asset in new_assets:
            key = asset.asset_name.strip().lower()
            if key not in seen_names:
                seen_names.add(key)
                truly_new.append(asset)

        log.info(
            "Exhaustive round %d: %d new assets (%d total, target ~%d)",
            round_num + 1, len(truly_new), len(all_assets) + len(truly_new),
            estimated_count,
        )

        if not truly_new:
            break

        all_assets.extend(truly_new)

        # If we've found close to the estimated count, stop
        if len(all_assets) >= estimated_count * 0.9:
            break

    return all_assets


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def run_extract(
    issuer_id: str, company_name: str, pages: list[dict[str, Any]],
    config: Config, existing_assets_summary: str | None = None,
    costs: CostTracker | None = None,
) -> list[Asset]:
    """Extract assets from scraped pages, skipping cached extractions."""
    show_stage(4, "Extracting assets")

    conn = get_connection(config)
    all_assets: list[Asset] = []

    try:
        to_extract: list[dict[str, Any]] = []
        # Dedup cached assets by (name, entity) since batched extraction
        # saves the full batch result against every page in the batch.
        seen_cached: set[tuple[str, str]] = set()
        for page in pages:
            pid = page.get("page_id") or url_hash(page["url"])
            cached = get_extraction_result(conn, pid, config.extract_model)
            if cached:
                for ad in (cached.get("assets_json") or []):
                    key = (ad.get("asset_name", ""), ad.get("entity_name", ""))
                    if key not in seen_cached:
                        seen_cached.add(key)
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

        # --- Two-pass: count assets per page, route high-count pages to
        # exhaustive extraction loop ---
        normal_docs: list[Document] = []
        exhaustive_docs: list[tuple[Document, int]] = []  # (doc, estimated_count)

        for doc in documents:
            count = await _estimate_asset_count(doc, company_name, config, costs)
            if count > _EXHAUSTIVE_THRESHOLD:
                log.info(
                    "Page %s: ~%d assets (above threshold %d), using exhaustive extraction",
                    doc.metadata.get("url", "?")[:80], count, _EXHAUSTIVE_THRESHOLD,
                )
                exhaustive_docs.append((doc, count))
            else:
                normal_docs.append(doc)

        # Normal extraction for standard pages
        new_assets: list[Asset] = []
        if normal_docs:
            prompt = EXTRACT_PROMPT_TEMPLATE.format(
                company=company_name,
                ald_summary=ald_summary,
            )
            extractor_usage = ExtractorUsage()
            new_assets = await extract(
                documents=normal_docs, schema=Asset, prompt=prompt,
                model=config.extract_model, max_concurrency=config.extractor_default_concurrency,
                config=config.extractor_config(),
                usage=extractor_usage,
            )
            if costs:
                costs.track_llm(
                    config.extract_model,
                    extractor_usage.input_tokens,
                    extractor_usage.output_tokens,
                    "extract",
                )

        # Exhaustive extraction for high-count pages
        for doc, estimated_count in exhaustive_docs:
            exhaustive_assets = await _exhaustive_extract(
                doc, company_name, estimated_count, config, costs,
            )
            new_assets.extend(exhaustive_assets)

        # Save extraction results per page.
        # Since doc-extractor batches pages together, we can't reliably map
        # individual assets to individual pages. Save all batch assets against
        # each page so the cache correctly detects "already extracted".
        all_dumped = [a.model_dump() for a in new_assets]
        for page in to_extract:
            pid = page.get("page_id") or url_hash(page["url"])
            save_extraction_result(
                conn, pid, issuer_id, page.get("content_hash", ""),
                config.extract_model, all_dumped,
            )

        all_assets.extend(new_assets)
    finally:
        conn.close()

    # Dedup assets with near-identical coordinates (signal injection double-count)
    all_assets = _dedup_by_coords(all_assets)
    return all_assets
