"""Stage 4: Extract — cache check → doc-extractor → save to Postgres."""

from __future__ import annotations

import logging
from typing import Any

from doc_extractor import (
    extract, extract_exhaustive, estimate_count,
    Document, Usage as ExtractorUsage, EXHAUSTIVE_THRESHOLD,
)

from ..config import Config
from ..cost import CostTracker
from ..db import get_connection, get_extraction_result, save_extraction_result, url_hash
from ..display import show_detail, show_spinner, show_stage
from ..models import Asset, ExtractedAsset, gics_reference_block, naturesense_reference_block

log = logging.getLogger(__name__)


_COORD_DEDUP_THRESHOLD = 0.0005  # ~55m at equator — matches signals.py


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

Field guidance:
- asset_name: include any official identifier or number (e.g. "Store #102",
  "Plant 3", "Unit B"). These help with deduplication downstream.
- entity_stake_pct: default to 100 unless the document says otherwise (joint
  venture, partial ownership, minority stake).
- supplementary_details: capture anything useful beyond the core fields.
  Use descriptive keys.

## Company Context
{company_context}
{ald_summary}

## NatureSense Asset Type Reference
Classify each asset's naturesense_asset_type using the descriptions below:
{naturesense_reference}

## GICS Industry Code Reference
Assign each asset's industry_code (6-digit) based on the asset type. Use the
descriptions below to pick the best-fit industry for each asset:
{gics_reference}
"""

COUNT_PROMPT_TEMPLATE = """\
Count the number of distinct physical assets belonging to {company} mentioned
in this document. Physical assets include facilities, plants, factories, mines,
quarries, offices, warehouses, stores, wind/solar farms, pipelines, terminals,
refineries, etc. Count each unique asset once.
"""


async def _summarize_description(description: str, company_name: str, model: str) -> str:
    """Use a cheap model to summarize the company description for extraction context."""
    import litellm
    response = await litellm.acompletion(
        model=model,
        messages=[
            {"role": "system", "content": (
                "Summarize this company description in 1-2 sentences for an asset extraction agent. "
                "Focus on: what the company does, what industry, key geographies, and what types of "
                "physical assets it likely has. Be concise."
            )},
            {"role": "user", "content": description},
        ],
        max_tokens=150,
    )
    return response.choices[0].message.content.strip()


async def _build_company_context(profile, summary_model: str) -> str:
    """Build a compact company context block from the profile.

    Accepts any object with CompanyProfile-like attributes (duck-typed).
    Uses a cheap model to summarize the description.
    """
    parts = [f"**{profile.legal_name}**"]

    ids = []
    if getattr(profile, "isin_list", None):
        ids.append(f"ISIN: {', '.join(profile.isin_list)}")
    if getattr(profile, "lei", None):
        ids.append(f"LEI: {profile.lei}")
    if ids:
        parts.append(" | ".join(ids))

    if getattr(profile, "description", ""):
        summary = await _summarize_description(
            profile.description, profile.legal_name, summary_model,
        )
        parts.append(summary)

    if getattr(profile, "subsidiaries", None):
        sub_names = [s.legal_name for s in profile.subsidiaries[:20]]
        parts.append(f"Subsidiaries: {', '.join(sub_names)}")

    if getattr(profile, "estimated_asset_count", None):
        parts.append(f"Estimated total assets: ~{profile.estimated_asset_count}")

    return "\n".join(parts)


async def run_extract(
    issuer_id: str, company_name: str, pages: list[dict[str, Any]],
    config: Config, existing_assets_summary: str | None = None,
    costs: CostTracker | None = None,
    profile: Any = None,
    skip_cache: bool = False,
) -> list[Asset]:
    """Extract assets from scraped pages, skipping cached extractions."""
    show_stage(4, "Extracting assets")

    conn = get_connection(config)
    all_assets: list[Asset] = []

    try:
        to_extract: list[dict[str, Any]] = []

        if skip_cache:
            show_detail(f"Bypassing extraction cache ({len(pages)} pages to extract)")
            to_extract = [p for p in pages if p.get("markdown")]
        else:
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
            if all_assets:
                show_detail(f"{len(all_assets)} assets loaded from cache, {len(to_extract)} pages to extract")

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

        company_context = (
            await _build_company_context(profile, config.summary_model)
            if profile else company_name
        )
        prompt = EXTRACT_PROMPT_TEMPLATE.format(
            company=company_name,
            company_context=company_context,
            ald_summary=ald_summary,
            naturesense_reference=naturesense_reference_block(),
            gics_reference=gics_reference_block(),
        )
        count_prompt = COUNT_PROMPT_TEMPLATE.format(company=company_name)
        extractor_cfg = config.extractor_config()

        # --- Two-pass: count assets per page, route high-count pages to
        # exhaustive extraction ---
        normal_docs: list[Document] = []
        exhaustive_docs: list[tuple[Document, int]] = []

        import asyncio

        async def _count_one(doc: Document) -> tuple[Document, int]:
            usage = ExtractorUsage()
            count = await estimate_count(
                doc, count_prompt, config.count_model,
                config=extractor_cfg, usage=usage,
            )
            if costs:
                costs.track_llm(
                    config.count_model,
                    usage.input_tokens, usage.output_tokens, "count",
                )
            return doc, count

        with show_spinner(f"Counting assets across {len(documents)} pages..."):
            count_results = await asyncio.gather(*[_count_one(doc) for doc in documents])

        RAG_ONLY_THRESHOLD = 120  # pages with 120+ assets → skip to RAG for QA
        rag_only_docs: list[tuple[Document, int]] = []

        for doc, count in count_results:
            if count >= RAG_ONLY_THRESHOLD:
                url_short = doc.metadata.get("url", "?")[:60]
                show_detail(f"  ~{count} assets in {url_short} → RAG-only (QA will query)")
                rag_only_docs.append((doc, count))
            elif count > EXHAUSTIVE_THRESHOLD:
                url_short = doc.metadata.get("url", "?")[:60]
                show_detail(f"  ~{count} assets in {url_short} (exhaustive)")
                exhaustive_docs.append((doc, count))
            else:
                normal_docs.append(doc)

        show_detail(
            f"Routing: {len(normal_docs)} normal, "
            f"{len(exhaustive_docs)} exhaustive, "
            f"{len(rag_only_docs)} RAG-only"
        )

        # Normal extraction for standard pages
        extracted: list[ExtractedAsset] = []
        if normal_docs:
            with show_spinner(f"Extracting from {len(normal_docs)} pages..."):
                extractor_usage = ExtractorUsage()
                extracted = await extract(
                    documents=normal_docs, schema=ExtractedAsset, prompt=prompt,
                    model=config.extract_model, max_concurrency=config.extractor_default_concurrency,
                    config=extractor_cfg, usage=extractor_usage,
                )
            if costs:
                costs.track_llm(
                    config.extract_model,
                    extractor_usage.input_tokens, extractor_usage.output_tokens, "extract",
                )
            show_detail(f"Extracted {len(extracted)} assets from {len(normal_docs)} pages")

        # Exhaustive extraction for medium-high-count pages
        for i, (doc, estimated_count) in enumerate(exhaustive_docs):
            url_short = doc.metadata.get("url", "?")[:60]
            with show_spinner(f"Exhaustive extraction ({i+1}/{len(exhaustive_docs)}): {url_short}"):
                exhaust_usage = ExtractorUsage()
                exhaustive_assets = await extract_exhaustive(
                    doc, ExtractedAsset, prompt, config.extract_model, estimated_count,
                    config=extractor_cfg, usage=exhaust_usage,
                )
            if costs:
                costs.track_llm(
                    config.extract_model,
                    exhaust_usage.input_tokens, exhaust_usage.output_tokens, "extract",
                )
            show_detail(f"  {len(exhaustive_assets)} assets from {url_short}")
            extracted.extend(exhaustive_assets)

        # Convert ExtractedAsset -> Asset (adds pipeline fields)
        new_assets = [Asset(**e.model_dump()) for e in extracted]

        # Save extraction results per page.
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
