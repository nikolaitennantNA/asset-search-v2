"""Stage 4: Extract — cache check → doc-extractor → save to Postgres."""

from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup
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


# ---------------------------------------------------------------------------
# Deterministic extraction — LLM generates schema once, apply to all pages
# ---------------------------------------------------------------------------

_SCHEMA_PROMPT = """\
You are analyzing a web page's HTML to create a reusable CSS selector schema for
extracting physical asset information. This schema will be applied to hundreds of
pages with the same template.

Look at this HTML and create a JSON object mapping asset fields to CSS selectors.
Use standard CSS selector syntax. For HTML attributes (like lat, lon, data-lat),
use the format "selector@attribute".

Required fields to map (skip any that don't exist in the HTML):
- asset_name: the facility/store/site name
- store_number: any identifier like "#33" or "Store 102"
- address: full street address
- city, state, zip: if separate from address
- latitude: coordinate (could be an attribute on a div, or in a Google Maps URL)
- longitude: coordinate
- phone: phone number
- entity_name: company name if shown

Also include a "latitude_source" field set to one of:
- "attribute" if coords are HTML attributes (e.g. <div lat="33.4">)
- "google_maps_url" if coords are in a Google Maps link href
- "text" if coords appear as text
- "none" if no coordinates found

Return ONLY a JSON object, no explanation. Example:
{{
  "asset_name": "h1",
  "store_number": ".store-number",
  "address": ".store-address a",
  "latitude": "#store-map@lat",
  "longitude": "#store-map@lon",
  "latitude_source": "attribute",
  "phone": "a[href^='tel:']"
}}

Here is the HTML (first 8000 chars):

{html}
"""


async def _generate_schema(html: str, model: str, costs: CostTracker | None) -> dict | None:
    """Use LLM to generate CSS selector schema from a sample page's HTML."""
    import litellm

    # Find the main content area — skip navigation/header boilerplate
    soup = BeautifulSoup(html, "html.parser")

    # Try common content containers in order of specificity
    content = (
        soup.find(class_="entry-content")
        or soup.find("main")
        or soup.find("article")
        or soup.find(id="content")
        or soup.find(id="main-content")
        or soup.find("body")
    )
    if content:
        for tag in content.find_all(["script", "style", "noscript"]):
            tag.decompose()
        truncated = str(content)[:10000]
    else:
        # No standard content container found — take the last 10K of HTML
        # (main content is usually at the end, after navigation/header)
        truncated = html[-10000:]
    try:
        response = await litellm.acompletion(
            model=model,
            messages=[
                {"role": "user", "content": _SCHEMA_PROMPT.format(html=truncated)},
            ],
            response_format={"type": "json_object"},
            max_tokens=500,
        )
        content = response.choices[0].message.content.strip()
        schema = json.loads(content)
        if costs:
            costs.track_llm(
                model,
                response.usage.prompt_tokens,
                response.usage.completion_tokens,
                "schema",
            )
        return schema
    except Exception as e:
        log.warning("Schema generation failed: %s", e)
        return None


def _apply_schema(html: str, schema: dict) -> dict[str, str]:
    """Apply CSS selector schema to extract fields from HTML."""
    soup = BeautifulSoup(html, "html.parser")
    result: dict[str, str] = {}

    for field, selector in schema.items():
        if field in ("latitude_source",):
            result[field] = selector
            continue

        # Handle attribute extraction: "selector@attr"
        if "@" in selector:
            css, attr = selector.rsplit("@", 1)
            el = soup.select_one(css)
            if el and el.get(attr):
                result[field] = el[attr].strip()
        else:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(strip=True)
                if text:
                    result[field] = text

    # Extract coordinates from Google Maps URL if that's the source
    if schema.get("latitude_source") == "google_maps_url" and not result.get("latitude"):
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "google.com/maps" in href:
                m = re.search(r"query=(-?\d+\.?\d*)[,%2C]+(-?\d+\.?\d*)", href)
                if not m:
                    m = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", href)
                if m:
                    result["latitude"] = m.group(1)
                    result["longitude"] = m.group(2)
                    break

    return result


def _fields_to_asset(fields: dict, company_name: str, template: dict) -> Asset | None:
    """Convert extracted fields dict to an Asset, using template for common fields."""
    name_parts = []
    if fields.get("asset_name"):
        name_parts.append(fields["asset_name"])
    if fields.get("store_number"):
        num = fields["store_number"]
        if num not in (fields.get("asset_name") or ""):
            name_parts.append(num)
    asset_name = " - ".join(name_parts) if name_parts else ""

    address_parts = []
    if fields.get("address"):
        address_parts.append(fields["address"])
    elif fields.get("city"):
        parts = [fields.get("city", ""), fields.get("state", "")]
        if fields.get("zip"):
            parts.append(fields["zip"])
        address_parts.append(", ".join(p for p in parts if p))
    address = " ".join(address_parts)

    lat = None
    lon = None
    try:
        if fields.get("latitude"):
            lat = float(fields["latitude"])
        if fields.get("longitude"):
            lon = float(fields["longitude"])
    except (ValueError, TypeError):
        pass

    if not asset_name and not address and lat is None:
        return None

    supplementary = {}
    if fields.get("phone"):
        supplementary["phone"] = fields["phone"]

    return Asset(
        asset_name=asset_name,
        entity_name=fields.get("entity_name") or template.get("entity_name", company_name),
        latitude=lat,
        longitude=lon,
        address=address,
        status="Operating",
        entity_stake_pct=100,
        asset_type_raw=template.get("asset_type_raw", ""),
        naturesense_asset_type=template.get("naturesense_asset_type", ""),
        industry_code=template.get("industry_code", ""),
        supplementary_details=supplementary,
    )


_DETERMINISTIC_MIN_GROUP = 10


async def _try_deterministic_extraction(
    pages: list[dict[str, Any]],
    company_name: str,
    model: str,
    costs: CostTracker | None,
) -> tuple[list[Asset], list[dict[str, Any]]]:
    """Try deterministic extraction on groups of template pages.

    Groups pages by URL prefix. For large groups (>10 pages with raw_html),
    generates a CSS selector schema from one sample page, validates it on
    a second sample, then applies to all pages in the group.

    Returns (deterministic_assets, remaining_pages_for_llm).
    """
    # Group pages by URL prefix (depth 2)
    prefix_groups: dict[str, list[dict]] = {}
    for page in pages:
        url = page.get("url", "")
        path = urlparse(url).path or "/"
        parts = [p for p in path.split("/") if p]
        prefix = "/" + "/".join(parts[:2]) + "/" if len(parts) >= 2 else "/"
        prefix_groups.setdefault(prefix, []).append(page)

    deterministic_assets: list[Asset] = []
    remaining: list[dict] = []

    # Sort by group size descending — only try the largest groups
    sorted_prefixes = sorted(prefix_groups.items(), key=lambda x: -len(x[1]))

    for prefix, group in sorted_prefixes:
        # Only try deterministic for large groups with raw HTML
        pages_with_html = [p for p in group if p.get("raw_html")]
        if len(pages_with_html) < _DETERMINISTIC_MIN_GROUP:
            remaining.extend(group)
            continue

        # Generate schema from first sample
        sample = pages_with_html[0]
        schema = await _generate_schema(sample["raw_html"], model, costs)
        if not schema:
            remaining.extend(group)
            continue

        # Validate: apply schema to two samples
        val1 = _apply_schema(sample["raw_html"], schema)
        val2 = _apply_schema(pages_with_html[1]["raw_html"], schema)
        if not (val1.get("asset_name") or val1.get("address")) or \
           not (val2.get("asset_name") or val2.get("address")):
            log.info("Schema validation failed for %s — falling back to LLM", prefix)
            remaining.extend(group)
            continue

        # LLM-extract the first sample to:
        # 1. Get template fields (asset_type_raw, naturesense_asset_type, industry_code)
        # 2. Compare with deterministic result to check for missing data
        sample_doc = Document(
            content=sample["markdown"],
            metadata={"url": sample["url"]},
        )
        template = {"entity_name": company_name}
        try:
            sample_assets = await extract(
                documents=[sample_doc], schema=ExtractedAsset,
                prompt=f"Extract the physical asset from this page for {company_name}.",
                model=model, max_concurrency=1,
            )
            if sample_assets:
                sa = sample_assets[0]
                template = {
                    "entity_name": sa.entity_name,
                    "asset_type_raw": sa.asset_type_raw,
                    "naturesense_asset_type": sa.naturesense_asset_type,
                    "industry_code": sa.industry_code,
                }
                # Check: did LLM find coords but schema didn't?
                det_asset = _fields_to_asset(val1, company_name, template)
                if sa.latitude and (not det_asset or det_asset.latitude is None):
                    log.info("Schema misses coordinates for %s — falling back to LLM", prefix)
                    remaining.extend(group)
                    continue
        except Exception as e:
            log.warning("LLM sample extraction failed for %s: %s", prefix, e)

        # Apply schema to all pages in the group
        extracted_count = 0
        for page in group:
            html = page.get("raw_html", "")
            if not html:
                remaining.append(page)
                continue
            fields = _apply_schema(html, schema)
            asset = _fields_to_asset(fields, company_name, template)
            if asset:
                asset.source_url = page.get("url", "")
                deterministic_assets.append(asset)
                extracted_count += 1
            else:
                remaining.append(page)

        show_detail(
            f"  Deterministic: {extracted_count} assets from {len(group)} pages ({prefix})"
        )

    return deterministic_assets, remaining


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

        # --- Deterministic extraction for template page groups ---
        det_assets, to_extract = await _try_deterministic_extraction(
            to_extract, company_name, config.extract_model, costs,
        )
        if det_assets:
            show_detail(f"Deterministic extraction: {len(det_assets)} assets")
            all_assets.extend(det_assets)

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
