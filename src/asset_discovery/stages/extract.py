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
        status="Open",
        entity_stake_pct=100,
        asset_type_raw=template.get("asset_type_raw", ""),
        naturesense_asset_type=template.get("naturesense_asset_type", ""),
        industry_code=template.get("industry_code", ""),
        supplementary_details=supplementary,
    )


_DETERMINISTIC_MIN_GROUP = 10

_ENRICH_PROMPT = """\
You have a list of {count} physical assets extracted deterministically from {company}'s
website. The core fields (name, address, coordinates) are correct but some classification
fields may be missing or generic.

Review and fill in these fields for ALL assets. Return a JSON array with one object per
asset, each containing ONLY these fields:
- index (0-based position in the input list)
- entity_isin (the ISIN of the entity that owns these assets, if you know it)
- asset_type_raw (e.g. "grocery store", "concrete plant", "distribution center")
- naturesense_asset_type (from the reference below)
- industry_code (6-digit GICS code from the reference below)
- entity_stake_pct (default 100 unless you know otherwise)
- status ("Open" unless the name suggests closed/planned)
- geocodable (true if the asset has a real street address that could be geocoded,
  false for vague/offshore/region-level locations)

Be consistent — if these are all the same type, give them all the same classification.
Only vary if you have a clear reason (e.g. different names suggest different asset types).

## NatureSense Asset Type Reference
{naturesense_reference}

## GICS Industry Code Reference
{gics_reference}

## Assets to enrich
{assets_json}
"""


async def _enrich_deterministic_assets(
    assets: list[Asset],
    company_name: str,
    model: str,
    costs: CostTracker | None,
) -> list[Asset]:
    """One LLM call to fill in classification fields for all deterministic assets."""
    if not assets:
        return assets

    import litellm

    # Build compact JSON of assets for the LLM
    compact = [
        {"index": i, "name": a.asset_name, "address": a.address}
        for i, a in enumerate(assets)
    ]
    # Only send first 100 + last 5 to avoid huge prompts (they're all the same type)
    if len(compact) > 105:
        sample = compact[:100] + compact[-5:]
        sample_note = f" (showing 105 of {len(compact)}, apply same classification to all)"
    else:
        sample = compact
        sample_note = ""

    prompt = _ENRICH_PROMPT.format(
        count=len(assets),
        company=company_name,
        assets_json=json.dumps(sample, indent=1) + sample_note,
        naturesense_reference=naturesense_reference_block(),
        gics_reference=gics_reference_block(),
    )

    try:
        response = await litellm.acompletion(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            max_tokens=2000,
        )
        content = response.choices[0].message.content.strip()
        result = json.loads(content)

        if costs:
            costs.track_llm(
                model,
                response.usage.prompt_tokens,
                response.usage.completion_tokens,
                "enrich",
            )

        # Parse enrichments — could be a list or {"assets": [...]}
        enrichments = result if isinstance(result, list) else result.get("assets", [])

        # Build lookup by index
        enrich_map: dict[int, dict] = {}
        for e in enrichments:
            if isinstance(e, dict) and "index" in e:
                enrich_map[int(e["index"])] = e

        # Apply to all assets. If we sampled, use the first enrichment as template
        template_enrich = enrichments[0] if enrichments else {}
        for i, asset in enumerate(assets):
            e = enrich_map.get(i, template_enrich)
            if e.get("entity_isin"):
                asset.entity_isin = e["entity_isin"]
            if e.get("asset_type_raw"):
                asset.asset_type_raw = e["asset_type_raw"]
            if e.get("naturesense_asset_type"):
                asset.naturesense_asset_type = e["naturesense_asset_type"]
            if e.get("industry_code"):
                asset.industry_code = str(e["industry_code"])
            if e.get("entity_stake_pct") is not None:
                asset.entity_stake_pct = float(e["entity_stake_pct"])
            if e.get("status"):
                asset.status = e["status"]
            if "geocodable" in e:
                asset.geocodable = bool(e["geocodable"])

        show_detail(f"  Enriched {len(assets)} deterministic assets")

    except Exception as e:
        log.warning("Enrichment failed: %s — using template fields", e)

    return assets


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

        # Validate: apply schema to multiple samples spread across the group.
        # If ANY sample fails, the template varies — fall back to LLM.
        n_validate = min(5, len(pages_with_html))
        # Pick samples spread across the group (first, last, middle)
        indices = [0, len(pages_with_html) - 1]
        step = max(1, len(pages_with_html) // n_validate)
        indices.extend(range(step, len(pages_with_html) - 1, step))
        indices = sorted(set(i for i in indices if i < len(pages_with_html)))[:n_validate]

        schema_ok = True
        for idx in indices:
            val = _apply_schema(pages_with_html[idx]["raw_html"], schema)
            if not (val.get("asset_name") or val.get("address")):
                log.info(
                    "Schema failed on sample %d/%d for %s (%s) — falling back to LLM",
                    idx, len(pages_with_html), prefix,
                    pages_with_html[idx]["url"][:60],
                )
                schema_ok = False
                break
        if not schema_ok:
            remaining.extend(group)
            continue

        val1 = _apply_schema(sample["raw_html"], schema)

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
Extract individual physical assets belonging to {company} from the documents below.

An asset is a SINGLE physical location you can point to on a map — one store, one
plant, one office, one warehouse. Each must have its own name and ideally its own
address or coordinates.

NOT assets (do not extract):
- Aggregate descriptions ("stores in Alabama", "store network", "500 locations")
- Categories or statistics ("stores with LED lighting", "stores opened in 2024")
- Events, programs, campaigns, foundations
- Datasets or file references ("KML dataset", "annual report")

Field guidance:
- asset_name: the specific name of THIS facility, including any number/ID
  (e.g. "Chandler - Store #33", "Aurora Distribution Center"). Do NOT prefix
  with the company name — entity_name already identifies the company.
- entity_name: always use the company's full legal name consistently:
  "{company}". Only use a subsidiary name if the document clearly states
  a different entity owns/operates the asset.
- entity_stake_pct: default to 100 unless stated otherwise.
- supplementary_details: anything useful beyond core fields (phone, hours, etc).

## Company Context
{company_context}
{ald_summary}

## NatureSense Asset Type Reference
{naturesense_reference}

## GICS Industry Code Reference
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
    import time as _time
    from rich.panel import Panel
    from rich.text import Text
    from ..display import console

    start = _time.monotonic()

    # Panel header
    header = Text()
    header.append("[4/6]", style="bold cyan")
    header.append(" Extracting assets", style="bold")
    header.append("  ·  ", style="dim")
    header.append(f"{len(pages)} pages")
    console.print(Panel(header, border_style="dim", padding=(0, 1)))

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

        import asyncio

        # --- Step 1: Count assets per page (all pages, cheap model) ---
        documents = [
            Document(
                content=p["markdown"],
                metadata={"url": p["url"], "page_id": p.get("page_id") or url_hash(p["url"])},
            )
            for p in to_extract if p.get("markdown")
        ]
        # Map doc URL → page dict for later reference
        page_by_url = {p["url"]: p for p in to_extract}

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
            company=company_name, company_context=company_context,
            ald_summary=ald_summary,
            naturesense_reference=naturesense_reference_block(),
            gics_reference=gics_reference_block(),
        )
        count_prompt = COUNT_PROMPT_TEMPLATE.format(company=company_name)
        extractor_cfg = config.extractor_config()

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

        # --- Step 2: Route pages ---
        RAG_ONLY_THRESHOLD = 120
        rag_only_pages: list[dict] = []
        extract_pages: list[dict] = []

        for doc, count in count_results:
            url = doc.metadata.get("url", "")
            page = page_by_url.get(url)
            if count >= RAG_ONLY_THRESHOLD:
                show_detail(f"  ~{count} assets in {url[:60]} → RAG query")
                if page:
                    rag_only_pages.append(page)
            elif page:
                extract_pages.append(page)

        # --- Step 3+4: Split deterministic vs LLM and run ALL in parallel ---
        # Group pages by prefix for potential deterministic extraction
        prefix_groups: dict[str, list[dict]] = {}
        non_prefix_pages: list[dict] = []
        for page in extract_pages:
            url = page.get("url", "")
            path = urlparse(url).path or "/"
            parts = [p for p in path.split("/") if p]
            prefix = "/" + "/".join(parts[:2]) + "/" if len(parts) >= 2 else "/"
            prefix_groups.setdefault(prefix, []).append(page)

        # Identify which prefix groups are large enough for deterministic
        det_candidates: list[tuple[str, list[dict]]] = []
        for prefix, group in sorted(prefix_groups.items(), key=lambda x: -len(x[1])):
            pages_with_html = [p for p in group if p.get("raw_html")]
            if len(pages_with_html) >= _DETERMINISTIC_MIN_GROUP:
                det_candidates.append((prefix, group))
            else:
                non_prefix_pages.extend(group)

        # Build LLM docs from pages that won't be deterministic
        llm_docs = [
            Document(
                content=p["markdown"],
                metadata={"url": p["url"], "page_id": p.get("page_id") or url_hash(p["url"])},
            )
            for p in non_prefix_pages if p.get("markdown")
        ]

        show_detail(
            f"Routing: {len(det_candidates)} prefix groups for deterministic, "
            f"{len(llm_docs)} LLM, "
            f"{len(rag_only_pages)} RAG-query"
        )

        async def _run_deterministic():
            """Run deterministic extraction on all candidate prefix groups."""
            if not det_candidates:
                return [], []
            det_assets: list[Asset] = []
            fallback_pages: list[dict] = []

            # Generate schemas for all prefix groups concurrently
            async def _process_prefix(prefix: str, group: list[dict]):
                pages_with_html = [p for p in group if p.get("raw_html")]
                sample = pages_with_html[0]
                schema = await _generate_schema(sample["raw_html"], config.extract_model, costs)
                if not schema:
                    return [], group

                # Validate on spread samples
                n_validate = min(5, len(pages_with_html))
                step = max(1, len(pages_with_html) // n_validate)
                indices = sorted(set([0, len(pages_with_html)-1] +
                    list(range(step, len(pages_with_html)-1, step))))[:n_validate]
                for idx in indices:
                    val = _apply_schema(pages_with_html[idx]["raw_html"], schema)
                    if not (val.get("asset_name") or val.get("address")):
                        return [], group

                # Get template from LLM sample
                sample_doc = Document(content=sample["markdown"], metadata={"url": sample["url"]})
                template = {"entity_name": company_name}
                try:
                    sample_assets = await extract(
                        documents=[sample_doc], schema=ExtractedAsset,
                        prompt=f"Extract the physical asset from this page for {company_name}.",
                        model=config.extract_model, max_concurrency=1,
                    )
                    if sample_assets:
                        sa = sample_assets[0]
                        template = {
                            "entity_name": sa.entity_name,
                            "asset_type_raw": sa.asset_type_raw,
                            "naturesense_asset_type": sa.naturesense_asset_type,
                            "industry_code": sa.industry_code,
                        }
                        val1 = _apply_schema(sample["raw_html"], schema)
                        det_asset = _fields_to_asset(val1, company_name, template)
                        if sa.latitude and (not det_asset or det_asset.latitude is None):
                            return [], group
                except Exception:
                    pass

                # Apply schema to all pages
                group_assets = []
                group_fallback = []
                for page in group:
                    html = page.get("raw_html", "")
                    if not html:
                        group_fallback.append(page)
                        continue
                    fields = _apply_schema(html, schema)
                    asset = _fields_to_asset(fields, company_name, template)
                    if asset:
                        asset.source_url = page.get("url", "")
                        group_assets.append(asset)
                    else:
                        group_fallback.append(page)

                show_detail(f"  Deterministic: {len(group_assets)} assets from {len(group)} pages ({prefix})")
                return group_assets, group_fallback

            results = await asyncio.gather(*[
                _process_prefix(prefix, group) for prefix, group in det_candidates
            ])

            for assets, fallback in results:
                det_assets.extend(assets)
                fallback_pages.extend(fallback)

            if det_assets:
                show_detail(f"Deterministic total: {len(det_assets)} assets")
                det_assets = await _enrich_deterministic_assets(
                    det_assets, company_name, config.extract_model, costs,
                )

            return det_assets, fallback_pages

        async def _run_llm():
            """LLM extraction — may receive additional fallback pages from deterministic."""
            if not llm_docs:
                return []
            extractor_usage = ExtractorUsage()
            result = await extract(
                documents=llm_docs, schema=ExtractedAsset, prompt=prompt,
                model=config.extract_model,
                max_concurrency=config.extractor_default_concurrency,
                config=extractor_cfg, usage=extractor_usage,
            )
            if costs:
                costs.track_llm(
                    config.extract_model,
                    extractor_usage.input_tokens, extractor_usage.output_tokens,
                    "extract",
                )
            show_detail(f"LLM extracted {len(result)} assets from {len(llm_docs)} pages")
            return result

        async def _run_rag_extraction():
            """Query RAG for assets in high-count pages — all pages concurrently."""
            if not rag_only_pages:
                return []
            try:
                from rag import RAGStore
                rag = RAGStore(config.corpgraph_db_url, config=config.rag_config())

                async def _rag_one(page: dict) -> list[ExtractedAsset]:
                    url_short = page["url"][:60]
                    query = (
                        f"List all physical assets, facilities, stores, offices, "
                        f"warehouses, and distribution centers for {company_name} "
                        f"mentioned in {page['url']}"
                    )
                    results = await rag.query(
                        query, namespace=issuer_id, top_k=20,
                    )
                    if not results:
                        return []
                    rag_doc = Document(
                        content="\n\n".join(r["content"] for r in results),
                        metadata={"url": page["url"]},
                    )
                    page_assets = await extract(
                        documents=[rag_doc], schema=ExtractedAsset, prompt=prompt,
                        model=config.extract_model, max_concurrency=1,
                        config=extractor_cfg,
                    )
                    show_detail(f"  RAG: {len(page_assets)} assets from {url_short}")
                    return page_assets

                results = await asyncio.gather(*[_rag_one(p) for p in rag_only_pages])
                return [asset for page_assets in results for asset in page_assets]
            except Exception as e:
                log.warning("RAG extraction failed: %s", e)
                return []

        show_detail("Running extraction paths in parallel...")
        det_result, llm_result, rag_result = await asyncio.gather(
            _run_deterministic(), _run_llm(), _run_rag_extraction(),
        )

        # Unpack deterministic result (assets + fallback pages)
        det_assets_final, det_fallback = det_result
        all_assets.extend(det_assets_final)

        # If deterministic had fallback pages, LLM-extract those too
        if det_fallback:
            fallback_docs = [
                Document(
                    content=p["markdown"],
                    metadata={"url": p["url"], "page_id": p.get("page_id") or url_hash(p["url"])},
                )
                for p in det_fallback if p.get("markdown")
            ]
            if fallback_docs:
                show_detail(f"Extracting {len(fallback_docs)} deterministic fallback pages via LLM...")
                fb_usage = ExtractorUsage()
                fb_result = await extract(
                    documents=fallback_docs, schema=ExtractedAsset, prompt=prompt,
                    model=config.extract_model, max_concurrency=config.extractor_default_concurrency,
                    config=extractor_cfg, usage=fb_usage,
                )
                llm_result.extend(fb_result)

        # Merge RAG results
        llm_result.extend(rag_result)

        # Build URL lookup from doc indices — each batch's documents are
        # numbered 0..N in the prompt via doc_index headers
        doc_urls = [p.get("url", "") for p in llm_pages if p.get("markdown")]
        new_assets = []
        for e in llm_result:
            dump = e.model_dump()
            # Use doc_index to set source_url, then drop it from Asset
            idx = dump.pop("doc_index", None)
            asset = Asset(**dump)
            if idx is not None and 0 <= idx < len(doc_urls):
                asset.source_url = doc_urls[idx]
            new_assets.append(asset)

        # Save extraction results
        all_dumped = [a.model_dump() for a in new_assets]
        for page in llm_pages:
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

    # Footer
    elapsed = _time.monotonic() - start
    mins, secs = divmod(int(elapsed), 60)
    time_str = f"{mins}m {secs:02d}s" if mins else f"{secs}s"
    footer = Text()
    footer.append(f"  Done  ·  {len(all_assets)} assets  ·  {time_str}", style="bold green")
    console.print(footer)
    console.print()

    return all_assets
