"""6-stage asset discovery pipeline orchestrator."""

from __future__ import annotations

import time
from typing import Any

from .config import Config
from .cost import CostTracker
from .models import Asset, QAReport


async def run(
    isin: str | None,
    config: Config | None = None,
    stop_after: str | None = None,
    profile_file: str | None = None,
) -> dict[str, Any]:
    """Run the full 6-stage pipeline for a company.

    Args:
        isin: Company ISIN or issuer_id. Optional when profile_file is given.
        config: Pipeline config. Defaults from env vars.
        stop_after: Stop after stage (profile/discover/scrape/extract/merge/qa).
        profile_file: Path to a JSON profile file. When set, Postgres is not
            queried for the profile and the issuer_id is taken from the file.

    Returns: dict with assets, qa_report, elapsed, stages_run, asset_count.
    """
    config = config or Config()
    start = time.monotonic()
    stages_run: list[str] = []
    costs = CostTracker()

    # --- Stage 1: Profile ---
    from corp_profile.profile import build_context_document

    if profile_file:
        from corp_profile.profile import build_profile_from_file
        profile = build_profile_from_file(profile_file)
    else:
        from corp_profile.profile import build_profile
        profile = build_profile(isin)

    # Optionally enrich the DB profile with LLM-based estimate refinement
    if config.profile_enrich:
        from corp_profile.enrich import EnrichConfig, enrich_profile
        enrich_cfg = config.profile_enrich_config()
        profile = enrich_profile(profile, enrich_cfg)

    context_doc = build_context_document(profile)

    # Use profile's issuer_id as the canonical identifier for all downstream stages
    issuer_id = profile.issuer_id or isin

    from .display import show_intro_panel
    show_intro_panel(profile.legal_name, issuer_id, profile)
    stages_run.append("profile")

    if stop_after == "profile":
        return _result([], None, start, stages_run)

    # --- Stage 2: Discover ---
    from .stages.discover import run_discover

    discovered_urls = await run_discover(issuer_id, context_doc, config)
    stages_run.append("discover")

    if stop_after == "discover":
        return _result([], None, start, stages_run)

    # --- Set up RAG store ---
    rag_store = None
    try:
        from rag import RAGStore
        rag_store = RAGStore(pg_url=config.corpgraph_db_url, config=config.rag_config())
    except ImportError:
        pass

    # --- Stage 3: Scrape ---
    from .stages.scrape import run_scrape

    pages = await run_scrape(issuer_id, discovered_urls, config, rag_store)
    stages_run.append("scrape")

    if stop_after == "scrape":
        return _result([], None, start, stages_run)

    # --- Stage 4: Extract ---
    from .stages.extract import run_extract

    existing_summary = _build_existing_summary(profile)
    assets = await run_extract(issuer_id, profile.legal_name, pages, config, existing_summary)
    stages_run.append("extract")

    if stop_after == "extract":
        return _result(assets, None, start, stages_run)

    # --- Stage 5: Merge ---
    from .stages.merge import run_merge

    assets = await run_merge(issuer_id, assets, config, industry_code=profile.primary_industry)
    stages_run.append("merge")

    if stop_after == "merge":
        return _result(assets, None, start, stages_run)

    # --- Stage 6: QA ---
    from .stages.qa import run_qa

    qa_report = await run_qa(issuer_id, context_doc, assets, config, rag_store)
    stages_run.append("qa")

    # --- Display results ---
    from .display import show_assets_table, show_cost_summary
    show_assets_table(assets)
    elapsed = time.monotonic() - start
    show_cost_summary(
        stages_run=stages_run, url_count=len(discovered_urls),
        page_count=len(pages), asset_count=len(assets), elapsed=elapsed,
        costs=costs,
    )

    return _result(assets, qa_report, start, stages_run, costs)


def _result(assets, qa_report, start, stages_run, costs=None):
    result = {
        "assets": assets, "qa_report": qa_report,
        "elapsed": time.monotonic() - start,
        "stages_run": stages_run, "asset_count": len(assets),
    }
    if costs:
        result["costs"] = costs.summary()
    return result


def _build_existing_summary(profile) -> str | None:
    if not hasattr(profile, "existing_assets") or not profile.existing_assets:
        if hasattr(profile, "estimated_asset_count") and profile.estimated_asset_count:
            return f"Estimated {profile.estimated_asset_count} total assets."
        return None
    by_type: dict[str, int] = {}
    for a in profile.existing_assets:
        t = getattr(a, "naturesense_asset_type", None) or "Unknown"
        by_type[t] = by_type.get(t, 0) + 1
    total = len(profile.existing_assets)
    breakdown = ", ".join(f"{c} {t}" for t, c in sorted(by_type.items(), key=lambda x: -x[1]))
    return f"This company has {total} known assets: {breakdown}."
