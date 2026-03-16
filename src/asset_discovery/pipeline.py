"""6-stage asset discovery pipeline orchestrator."""

from __future__ import annotations

import csv
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Config
from .cost import CostTracker
from .models import Asset, QAReport


# ---------------------------------------------------------------------------
# Intermediate file saving
# ---------------------------------------------------------------------------

def _make_run_dir(issuer_id: str) -> Path:
    """Create and return output/<issuer_id>/<timestamp>/ directory."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    run_dir = Path("output") / issuer_id / ts
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _save_profile(run_dir: Path, profile, context_doc: str) -> None:
    (run_dir / "profile.json").write_text(
        json.dumps(profile.model_dump(), indent=2, default=str)
    )
    (run_dir / "context.md").write_text(context_doc)


_URL_CSV_FIELDS = [
    "url", "category", "notes",
    "proxy_mode", "wait_for", "js_code", "scan_full_page", "screenshot",
]


def _save_urls(run_dir: Path, urls: list[dict[str, Any]]) -> None:
    path = run_dir / "discovered_urls.csv"
    if not urls:
        path.write_text(",".join(_URL_CSV_FIELDS) + "\n")
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_URL_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for u in urls:
            writer.writerow({field: u.get(field, "") for field in _URL_CSV_FIELDS})


def _slug(url: str) -> str:
    """Turn a URL into a filesystem-safe filename."""
    # Strip protocol and trailing slash
    name = re.sub(r"^https?://", "", url).rstrip("/")
    # Replace non-alphanumeric with hyphens, collapse runs
    name = re.sub(r"[^a-zA-Z0-9]+", "-", name).strip("-")
    return name[:120]


def _save_pages(run_dir: Path, pages: list[dict[str, Any]]) -> None:
    pages_dir = run_dir / "pages"
    pages_dir.mkdir(exist_ok=True)
    html_dir = run_dir / "pages_html"
    html_dir.mkdir(exist_ok=True)
    for page in pages:
        slug = _slug(page.get("url", "unknown"))
        md = page.get("markdown", "")
        if md:
            (pages_dir / f"{slug}.md").write_text(md)
        raw_html = page.get("raw_html", "")
        if raw_html:
            (html_dir / f"{slug}.html").write_text(raw_html)


def _save_extractions(run_dir: Path, assets: list[Asset]) -> None:
    (run_dir / "extracted_assets.json").write_text(
        json.dumps([a.model_dump() for a in assets], indent=2, default=str)
    )


def _save_merged(run_dir: Path, assets: list[Asset]) -> None:
    (run_dir / "final_assets.json").write_text(
        json.dumps([a.model_dump() for a in assets], indent=2, default=str)
    )


def _save_qa(run_dir: Path, qa_report: QAReport) -> None:
    (run_dir / "qa_report.json").write_text(
        json.dumps(qa_report.model_dump(), indent=2, default=str)
    )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
# TODO: Add --start-from <stage> to resume from any stage, loading prior
# results from Postgres (discovered_urls, scraped_pages, extraction_results,
# discovered_assets). Also leverages Spider server-side cache for
# re-scrapes at 0 credits. Needs: load profile/context_doc from DB or
# prior run_dir, and wire up stage-skip logic.

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
    from .display import show_stage
    show_stage(1, "Profiling")

    from corp_profile.profile import build_context_document

    if profile_file:
        from corp_profile.profile import build_profile_from_file
        profile = build_profile_from_file(profile_file)
    else:
        from corp_profile.profile import build_profile
        profile = build_profile(isin)

    # Optionally research the profile from scratch via LLM + web search
    if config.profile_research:
        from corp_profile.research import research_profile
        research_cfg = config.profile_research_config()
        profile, _research_changes = research_profile(
            identifier=isin, seed=profile, config=research_cfg,
        )

    # Optionally enrich the DB profile with LLM
    if config.profile_enrich or config.profile_web:
        from corp_profile.enrich import enrich_profile
        enrich_cfg = config.profile_enrich_config()
        web_cfg = config.profile_web_config() if config.profile_web else None
        profile, _changes = enrich_profile(profile, enrich_cfg, web_config=web_cfg)

    context_doc = build_context_document(profile)

    # Use profile's issuer_id as the canonical identifier for all downstream stages
    issuer_id = profile.issuer_id or isin

    from .display import show_intro_panel
    show_intro_panel(profile.legal_name, issuer_id, profile)
    stages_run.append("profile")

    run_dir = _make_run_dir(issuer_id)
    _save_profile(run_dir, profile, context_doc)

    if stop_after == "profile":
        return _result([], None, start, stages_run)

    # --- Stage 2: Discover ---
    from .stages.discover import run_discover

    discovered_urls = await run_discover(issuer_id, context_doc, config, costs)
    stages_run.append("discover")
    _save_urls(run_dir, discovered_urls)

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

    pages = await run_scrape(issuer_id, discovered_urls, config, rag_store, costs)
    stages_run.append("scrape")
    _save_pages(run_dir, pages)

    if stop_after == "scrape":
        return _result([], None, start, stages_run)

    # --- Stage 4: Extract ---
    from .stages.extract import run_extract

    existing_summary = _build_existing_summary(profile)
    assets = await run_extract(issuer_id, profile.legal_name, pages, config, existing_summary, costs)
    stages_run.append("extract")
    _save_extractions(run_dir, assets)

    if stop_after == "extract":
        return _result(assets, None, start, stages_run)

    # --- Stage 5: Merge ---
    from .stages.merge import run_merge

    assets = await run_merge(issuer_id, assets, config, industry_code=profile.primary_industry, costs=costs)
    stages_run.append("merge")
    _save_merged(run_dir, assets)

    if stop_after == "merge":
        return _result(assets, None, start, stages_run)

    # --- Stage 6: QA ---
    from .stages.qa import run_qa

    qa_report = await run_qa(issuer_id, context_doc, assets, config, rag_store, costs)
    stages_run.append("qa")
    _save_qa(run_dir, qa_report)

    # Persist QA report (including coverage flags) to DB
    from .db import get_connection, save_qa_report
    conn = get_connection(config)
    try:
        save_qa_report(conn, issuer_id, qa_report.model_dump())
    finally:
        conn.close()

    # --- Display results ---
    from .display import show_assets_table, show_cost_summary, show_coverage_flags
    show_assets_table(assets)
    show_coverage_flags(qa_report)
    elapsed = time.monotonic() - start
    show_cost_summary(
        stages_run=stages_run, url_count=len(discovered_urls),
        page_count=len(pages), asset_count=len(assets), elapsed=elapsed,
        costs=costs,
    )

    from .display import show_detail
    show_detail(f"Run output saved to {run_dir}")

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
