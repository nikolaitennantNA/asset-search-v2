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

def _make_run_dir(issuer_id: str, company_name: str = "") -> Path:
    """Create and return output/<company_shortid>/<timestamp>/ directory."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    # Build a readable folder name: company_name_abc123
    short_id = issuer_id[:6]
    if company_name:
        slug = re.sub(r"[^a-z0-9]+", "_", company_name.lower()).strip("_")[:30]
        folder = f"{slug}_{short_id}"
    else:
        folder = issuer_id
    run_dir = Path("output") / folder / ts
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


_ALD_FIELDS = [
    "asset_id", "entity_name", "entity_isin", "parent_name", "parent_isin",
    "name", "entity_stake_pct", "latitude", "longitude", "address", "status",
    "capacity", "capacity_units", "asset_type_raw", "naturesense_asset_type",
    "industry_code", "date_researched", "supplementary_details",
    "attribution_source", "source_url",
]


def _save_merged(run_dir: Path, assets: list[Asset]) -> None:
    """Save intermediate JSON after merge (pre-QA checkpoint)."""
    (run_dir / "final_assets.json").write_text(
        json.dumps([a.model_dump() for a in assets], indent=2, default=str)
    )


def _save_final(run_dir: Path, assets: list[Asset], qa_report=None) -> None:
    """Save final output: JSON + CSV (ALD format) + XLSX (Key + Assets sheets)."""
    # JSON
    (run_dir / "final_assets.json").write_text(
        json.dumps([a.model_dump() for a in assets], indent=2, default=str)
    )

    # CSV in ALD format
    with open(run_dir / "final_assets.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_ALD_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for a in assets:
            row = a.model_dump()
            row["name"] = row.pop("asset_name", "")
            for k, v in row.items():
                if isinstance(v, (dict, list)):
                    row[k] = json.dumps(v) if v else ""
            writer.writerow({k: row.get(k, "") for k in _ALD_FIELDS})

    # XLSX with Key + Assets sheets
    try:
        import openpyxl

        wb = openpyxl.Workbook()

        # Key sheet
        key_ws = wb.active
        key_ws.title = "Key"
        key_ws.append(["Quality Flags"])
        if qa_report and qa_report.coverage_flags:
            for flag in qa_report.coverage_flags:
                key_ws.append([f"[{flag.severity}] {flag.flag_type}: {flag.description}"])
        else:
            key_ws.append(["No quality flags — coverage looks good."])
        if qa_report and qa_report.summary:
            key_ws.append([])
            key_ws.append(["QA Summary"])
            key_ws.append([qa_report.summary])

        # Assets sheet
        data_ws = wb.create_sheet("Assets")
        data_ws.append(_ALD_FIELDS)
        for a in assets:
            row = a.model_dump()
            row["name"] = row.pop("asset_name", "")
            for k, v in row.items():
                if isinstance(v, (dict, list)):
                    row[k] = json.dumps(v) if v else ""
            data_ws.append([row.get(k, "") for k in _ALD_FIELDS])

        wb.save(run_dir / "final_assets.xlsx")
    except ImportError:
        pass


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

_STAGES = ["profile", "discover", "scrape", "extract", "merge", "qa"]


async def run(
    isin: str | None,
    config: Config | None = None,
    stop_after: str | None = None,
    start_from: str | None = None,
    profile_file: str | None = None,
    verbose: bool = False,
    no_cache: bool = False,
) -> dict[str, Any]:
    """Run the full 6-stage pipeline for a company.

    Args:
        isin: Company ISIN or issuer_id. Optional when profile_file is given.
        config: Pipeline config. Defaults from env vars.
        stop_after: Stop after stage (profile/discover/scrape/extract/merge/qa).
        start_from: Resume from this stage, loading prior results from DB.
            Profile is always loaded (from DB or file) regardless.
        profile_file: Path to a JSON profile file. When set, Postgres is not
            queried for the profile and the issuer_id is taken from the file.

    Returns: dict with assets, qa_report, elapsed, stages_run, asset_count.
    """
    config = config or Config()
    start = time.monotonic()
    stages_run: list[str] = []
    costs = CostTracker()

    def _should_run(stage: str) -> bool:
        if start_from is None:
            return True
        return _STAGES.index(stage) >= _STAGES.index(start_from)

    # --- Stage 1: Profile (always runs — needed by all downstream stages) ---
    from .display import show_stage
    profile_start = time.monotonic()
    show_stage(1, "Profiling")

    import corp_profile
    from .display import show_detail, show_spinner

    if config.profile_research and _should_run("profile"):
        with show_spinner("Building profile from scratch..."):
            profile, context_doc = corp_profile.research(
                isin,
                config=config.profile_research_config(),
            )
    elif _should_run("profile"):
        needs_llm = config.profile_enrich or config.profile_web
        with show_spinner("Loading and enriching profile..."):
            profile, context_doc = corp_profile.run(
                identifier=isin,
                from_file=profile_file,
                enrich=config.profile_enrich,
                web=config.profile_web,
                enrich_config=config.profile_enrich_config() if needs_llm else None,
                web_config=config.profile_web_config() if config.profile_web else None,
                skip_cache=no_cache,
            )
    else:
        show_detail("Skipped enrichment (loaded from DB)")
        profile, context_doc = corp_profile.run(
            identifier=isin,
            from_file=profile_file,
            skip_cache=no_cache,
        )

    profile_elapsed = time.monotonic() - profile_start
    show_detail(f"Profile loaded in {profile_elapsed:.0f}s")

    # Use profile's issuer_id as the canonical identifier for all downstream stages
    issuer_id = profile.issuer_id or isin

    from .display import show_intro_panel
    show_intro_panel(profile.legal_name, issuer_id, profile)
    stages_run.append("profile")

    run_dir = _make_run_dir(issuer_id, profile.legal_name)
    _save_profile(run_dir, profile, context_doc)

    if stop_after == "profile":
        return _result([], None, start, stages_run)

    # --- Stage 2: Discover ---
    if _should_run("discover"):
        from .stages.discover import run_discover
        discovered_urls = await run_discover(issuer_id, context_doc, config, costs, verbose=verbose)
    else:
        from .db import get_connection, get_discovered_urls
        show_stage(2, "Loading cached URLs")
        conn = get_connection(config)
        try:
            discovered_urls = get_discovered_urls(conn, issuer_id)
        finally:
            conn.close()
        from .display import show_detail
        show_detail(f"Loaded {len(discovered_urls)} cached URLs from DB")

    stages_run.append("discover")
    _save_urls(run_dir, discovered_urls)

    if stop_after == "discover":
        return _result([], None, start, stages_run)

    # --- Set up RAG store ---
    from rag import RAGStore
    rag_store = RAGStore(config.corpgraph_db_url, config=config.rag_config())

    # --- Stage 3: Scrape ---
    from .stages.scrape import run_scrape

    pages = await run_scrape(issuer_id, discovered_urls, config, rag_store, costs, no_cache=no_cache)
    stages_run.append("scrape")
    _save_pages(run_dir, pages)

    if stop_after == "scrape":
        return _result([], None, start, stages_run)

    # --- Stage 4: Extract ---
    from .stages.extract import run_extract

    existing_summary = _build_existing_summary(profile)
    assets = await run_extract(
        issuer_id, profile.legal_name, pages, config, existing_summary, costs,
        profile=profile, skip_cache=no_cache or (start_from == "extract"),
    )
    stages_run.append("extract")
    _save_extractions(run_dir, assets)

    if stop_after == "extract":
        return _result(assets, None, start, stages_run)

    # --- Stage 5: Merge ---
    from .stages.merge import run_merge

    assets = await run_merge(issuer_id, assets, config, costs=costs)
    stages_run.append("merge")
    _save_merged(run_dir, assets)

    if stop_after == "merge":
        return _result(assets, None, start, stages_run)

    # --- Geocode assets missing coordinates ---
    assets_needing_geocode = [
        a for a in assets
        if a.geocodable and a.latitude is None and a.longitude is None and a.address
    ]
    if assets_needing_geocode:
        import logging
        logging.getLogger("geo_resolve").setLevel(logging.WARNING)
        try:
            from geo_resolve import Geocoder
            from .display import show_spinner
            gc = Geocoder()
            geocoded = 0
            with show_spinner(f"  Geocoding {len(assets_needing_geocode)} assets..."):
                for asset in assets_needing_geocode:
                    try:
                        lat, lon = gc.geocode(asset.address)
                        if lat and lon:
                            asset.latitude = lat
                            asset.longitude = lon
                            geocoded += 1
                    except Exception:
                        pass
            show_detail(f"Geocoded {geocoded}/{len(assets_needing_geocode)} assets")
        except ImportError:
            pass
        except Exception as e:
            show_detail(f"Geocoding failed: {e}")

    # --- Verify (optional) ---
    try:
        from .stages.verify import run_verify
        assets = await run_verify(assets, config, costs=costs)
        stages_run.append("verify")
    except ImportError:
        pass
    except Exception as e:
        show_detail(f"Verification skipped: {e}")

    # --- Stage 6: QA ---
    from .stages.qa import run_qa

    qa_report = await run_qa(issuer_id, context_doc, assets, config, rag_store, costs)
    stages_run.append("qa")
    _save_qa(run_dir, qa_report)

    # Flag assets if QA found high-severity issues
    high_flags = [f for f in (qa_report.coverage_flags or []) if f.severity == "high"]
    if high_flags:
        flag_text = "; ".join(f.description for f in high_flags)
        for asset in assets:
            asset.qa_flag = flag_text

    # Persist QA report (including coverage flags) to DB
    from .db import get_connection, save_qa_report
    conn = get_connection(config)
    try:
        save_qa_report(conn, issuer_id, qa_report.model_dump())
    finally:
        conn.close()

    # Save final output with QA flags (JSON + CSV + XLSX)
    _save_final(run_dir, assets, qa_report=qa_report)

    # --- Display results ---
    from .display import show_assets_table, show_cost_summary, show_coverage_flags, console
    from rich.text import Text

    # QA summary
    if qa_report.summary:
        from rich.panel import Panel
        console.print()
        console.print(Panel(
            qa_report.summary,
            title="[bold]QA Summary[/bold]",
            border_style="cyan",
            padding=(1, 2),
        ))

    show_assets_table(assets)
    show_coverage_flags(qa_report)
    elapsed = time.monotonic() - start
    show_cost_summary(
        stages_run=stages_run, url_count=len(discovered_urls),
        page_count=len(pages), asset_count=len(assets), elapsed=elapsed,
        costs=costs,
    )

    # Save cost breakdown
    if costs:
        cost_data = costs.summary()
        cost_data["elapsed_seconds"] = round(elapsed, 1)
        (run_dir / "cost_summary.json").write_text(
            json.dumps(cost_data, indent=2, default=str)
        )

    console.print()
    console.print(f"  [dim]Saved to[/dim] [bold]{run_dir}[/bold]")
    console.print()

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
