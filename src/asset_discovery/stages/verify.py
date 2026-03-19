"""Stage 7 (optional): Geo-verify — multi-signal location verification + correction.

Runs geo-verify's full pipeline on merged assets to produce confidence scores,
LLM verdicts, and coordinate corrections. Gracefully skips if geo-verify not installed.

Assets gain: verification_confidence, verification_verdict, corrected coords (optional).
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import Config
    from ..cost import CostTracker
    from ..models import Asset

log = logging.getLogger(__name__)


async def run_verify(
    assets: list[Asset],
    config: Config,
    *,
    costs: CostTracker | None = None,
    model_path: str | None = None,
    apply_corrections: bool = False,
    min_correction_confidence: float = 0.7,
) -> list[Asset]:
    """Run geo-verify on merged assets.

    Args:
        assets: Merged assets from the pipeline.
        config: Pipeline config.
        costs: Cost tracker.
        model_path: Path to trained CatBoost model (.cbm). If None, signals-only.
        apply_corrections: Whether to apply coordinate corrections to assets.
        min_correction_confidence: Only apply corrections above this confidence.

    Returns:
        Assets with verification metadata added to extra fields.
    """
    try:
        from geo_verify.config import GeoVerifyConfig
        from geo_verify.pipeline import VerifyPipeline
        from geo_verify.signals.registry import build_signals
        from geo_verify.models import Asset as GVAsset
    except ImportError:
        log.info("geo-verify not installed — skipping verification stage")
        return assets

    if not assets:
        return assets

    gv_config = GeoVerifyConfig.load()

    # Build tiered signal pipeline
    tiers = build_signals(
        gv_config,
        spatial=True,
        geocode=True,
        reprompt=bool(gv_config.reprompt_api_key),
        vlm=bool(gv_config.openai_api_key) and not gv_config.vlm_uncertain_only,
    )

    pipeline = VerifyPipeline(tiers=tiers)

    # Convert asset-discovery Assets to geo-verify Assets
    gv_assets = []
    for a in assets:
        if a.latitude is None or a.longitude is None:
            gv_assets.append(None)
            continue
        gv_assets.append(GVAsset(
            asset_id=a.asset_id,
            latitude=a.latitude,
            longitude=a.longitude,
            name=a.asset_name,
            entity_name=a.entity_name,
            asset_type=getattr(a, "asset_type_raw", ""),
            address=getattr(a, "address", ""),
        ))

    # Filter to only assets with coords
    valid_indices = [i for i, ga in enumerate(gv_assets) if ga is not None]
    valid_gv_assets = [gv_assets[i] for i in valid_indices]

    if not valid_gv_assets:
        log.info("No assets with coordinates to verify")
        return assets

    log.info("Verifying %d assets with %d signals",
             len(valid_gv_assets), pipeline.signal_count)

    # Run full pipeline
    reports = await pipeline.run_full(
        valid_gv_assets,
        gv_config,
        model_path=model_path,
        run_llm_verify=True,
        run_correction=gv_config.correction_enabled,
    )

    # Map results back to original assets
    for j, report in enumerate(reports):
        i = valid_indices[j]
        asset = assets[i]

        # Store verification results as QA metadata
        if report.confidence >= 0:
            asset.qa_flag = (asset.qa_flag + "; " if asset.qa_flag else "") + \
                f"verify:{report.confidence:.2f}"

        # Apply corrections if enabled and confident enough
        if (apply_corrections
                and report.corrected_lat is not None
                and report.correction_confidence >= min_correction_confidence
                and not report.correction_needs_review):
            asset.latitude = report.corrected_lat
            asset.longitude = report.corrected_lon
            log.info("Corrected %s: %.6f,%.6f → %.6f,%.6f (%.0fm, %s)",
                     asset.asset_name,
                     report.feature_row.get("latitude", 0),
                     report.feature_row.get("longitude", 0),
                     report.corrected_lat, report.corrected_lon,
                     report.correction_distance_m,
                     report.correction_method)

    verified_count = sum(1 for r in reports if r.verified)
    corrected_count = sum(1 for r in reports if r.corrected_lat is not None)
    log.info("Verification complete: %d/%d verified, %d corrections proposed",
             verified_count, len(reports), corrected_count)

    return assets
