"""Pydantic models for the asset discovery pipeline — TREX ALD aligned."""

from __future__ import annotations

import csv
from pathlib import Path

from typing import Any

from pydantic import BaseModel, Field


_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"


# --- NatureSense asset types — loaded from data/naturesense_asset_types.csv ---

def _load_naturesense_types() -> list[dict[str, str]]:
    """Load NatureSense asset types from CSV."""
    path = _DATA_DIR / "naturesense_asset_types.csv"
    with path.open(newline="", encoding="utf-8") as f:
        return [
            {
                "name": row["asset_type"].strip(),
                "buffer_m": row.get("buffer_distance_m", ""),
                "description": row.get("description", ""),
            }
            for row in csv.DictReader(f)
            if row.get("asset_type", "").strip()
        ]


NATURESENSE_DATA: list[dict[str, str]] = _load_naturesense_types()
NATURESENSE_TYPES: list[str] = [n["name"] for n in NATURESENSE_DATA]


def naturesense_reference_block() -> str:
    """Build a NatureSense reference string for inclusion in extraction prompts."""
    lines = []
    for n in NATURESENSE_DATA:
        line = f"- {n['name']}"
        if n["description"]:
            line += f": {n['description']}"
        lines.append(line)
    return "\n".join(lines)


# --- GICS 6-digit industry codes — loaded from data/gics_industries.csv ---

def _load_gics_industries() -> list[dict[str, str]]:
    """Load GICS industries from CSV."""
    path = _DATA_DIR / "gics_industries.csv"
    with path.open(newline="", encoding="utf-8") as f:
        return [
            {
                "code": row["industry_code"],
                "name": row["industry_name"],
                "description": row.get("industry_description", ""),
            }
            for row in csv.DictReader(f)
            if row.get("industry_code")
        ]


GICS_INDUSTRIES: list[dict[str, str]] = _load_gics_industries()
GICS_CODES: list[str] = [g["code"] for g in GICS_INDUSTRIES]


def gics_reference_block() -> str:
    """Build a GICS reference string for inclusion in extraction prompts."""
    lines = []
    for g in GICS_INDUSTRIES:
        line = f"- {g['code']}: {g['name']}"
        if g["description"]:
            line += f" — {g['description']}"
        lines.append(line)
    return "\n".join(lines)


class ExtractedAsset(BaseModel):
    """Schema passed to the extraction LLM — only fields the model should fill."""

    asset_name: str = Field(
        description="Facility or site proper name, including any identifier or number "
        "(e.g. 'Hornsea 2 Offshore Wind Farm', 'Denver Store #102', 'Plant 3'). "
        "Do not include city, state, or country unless it's part of the official name.",
    )
    entity_name: str = Field(
        description="Company or subsidiary that directly owns/operates this asset.",
    )
    entity_isin: str = Field(
        default="",
        description="ISIN of the owning entity, if mentioned.",
    )
    parent_name: str = Field(
        default="",
        description="Parent company of entity_name, only when entity_name is a subsidiary. "
        "Leave blank if entity_name is the top-level company.",
    )
    parent_isin: str = Field(
        default="",
        description="ISIN of the parent company, if mentioned.",
    )
    entity_stake_pct: float | None = Field(
        default=None,
        description="Ownership percentage (0-100), if mentioned.",
    )
    latitude: float | None = Field(
        default=None,
        description="Decimal latitude (-90 to +90).",
    )
    longitude: float | None = Field(
        default=None,
        description="Decimal longitude (-180 to +180).",
    )
    address: str = Field(
        default="",
        description="Full address if available, or a location description for assets "
        "without a street address (e.g. 'North Sea, 50 km off Lower Saxony coast').",
    )
    status: str = Field(
        default="",
        description="Operating, Construction, Planned, or Cancelled.",
    )
    capacity: float | None = Field(
        default=None,
        description="Numeric capacity value only (e.g. 500, not '500 MW'). "
        "For facilities with multiple output types, put the primary here "
        "and additional capacities in supplementary_details.",
    )
    capacity_units: str = Field(
        default="",
        description="Unit for capacity (MW, GW, tonnes, barrels, sq ft, etc.).",
    )
    asset_type_raw: str = Field(
        default="",
        description="Free text asset type (e.g. 'cement plant', 'offshore wind farm', "
        "'asphalt batching plant').",
    )
    naturesense_asset_type: str = Field(
        default="",
        description="Classify into one of: " + ", ".join(NATURESENSE_TYPES)
        + ". See the NatureSense reference in the prompt for descriptions.",
    )
    industry_code: str = Field(
        default="",
        description="6-digit GICS industry code for this asset. "
        "See the GICS reference in the prompt for valid codes and descriptions.",
    )
    supplementary_details: dict[str, Any] = Field(
        default_factory=dict,
        description="Dict of additional context with descriptive keys "
        "(e.g. fuel_type, year_built, technology, additional_capacity).",
    )


class Asset(ExtractedAsset):
    """Full asset with pipeline-set fields added after extraction."""

    asset_id: str = ""
    date_researched: str = ""
    attribution_source: str = ""
    source_url: str = ""
    domain_source: str = ""
    qa_flag: str = ""  # set by QA: "underreported", "low_confidence", etc.


class CoverageFlag(BaseModel):
    flag_type: str
    description: str
    severity: str = "medium"


class QAReport(BaseModel):
    quality_score: float = 0.0
    missing_types: list[str] = []
    missing_regions: list[str] = []
    issues: list[str] = []
    should_enrich: bool = False
    coverage_flags: list[CoverageFlag] = []


class DiscoveredUrl(BaseModel):
    url: str
    category: str
    notes: str | None = None
    # Spider automation scripts for exceptional pages requiring interaction.
    # e.g. {"*": [{"Click": "button.show-all-locations"}, {"Wait": 2000}]}
    # Only set when the discover agent finds a page that genuinely requires
    # clicking a button, expanding a section, etc. to reveal content.
    # For 99% of URLs this stays None and Spider's smart mode handles everything.
    automation_scripts: dict | None = None
