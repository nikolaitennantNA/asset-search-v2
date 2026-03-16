"""Pydantic models for the asset discovery pipeline — TREX ALD aligned."""

from __future__ import annotations


from pydantic import BaseModel, Field


# naturesense_asset_type enum — used in Field description below
NATURESENSE_TYPES = [
    "Agricultural & Food Production", "Electricity Distribution", "Energy Production",
    "Heavy Industrial & Manufacturing", "IT Facility/Data Center", "Mining Operations",
    "Office/Housing", "Oil & Gas Facilities",
    "Other (5km buffer area of influence)", "Other (10km buffer area of influence)",
    "Other (20km buffer area of influence)", "Other (50km buffer area of influence)",
    "R&D Facility", "Retail", "Transportation and Logistics Facility", "Warehouse",
]


class Asset(BaseModel):
    """Asset extraction model — TREX ALD aligned."""

    # --- Extracted by LLM ---
    asset_name: str = Field(
        description="Facility or site proper name (e.g. 'Hornsea 2 Offshore Wind Farm'). "
        "Do not include city, state, or country in the name.",
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
        description="Classify into one of: " + ", ".join(NATURESENSE_TYPES),
    )
    supplementary_details: dict[str, str] = Field(
        default_factory=dict,
        description="Dict of additional context with descriptive keys "
        "(e.g. fuel_type, year_built, technology, additional_capacity).",
    )

    # --- Set by pipeline, not by LLM ---
    asset_id: str = ""
    industry_code: str = ""
    date_researched: str = ""
    attribution_source: str = ""
    source_url: str = ""
    domain_source: str = ""


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
