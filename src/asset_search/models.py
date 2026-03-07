from __future__ import annotations

from pydantic import BaseModel


class Asset(BaseModel):
    """A physical asset discovered for a company. Matches ALD assets table schema."""

    asset_name: str
    entity_name: str = ""
    entity_isin: str = ""
    parent_name: str = ""
    parent_isin: str = ""
    address: str = ""
    latitude: float | None = None
    longitude: float | None = None
    asset_type: str = ""
    status: str = ""
    capacity: float | None = None
    capacity_units: str = ""
    ownership_pct: float | None = None
    source_url: str = ""
    domain_source: str = ""
    supplementary_details: dict = {}


class QAReport(BaseModel):
    """Output of QA stage."""

    quality_score: float = 0.0
    missing_types: list[str] = []
    missing_regions: list[str] = []
    issues: list[str] = []
    should_enrich: bool = False
    coverage_flags: list[str] = []


class CoverageFlag(BaseModel):
    """A flag raised when coverage is clearly insufficient."""

    issuer_id: str
    flag_type: str
    severity: str = "warning"
    message: str
    details: dict = {}
