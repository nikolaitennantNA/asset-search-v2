"""Pydantic models for the asset discovery pipeline — TREX ALD aligned."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class Asset(BaseModel):
    """Asset extraction model — TREX ALD aligned."""

    # --- TREX fields ---
    asset_name: str
    entity_name: str
    entity_isin: str = ""
    parent_name: str = ""
    parent_isin: str = ""
    entity_stake_pct: float | None = None
    latitude: float | None = None
    longitude: float | None = None
    status: str = ""
    capacity: float | None = None
    capacity_units: str = ""
    asset_type_raw: str = ""
    supplementary_details: dict = {}

    # --- Set by pipeline, not by LLM ---
    asset_id: str = ""
    naturesense_asset_type: str = ""
    industry_code: str = ""
    date_researched: str = ""
    attribution_source: str = ""

    # --- Pipeline working fields (not in TREX export) ---
    address: str = ""
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
    """URL discovered by the discover agent with optional scrape configuration.

    The agent sets structured fields to control how the scrape stage processes
    each URL. Freeform notes remain for human-readable context.
    """
    url: str
    category: str
    notes: str = ""

    # Scrape config — agent sets these based on what it learned about the page/domain
    strategy: Literal["http", "browser"] | None = None  # None = use pipeline default
    proxy_mode: Literal["auto", "datacenter", "residential"] | None = None
    wait_for: str | None = None
    js_code: str | None = None
    scan_full_page: bool = False
    screenshot: bool = False
