"""Stage 3: Discover -- pydantic-ai agent that finds and classifies URLs.

Uses a single agent to:
1. Explore known domains (sitemaps, crawling)
2. Search for additional asset-relevant URLs
3. Classify all URLs by type and priority tier
"""

from __future__ import annotations


async def run_discover(
    isin: str,
    enrichment_data: dict,
    config: object,
) -> list[dict]:
    """Discover and classify asset-relevant URLs.

    Returns a list of classified URL dicts with fields:
        url, domain, domain_source, page_type, priority_tier
    """
    raise NotImplementedError("Stage 3 (Discover) not yet implemented")
