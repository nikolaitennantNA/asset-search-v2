"""Stage 2: Collect -- calls corp-enrich to gather enrichment data.

Retrieves company metadata, subsidiaries, domains, and other enrichment
data from the corp-enrich service to seed URL discovery.
"""

from __future__ import annotations


async def run_collect(isin: str, config: object) -> dict:
    """Collect enrichment data for a company via corp-enrich.

    Returns a dict with company metadata, subsidiaries, domains, etc.
    """
    raise NotImplementedError("Stage 2 (Collect) not yet implemented")
