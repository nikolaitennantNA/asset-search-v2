"""Stage 6: Validate -- dedup all sources + enrich + clean.

Deduplicates assets across extraction batches, enriches with geocoding
and metadata, and cleans up field values.
"""

from __future__ import annotations

from ..models import Asset


async def run_validate(
    assets: list[Asset],
    config: object,
) -> list[Asset]:
    """Deduplicate, enrich, and clean extracted assets.

    Returns the validated asset list.
    """
    raise NotImplementedError("Stage 6 (Validate) not yet implemented")
