"""Stage 7: QA -- pydantic-ai agent for coverage evaluation.

Evaluates the quality and completeness of discovered assets against
expected coverage. Triggers deep search for missing asset types or regions.
"""

from __future__ import annotations

from ..models import Asset, QAReport


async def run_qa(
    assets: list[Asset],
    config: object,
) -> QAReport:
    """Evaluate asset coverage and quality.

    Returns a QAReport with coverage analysis and flags.
    """
    raise NotImplementedError("Stage 7 (QA) not yet implemented")
