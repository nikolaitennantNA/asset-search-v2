"""GICS mapping lookup — maps asset_type_raw to naturesense category + industry code.

Two-tier matching:
  1. Exact match (case-insensitive dict lookup) — free, instant.
  2. Semantic embedding fallback (fastembed + cosine similarity) — handles
     paraphrases like "Underground gold mining operation" → "Gold mining company".
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from fastembed import TextEmbedding

logger = logging.getLogger(__name__)

_CSV_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "gics_mapping.csv"

# Minimum cosine similarity for semantic match. Below this, we return None
# and let the LLM-assigned naturesense stand. Tuned empirically: 0.75 catches
# good matches like "Cement manufacturing facility" → "Cement manufacturer" (0.91)
# while rejecting bad ones like "Lithium brine evaporation" → "Hydroelectric" (0.69).
_SEMANTIC_THRESHOLD = 0.75


@dataclass(frozen=True, slots=True)
class GICSMatch:
    """Result of a GICS mapping lookup."""

    naturesense_asset_type: str
    industry_code: str
    gics_industry: str
    score: float = 1.0  # 1.0 for exact match, <1.0 for semantic match


class GICSMapping:
    """Two-tier lookup from asset_type_raw to naturesense + GICS industry code.

    Tier 1: Exact case-insensitive dict lookup.
    Tier 2: Semantic embedding similarity via fastembed (bge-small-en-v1.5).

    Uses the RAF-reviewed naturesense category when available (expert override),
    otherwise falls back to the original asset_type_category.
    """

    def __init__(
        self,
        csv_path: str | Path | None = None,
        semantic_threshold: float = _SEMANTIC_THRESHOLD,
    ) -> None:
        self._exact_map: dict[str, GICSMatch] = {}
        self._labels: list[str] = []
        self._matches: list[GICSMatch] = []
        self._label_vecs: np.ndarray | None = None
        self._model: TextEmbedding | None = None
        self._threshold = semantic_threshold
        self._load(Path(csv_path) if csv_path else _CSV_PATH)

    def _load(self, path: Path) -> None:
        with path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                asset_type = row["asset_type"].strip()
                if not asset_type:
                    continue

                # RAF-reviewed column is the expert override; prefer it when present
                naturesense = (
                    row.get("asset_type_category_raf_reviewed", "").strip()
                    or row.get("asset_type_category", "").strip()
                )
                industry_code = row.get("industry_code", "").strip()
                gics_industry = row.get("asset_gics_industry", "").strip()

                if not naturesense:
                    continue

                match = GICSMatch(
                    naturesense_asset_type=naturesense,
                    industry_code=industry_code,
                    gics_industry=gics_industry,
                )

                self._exact_map[asset_type.lower()] = match
                self._labels.append(asset_type)
                self._matches.append(match)

    def _ensure_embeddings(self) -> None:
        """Lazy-init: encode all labels on first semantic lookup."""
        if self._label_vecs is not None:
            return
        logger.info("GICS: encoding %d labels with bge-small-en-v1.5...", len(self._labels))
        self._model = TextEmbedding("BAAI/bge-small-en-v1.5")
        self._label_vecs = np.array(list(self._model.embed(self._labels)))
        logger.info("GICS: label embeddings ready, shape %s", self._label_vecs.shape)

    def lookup(self, asset_type_raw: str) -> GICSMatch | None:
        """Look up a raw asset type string.

        Tries exact match first, then falls back to semantic similarity.
        Returns None if no match meets the threshold.
        """
        if not asset_type_raw:
            return None

        key = asset_type_raw.strip().lower()

        # Tier 1: exact match
        exact = self._exact_map.get(key)
        if exact is not None:
            return exact

        # Tier 2: semantic embedding match
        return self._semantic_lookup(asset_type_raw.strip())

    def _semantic_lookup(self, query: str) -> GICSMatch | None:
        self._ensure_embeddings()
        assert self._model is not None
        assert self._label_vecs is not None

        query_vec = np.array(list(self._model.embed([query])))
        sims = (query_vec @ self._label_vecs.T).flatten()
        best_idx = int(sims.argmax())
        best_score = float(sims[best_idx])

        if best_score < self._threshold:
            return None

        orig = self._matches[best_idx]
        return GICSMatch(
            naturesense_asset_type=orig.naturesense_asset_type,
            industry_code=orig.industry_code,
            gics_industry=orig.gics_industry,
            score=best_score,
        )

    def __len__(self) -> int:
        return len(self._exact_map)

    def __contains__(self, asset_type_raw: str) -> bool:
        return self.lookup(asset_type_raw) is not None


# Module-level singleton — loaded once on first import.
_singleton: GICSMapping | None = None


def get_gics_mapping() -> GICSMapping:
    """Return the shared GICSMapping instance (lazy-loaded singleton)."""
    global _singleton
    if _singleton is None:
        _singleton = GICSMapping()
    return _singleton
