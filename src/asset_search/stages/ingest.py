"""Stage 5b: Ingest -- Chonkie chunk + embed into pgvector.

Takes scraped page content, chunks it with Chonkie, embeds chunks via
Cohere, and stores them in pgvector for retrieval-augmented extraction.
"""

from __future__ import annotations


async def run_ingest(
    pages: list[dict],
    config: object,
) -> int:
    """Chunk, embed, and store pages in pgvector.

    Returns the number of chunks stored.
    """
    raise NotImplementedError("Stage 5b (Ingest) not yet implemented")
