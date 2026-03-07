"""Stage 5a: Extract -- instructor structured extraction from scraped pages.

Batches pages into LLM-context-sized groups by token count, then extracts
physical assets via instructor structured output.

Batching logic ported from asset-search v1.
"""

from __future__ import annotations

import tiktoken

from ..models import Asset

_TOKENIZER = tiktoken.get_encoding("cl100k_base")


def _split_oversized_page(
    page: dict,
    window_tokens: int = 35_000,
    overlap_tokens: int = 5_000,
) -> list[dict]:
    """Split an oversized page into overlapping windows.

    Each window is a copy of the page with truncated text so it fits within
    a single extraction batch. Overlap ensures assets near a split boundary
    appear in at least one complete window.
    """
    encoded = _TOKENIZER.encode(page["text"])
    stride = window_tokens - overlap_tokens
    windows: list[dict] = []
    for start in range(0, len(encoded), stride):
        chunk_ids = encoded[start : start + window_tokens]
        text = _TOKENIZER.decode(chunk_ids)
        n_windows = (len(encoded) + stride - 1) // stride
        suffix = f" (window {len(windows) + 1}/{n_windows})"
        windows.append({
            **page,
            "text": text,
            "tokens": len(chunk_ids),
            "page_title": page.get("page_title", "") + suffix,
        })
        if start + window_tokens >= len(encoded):
            break
    return windows


def build_batches(
    pages: list[dict],
    max_tokens_per_batch: int = 65_000,
) -> list[list[dict]]:
    """Group pages into LLM-context-sized batches.

    Pages are added to a batch until the token budget is exceeded, then a
    new batch starts. Single pages exceeding the budget are split into
    overlapping windows via _split_oversized_page.
    """
    batches: list[list[dict]] = []
    current_batch: list[dict] = []
    current_tokens = 0

    for page in pages:
        page_tokens = page.get("tokens", 0)
        if not page_tokens:
            page_tokens = len(_TOKENIZER.encode(page.get("text", "")))
            page["tokens"] = page_tokens

        if page_tokens > max_tokens_per_batch:
            if current_batch:
                batches.append(current_batch)
                current_batch = []
                current_tokens = 0
            for window in _split_oversized_page(
                page, window_tokens=max_tokens_per_batch - 5_000
            ):
                batches.append([window])
            continue

        if current_tokens + page_tokens > max_tokens_per_batch:
            batches.append(current_batch)
            current_batch = []
            current_tokens = 0

        current_batch.append(page)
        current_tokens += page_tokens

    if current_batch:
        batches.append(current_batch)

    return batches


async def extract_batch(
    pages: list[dict], profile_context: str
) -> list[Asset]:
    """Extract assets from a batch of pages using instructor.

    TODO: implement with instructor structured output.
    """
    raise NotImplementedError("Stage 5 extraction not yet implemented")
