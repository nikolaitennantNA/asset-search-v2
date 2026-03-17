"""Cost tracking for LLM calls, API usage, and embeddings.

Tracks tokens per model and per stage, plus non-LLM API costs (Spider,
Exa, embeddings, Cohere rerank). Produces a summary at pipeline end.

Ported from asset-discovery v1 with updated pricing for 2026 models.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ── Pricing tables (USD per 1M tokens) ────────────────────────────────────────
# (input, output) — updated Mar 2026

_MODEL_PRICING: dict[str, tuple[float, float]] = {
    # OpenAI (per 1M tokens, updated Mar 2026)
    "gpt-5.4": (2.50, 15.00),
    "gpt-5.2": (1.75, 14.00),
    "gpt-5.1": (1.25, 10.00),
    "gpt-5": (1.25, 10.00),
    "gpt-5-mini": (0.25, 2.00),
    "gpt-5-nano": (0.05, 0.40),
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-4.1-nano": (0.10, 0.40),
    "gpt-4.1": (2.00, 8.00),
    # Anthropic / Bedrock (bare model ID after stripping provider + region)
    "anthropic.claude-opus-4-6-20250219-v1:0": (15.00, 75.00),
    "anthropic.claude-sonnet-4-6-20250514-v1:0": (3.00, 15.00),
    "anthropic.claude-haiku-4-5-20251001-v1:0": (0.80, 4.00),
    "anthropic.claude-sonnet-4-5-20250929-v1:0": (3.00, 15.00),
}

# Non-LLM API pricing (USD)
_FIRECRAWL_USD_PER_CREDIT = 0.001
_EXA_USD_PER_SEARCH = 0.015
_EMBEDDING_USD_PER_1M = 0.02  # text-embedding-3-small
_COHERE_USD_PER_RERANK = 0.002
_USD_TO_GBP = 0.741


def _strip_model_prefix(model: str) -> str:
    """Strip provider + region prefix: 'bedrock/us.anthropic.claude-...' → 'anthropic.claude-...'"""
    bare = model.split("/", 1)[-1] if "/" in model else model
    for region in ("global.", "us.", "eu.", "jp.", "apac."):
        if bare.startswith(region):
            bare = bare[len(region) :]
            break
    return bare


# ── Cost Tracker ──────────────────────────────────────────────────────────────


@dataclass
class CostTracker:
    """Accumulates costs across all pipeline stages and API calls."""

    # Per-model token counts: {"bedrock/opus": {"input": N, "output": M}}
    tokens_by_model: dict[str, dict[str, int]] = field(default_factory=dict)
    # Per-stage token counts: {"discover": {"input": N, "output": M, "calls": C}}
    tokens_by_stage: dict[str, dict[str, int]] = field(default_factory=dict)

    # Non-LLM API counters
    spider_pages: int = 0
    spider_cost_usd: float = 0.0
    exa_searches: int = 0
    embedding_tokens: int = 0
    cohere_rerank_calls: int = 0
    firecrawl_credits: int = 0

    # Totals (computed from per-model)
    total_input_tokens: int = 0
    total_output_tokens: int = 0

    def track_llm(
        self, model: str, input_tokens: int, output_tokens: int, stage: str = ""
    ) -> None:
        """Record an LLM call."""
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens

        entry = self.tokens_by_model.setdefault(model, {"input": 0, "output": 0})
        entry["input"] += input_tokens
        entry["output"] += output_tokens

        if stage:
            s = self.tokens_by_stage.setdefault(
                stage, {"input": 0, "output": 0, "calls": 0}
            )
            s["input"] += input_tokens
            s["output"] += output_tokens
            s["calls"] += 1

    def track_pydantic_ai(self, usage: Any, model: str = "", stage: str = "") -> None:
        """Record usage from a pydantic-ai agent run result."""
        if usage is None:
            return
        inp = getattr(usage, "input_tokens", None) or getattr(
            usage, "request_tokens", 0
        )
        out = getattr(usage, "output_tokens", None) or getattr(
            usage, "response_tokens", 0
        )
        self.track_llm(model, inp, out, stage)

    def track_litellm(self, response: Any, model: str = "", stage: str = "") -> None:
        """Record usage from a litellm completion response."""
        if not hasattr(response, "usage") or not response.usage:
            return
        inp = response.usage.prompt_tokens or 0
        out = response.usage.completion_tokens or 0
        self.track_llm(model, inp, out, stage)

    def track_spider(self, pages: int, cost_usd: float = 0.0) -> None:
        """Track Spider API usage with real USD cost."""
        self.spider_pages += pages
        self.spider_cost_usd += cost_usd

    def track_exa(self, searches: int = 1) -> None:
        self.exa_searches += searches

    def track_embedding(self, tokens: int) -> None:
        self.embedding_tokens += tokens

    def track_cohere_rerank(self, calls: int = 1) -> None:
        self.cohere_rerank_calls += calls

    def track_firecrawl(self, credits: int = 1) -> None:
        self.firecrawl_credits += credits

    # ── Cost calculation ──────────────────────────────────────────────────

    def llm_cost_usd(self) -> float:
        """Calculate total LLM cost from per-model token counts."""
        if not self.tokens_by_model:
            # Fallback: assume gpt-5-mini pricing
            return (
                self.total_input_tokens * 0.25
                + self.total_output_tokens * 2.00
            ) / 1_000_000

        usd = 0.0
        for model, counts in self.tokens_by_model.items():
            bare = _strip_model_prefix(model)
            inp_price, out_price = _MODEL_PRICING.get(
                bare, _MODEL_PRICING.get(model, (0.25, 2.00))
            )
            usd += (
                counts["input"] * inp_price + counts["output"] * out_price
            ) / 1_000_000
        return usd

    def api_cost_usd(self) -> float:
        """Calculate non-LLM API costs."""
        return (
            self.spider_cost_usd
            + self.exa_searches * _EXA_USD_PER_SEARCH
            + self.embedding_tokens * _EMBEDDING_USD_PER_1M / 1_000_000
            + self.cohere_rerank_calls * _COHERE_USD_PER_RERANK
            + self.firecrawl_credits * _FIRECRAWL_USD_PER_CREDIT
        )

    def total_cost_usd(self) -> float:
        return self.llm_cost_usd() + self.api_cost_usd()

    def total_cost_gbp(self) -> float:
        return self.total_cost_usd() * _USD_TO_GBP

    def summary(self) -> dict[str, Any]:
        """Return a summary dict for display or logging."""
        return {
            "total_usd": round(self.total_cost_usd(), 4),
            "total_gbp": round(self.total_cost_gbp(), 4),
            "llm_usd": round(self.llm_cost_usd(), 4),
            "api_usd": round(self.api_cost_usd(), 4),
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "by_model": dict(self.tokens_by_model),
            "by_stage": dict(self.tokens_by_stage),
            "spider_pages": self.spider_pages,
            "spider_cost_usd": round(self.spider_cost_usd, 6),
            "exa_searches": self.exa_searches,
            "embedding_tokens": self.embedding_tokens,
            "cohere_rerank_calls": self.cohere_rerank_calls,
            "firecrawl_credits": self.firecrawl_credits,
        }
