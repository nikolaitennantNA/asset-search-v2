"""Tests for cost.py — model prefix stripping, cost tracking, pricing math."""

from types import SimpleNamespace

import pytest

from asset_discovery.cost import _strip_model_prefix, CostTracker


# ── _strip_model_prefix ────────────────────────────────────────────────────


def test_strip_model_prefix_bedrock():
    assert _strip_model_prefix("bedrock/us.anthropic.claude-opus-4-6-20250219-v1:0") == "anthropic.claude-opus-4-6-20250219-v1:0"


def test_strip_model_prefix_openai():
    assert _strip_model_prefix("openai/gpt-5") == "gpt-5"


def test_strip_model_prefix_no_prefix():
    assert _strip_model_prefix("gpt-5-mini") == "gpt-5-mini"


@pytest.mark.parametrize("region", ["us.", "global.", "eu.", "jp.", "apac."])
def test_strip_model_prefix_all_regions(region):
    model = f"bedrock/{region}anthropic.claude-opus-4-6-v1"
    assert _strip_model_prefix(model) == "anthropic.claude-opus-4-6-v1"


# ── track_llm ───────────────────────────────────────────────────────────────


def test_track_llm_accumulates():
    ct = CostTracker()
    ct.track_llm("model-a", 100, 50, stage="discover")
    ct.track_llm("model-a", 200, 100, stage="discover")
    assert ct.tokens_by_model["model-a"] == {"input": 300, "output": 150}
    assert ct.tokens_by_stage["discover"]["calls"] == 2
    assert ct.total_input_tokens == 300
    assert ct.total_output_tokens == 150


def test_track_llm_no_stage():
    ct = CostTracker()
    ct.track_llm("model-a", 100, 50)
    assert ct.tokens_by_model["model-a"] == {"input": 100, "output": 50}
    assert ct.tokens_by_stage == {}


# ── track_pydantic_ai ──────────────────────────────────────────────────────


def test_track_pydantic_ai_input_output():
    ct = CostTracker()
    usage = SimpleNamespace(input_tokens=100, output_tokens=50)
    ct.track_pydantic_ai(usage, model="m", stage="qa")
    assert ct.total_input_tokens == 100
    assert ct.total_output_tokens == 50


def test_track_pydantic_ai_request_response_fallback():
    ct = CostTracker()
    usage = SimpleNamespace(request_tokens=200, response_tokens=80)
    ct.track_pydantic_ai(usage, model="m", stage="qa")
    assert ct.total_input_tokens == 200
    assert ct.total_output_tokens == 80


def test_track_pydantic_ai_none_usage():
    ct = CostTracker()
    ct.track_pydantic_ai(None, model="m")
    assert ct.total_input_tokens == 0


# ── track_litellm ──────────────────────────────────────────────────────────


def test_track_litellm_with_usage():
    ct = CostTracker()
    resp = SimpleNamespace(usage=SimpleNamespace(prompt_tokens=300, completion_tokens=120))
    ct.track_litellm(resp, model="m", stage="merge")
    assert ct.total_input_tokens == 300
    assert ct.total_output_tokens == 120


def test_track_litellm_no_usage():
    ct = CostTracker()
    resp = SimpleNamespace()  # no .usage attribute
    ct.track_litellm(resp, model="m")
    assert ct.total_input_tokens == 0


# ── Cost calculations ──────────────────────────────────────────────────────


def test_llm_cost_usd_known_model():
    """Opus: $15/1M input, $75/1M output."""
    ct = CostTracker()
    ct.track_llm("bedrock/us.anthropic.claude-opus-4-6-20250219-v1:0", 1_000_000, 1_000_000)
    assert ct.llm_cost_usd() == pytest.approx(15.0 + 75.0)


def test_llm_cost_usd_unknown_model():
    """Unknown model falls back to hardcoded (0.25, 2.00) per 1M."""
    ct = CostTracker()
    ct.track_llm("unknown/mystery-model", 1_000_000, 1_000_000)
    assert ct.llm_cost_usd() == pytest.approx(0.25 + 2.00)


def test_api_cost_usd():
    ct = CostTracker()
    ct.track_crawl4ai(100)
    ct.track_exa(10)
    ct.track_embedding(1_000_000)
    ct.track_cohere_rerank(5)
    ct.track_firecrawl(20)
    expected = (
        100 * 0.001      # crawl4ai
        + 10 * 0.015     # exa
        + 1.0 * 0.02     # embedding (1M tokens)
        + 5 * 0.002      # cohere
        + 20 * 0.001     # firecrawl
    )
    assert ct.api_cost_usd() == pytest.approx(expected)


def test_track_firecrawl():
    ct = CostTracker()
    ct.track_firecrawl(5)
    assert ct.firecrawl_credits == 5
    ct.track_firecrawl(3)
    assert ct.firecrawl_credits == 8


def test_total_cost_usd():
    ct = CostTracker()
    ct.track_llm("gpt-5-mini", 1_000_000, 1_000_000)
    ct.track_crawl4ai(10)
    assert ct.total_cost_usd() == pytest.approx(ct.llm_cost_usd() + ct.api_cost_usd())


def test_total_cost_gbp():
    ct = CostTracker()
    ct.track_llm("gpt-5-mini", 1_000_000, 0)
    assert ct.total_cost_gbp() == pytest.approx(ct.total_cost_usd() * 0.741)


def test_summary_dict_keys():
    ct = CostTracker()
    summary = ct.summary()
    expected_keys = {
        "total_usd", "total_gbp", "llm_usd", "api_usd",
        "total_input_tokens", "total_output_tokens",
        "by_model", "by_stage",
        "crawl4ai_pages", "exa_searches", "embedding_tokens",
        "cohere_rerank_calls", "firecrawl_credits",
    }
    assert set(summary.keys()) == expected_keys
