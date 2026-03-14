"""Tests for config.py — model conversion, resolution helpers, sub-module builders."""

import os
from unittest.mock import patch

import pytest

from asset_discovery.config import (
    _to_pydantic_ai_model,
    _resolve_str,
    _resolve_int,
    _resolve_float,
    _resolve_bool,
)


# ── _to_pydantic_ai_model ──────────────────────────────────────────────────


def test_to_pydantic_ai_model_bedrock():
    assert _to_pydantic_ai_model("bedrock/us.anthropic.claude-opus-4-6-v1") == "bedrock:us.anthropic.claude-opus-4-6-v1"


def test_to_pydantic_ai_model_openai():
    assert _to_pydantic_ai_model("openai/gpt-5") == "openai:gpt-5"


def test_to_pydantic_ai_model_anthropic():
    assert _to_pydantic_ai_model("anthropic/claude-opus-4-6") == "anthropic:claude-opus-4-6"


def test_to_pydantic_ai_model_litellm_fallback():
    assert _to_pydantic_ai_model("groq/llama-3-70b") == "litellm:groq/llama-3-70b"


def test_to_pydantic_ai_model_already_native():
    assert _to_pydantic_ai_model("bedrock:us.anthropic.claude-opus-4-6-v1") == "bedrock:us.anthropic.claude-opus-4-6-v1"


def test_to_pydantic_ai_model_bare_string():
    assert _to_pydantic_ai_model("gpt-5") == "gpt-5"


# ── _resolve_str ────────────────────────────────────────────────────────────


def test_resolve_str_env_wins():
    with patch.dict(os.environ, {"MY_KEY": "from_env"}):
        result = _resolve_str("MY_KEY", {"key": "from_toml"}, "key", "default")
        assert result == "from_env"


def test_resolve_str_toml_wins():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("MY_KEY", None)
        result = _resolve_str("MY_KEY", {"key": "from_toml"}, "key", "default")
        assert result == "from_toml"


def test_resolve_str_default_fallback():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("MY_KEY", None)
        result = _resolve_str("MY_KEY", {}, "key", "my_default")
        assert result == "my_default"


# ── _resolve_int / _resolve_float ───────────────────────────────────────────


def test_resolve_int_env_wins():
    with patch.dict(os.environ, {"MY_INT": "42"}):
        result = _resolve_int("MY_INT", {"val": 10}, "val", 1)
        assert result == 42


def test_resolve_float_env_wins():
    with patch.dict(os.environ, {"MY_FLOAT": "3.14"}):
        result = _resolve_float("MY_FLOAT", {"val": 1.0}, "val", 0.0)
        assert result == pytest.approx(3.14)


# ── _resolve_bool ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("val", ["true", "1", "yes", "True", "YES"])
def test_resolve_bool_true_strings(val):
    with patch.dict(os.environ, {"MY_BOOL": val}):
        assert _resolve_bool("MY_BOOL", {}, "b", False) is True


def test_resolve_bool_toml_layer():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("MY_BOOL", None)
        assert _resolve_bool("MY_BOOL", {"b": True}, "b", False) is True
        assert _resolve_bool("MY_BOOL", {"b": False}, "b", True) is False


# ── Sub-module config builders ──────────────────────────────────────────────


def test_scraper_config_builder():
    from asset_discovery.config import Config
    cfg = Config()
    sc = cfg.scraper_config()
    assert sc.base_url == cfg.scraper_base_url
    assert sc.batch_limit == cfg.scraper_batch_limit
    assert sc.strategy == cfg.scraper_strategy


def test_extractor_config_builder():
    from asset_discovery.config import Config
    cfg = Config()
    ec = cfg.extractor_config()
    assert ec.max_batch_tokens == cfg.extractor_max_batch_tokens
    assert ec.max_page_tokens == cfg.extractor_max_page_tokens
    assert ec.max_retries == cfg.extractor_max_retries
