"""Pipeline configuration — single source of truth for all tunables.

Secrets (API keys, DB URLs) come from .env / environment variables.
Config (models, caps, sub-module settings) comes from config.toml with env var overrides.

Resolution order per field: env var > config.toml > hardcoded default.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


# ── TOML loading ──────────────────────────────────────────────────────────────


def _load_toml() -> dict:
    """Load config.toml from CWD or package root."""
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]

    for candidate in [Path("config.toml"), Path(__file__).resolve().parents[3] / "config.toml"]:
        if candidate.exists():
            with open(candidate, "rb") as f:
                return tomllib.load(f)
    return {}


# ── Resolution helpers ────────────────────────────────────────────────────────


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _resolve_str(env_key: str, toml_section: dict, toml_key: str, default: str) -> str:
    return os.environ.get(env_key) or toml_section.get(toml_key, default)


def _resolve_int(env_key: str, toml_section: dict, toml_key: str, default: int) -> int:
    env_val = os.environ.get(env_key)
    if env_val is not None:
        return int(env_val)
    return int(toml_section.get(toml_key, default))


def _resolve_float(env_key: str, toml_section: dict, toml_key: str, default: float) -> float:
    env_val = os.environ.get(env_key)
    if env_val is not None:
        return float(env_val)
    return float(toml_section.get(toml_key, default))


def _to_pydantic_ai_model(litellm_model: str) -> str:
    """Convert a litellm model string to pydantic-ai ``provider:model`` format.

    pydantic-ai expects ``provider:model`` (colon-separated).  Litellm strings
    like ``bedrock/us.anthropic.claude-…`` use slashes.  For providers with
    native pydantic-ai support (bedrock, openai, anthropic) we convert to the
    native ``provider:model`` format.  Everything else falls back to litellm.
    """
    # Already in pydantic-ai native format (e.g. "anthropic:claude-…")
    if ":" in litellm_model and "/" not in litellm_model.split(":")[0]:
        return litellm_model
    # Litellm slash format — convert native providers, wrap the rest
    if "/" in litellm_model:
        provider, model = litellm_model.split("/", 1)
        # These providers have native pydantic-ai support — use colon format
        if provider in ("bedrock", "openai", "anthropic"):
            return f"{provider}:{model}"
        return f"litellm:{litellm_model}"
    return litellm_model


def _resolve_bool(env_key: str, toml_section: dict, toml_key: str, default: bool) -> bool:
    env_val = os.environ.get(env_key)
    if env_val is not None:
        return env_val.lower() in ("true", "1", "yes")
    toml_val = toml_section.get(toml_key)
    if toml_val is not None:
        return bool(toml_val)
    return default


# ── Config ────────────────────────────────────────────────────────────────────


@dataclass
class Config:
    """Master pipeline config. Constructed once at startup, threaded to all stages.

    Secrets come from env vars only. Everything else from config.toml with env overrides.
    """

    # ── Secrets (env vars only, never in config.toml) ─────────────────────
    corpgraph_db_url: str = ""
    crawl4ai_api_key: str = ""
    firecrawl_api_key: str = ""
    openai_api_key: str = ""
    cohere_api_key: str = ""
    exa_api_key: str = ""

    # ── Models (config.toml [models], env override) ───────────────────────
    profile_model: str = ""
    discover_model: str = ""
    extract_model: str = ""
    merge_model: str = ""
    qa_model: str = ""

    # ── corp-profile (config.toml [profile]) ──────────────────────────────
    profile_enrich: bool = False
    profile_web: bool = False
    profile_research_model: str = ""
    profile_research_provider: str = "auto"
    profile_enrich_model: str = ""
    profile_web_model: str = ""
    profile_web_provider: str = "auto"

    # ── web-scraper (config.toml [scraper]) ───────────────────────────────
    # Keys mirror ScraperConfig: base_url, batch_limit, poll_interval_s,
    # client_timeout_s, strategy, default_timeout_ms, default_proxy
    scraper_base_url: str = "https://api.crawl4ai.com"
    scraper_batch_limit: int = 10
    scraper_poll_interval_s: float = 2.0
    scraper_client_timeout_s: float = 120.0
    scraper_strategy: str = "http"
    scraper_default_timeout_ms: int = 30_000
    scraper_default_proxy: bool = True
    scraper_default_proxy_mode: str = "auto"
    scraper_remove_overlays: bool = True

    # ── doc-extractor (config.toml [extractor]) ───────────────────────────
    # Keys mirror ExtractorConfig: max_batch_tokens, max_page_tokens,
    # overlap_tokens, max_retries, default_concurrency
    extractor_max_batch_tokens: int = 65_000
    extractor_max_page_tokens: int = 35_000
    extractor_overlap_tokens: int = 5_000
    extractor_max_retries: int = 2
    extractor_default_concurrency: int = 10

    # ── rag (config.toml [rag]) ───────────────────────────────────────────
    # Keys mirror RAGConfig: embedding_model, embedding_dim, chunk_tokens,
    # overlap_tokens, token_cap, embed_batch_size, retrieval_top_k,
    # rerank_top_n, rerank_model (cohere_api_key is a secret)
    rag_embedding_model: str = "text-embedding-3-small"
    rag_embedding_dim: int = 1536
    rag_chunk_tokens: int = 512
    rag_overlap_tokens: int = 100
    rag_token_cap: int = 4096
    rag_embed_batch_size: int = 100
    rag_retrieval_top_k: int = 80
    rag_rerank_top_n: int = 20
    rag_rerank_model: str = "rerank-v3.5"

    # ── AWS (config.toml [aws]) ───────────────────────────────────────────
    aws_region: str = "us-east-2"
    aws_profile: str = ""

    # ── Search (config.toml [search]) ─────────────────────────────────────
    search_provider: str = "exa"

    # ── Pipeline caps (config.toml [pipeline]) ────────────────────────────
    max_scrape_concurrency: int = 20
    page_stale_days: int = 30
    max_discover_tool_calls: int = 200
    max_discover_minutes: int = 15
    max_qa_iterations: int = 2
    max_urls_per_run: int = 5000

    def __post_init__(self):
        toml = _load_toml()
        models = toml.get("models", {})
        profile = toml.get("profile", {})
        scraper = toml.get("scraper", {})
        extractor = toml.get("extractor", {})
        rag = toml.get("rag", {})
        aws = toml.get("aws", {})
        search = toml.get("search", {})
        pipeline = toml.get("pipeline", {})

        bedrock_default = "bedrock/us.anthropic.claude-opus-4-6-v1"

        # ── Secrets (env only) ────────────────────────────────────────────
        self.corpgraph_db_url = _env("CORPGRAPH_DB_URL", "postgresql://corpgraph:corpgraph@localhost:5432/corpgraph")
        self.crawl4ai_api_key = _env("CRAWL4AI_API_KEY")
        self.firecrawl_api_key = _env("FIRECRAWL_API_KEY")
        self.openai_api_key = _env("OPENAI_API_KEY")
        self.cohere_api_key = _env("COHERE_API_KEY")
        self.exa_api_key = _env("EXA_API_KEY")

        # ── Models ────────────────────────────────────────────────────────
        self.profile_model = _resolve_str("PROFILE_MODEL", models, "profile", "bedrock/us.anthropic.claude-haiku-4-5-20251001-v1:0")
        self.discover_model = _resolve_str("DISCOVER_MODEL", models, "discover", bedrock_default)
        self.extract_model = _resolve_str("EXTRACT_MODEL", models, "extract", bedrock_default)
        self.merge_model = _resolve_str("MERGE_MODEL", models, "merge", "openai/gpt-5-mini")
        self.qa_model = _resolve_str("QA_MODEL", models, "qa", bedrock_default)

        # ── corp-profile (keys mirror per-stage config) ─────────────────
        self.profile_enrich = _resolve_bool("PROFILE_ENRICH", profile, "enrich", False)
        self.profile_web = _resolve_bool("PROFILE_WEB", profile, "web", False)
        self.profile_research_model = _resolve_str("PROFILE_RESEARCH_MODEL", profile, "research_model", "openai/gpt-5-mini")
        self.profile_research_provider = _resolve_str("PROFILE_RESEARCH_PROVIDER", profile, "research_provider", "auto")
        self.profile_enrich_model = _resolve_str("PROFILE_ENRICH_MODEL", profile, "enrich_model", self.profile_model)
        self.profile_web_model = _resolve_str("PROFILE_WEB_MODEL", profile, "web_model", "openai/gpt-5-mini")
        self.profile_web_provider = _resolve_str("PROFILE_WEB_PROVIDER", profile, "web_provider", "auto")

        # ── web-scraper (keys mirror ScraperConfig) ──────────────────────
        self.scraper_base_url = _resolve_str("SCRAPER_BASE_URL", scraper, "base_url", "https://api.crawl4ai.com")
        self.scraper_batch_limit = _resolve_int("SCRAPER_BATCH_LIMIT", scraper, "batch_limit", 10)
        self.scraper_poll_interval_s = _resolve_float("SCRAPER_POLL_INTERVAL_S", scraper, "poll_interval_s", 2.0)
        self.scraper_client_timeout_s = _resolve_float("SCRAPER_CLIENT_TIMEOUT_S", scraper, "client_timeout_s", 120.0)
        self.scraper_strategy = _resolve_str("SCRAPER_STRATEGY", scraper, "strategy", "http")
        self.scraper_default_timeout_ms = _resolve_int("SCRAPER_DEFAULT_TIMEOUT_MS", scraper, "default_timeout_ms", 30_000)
        self.scraper_default_proxy = _resolve_bool("SCRAPER_DEFAULT_PROXY", scraper, "default_proxy", False)
        self.scraper_default_proxy_mode = _resolve_str("SCRAPER_DEFAULT_PROXY_MODE", scraper, "default_proxy_mode", "")
        self.scraper_remove_overlays = _resolve_bool("SCRAPER_REMOVE_OVERLAYS", scraper, "remove_overlays", True)

        # ── doc-extractor (keys mirror ExtractorConfig) ──────────────────
        self.extractor_max_batch_tokens = _resolve_int("EXTRACTOR_MAX_BATCH_TOKENS", extractor, "max_batch_tokens", 120_000)
        self.extractor_max_page_tokens = _resolve_int("EXTRACTOR_MAX_PAGE_TOKENS", extractor, "max_page_tokens", 60_000)
        self.extractor_overlap_tokens = _resolve_int("EXTRACTOR_OVERLAP_TOKENS", extractor, "overlap_tokens", 5_000)
        self.extractor_max_retries = _resolve_int("EXTRACTOR_MAX_RETRIES", extractor, "max_retries", 2)
        self.extractor_default_concurrency = _resolve_int("EXTRACTOR_DEFAULT_CONCURRENCY", extractor, "default_concurrency", 10)

        # ── rag (keys mirror RAGConfig) ──────────────────────────────────
        self.rag_embedding_model = _resolve_str("RAG_EMBEDDING_MODEL", rag, "embedding_model", "text-embedding-3-small")
        self.rag_embedding_dim = _resolve_int("RAG_EMBEDDING_DIM", rag, "embedding_dim", 1536)
        self.rag_chunk_tokens = _resolve_int("RAG_CHUNK_TOKENS", rag, "chunk_tokens", 512)
        self.rag_overlap_tokens = _resolve_int("RAG_OVERLAP_TOKENS", rag, "overlap_tokens", 100)
        self.rag_token_cap = _resolve_int("RAG_TOKEN_CAP", rag, "token_cap", 4096)
        self.rag_embed_batch_size = _resolve_int("RAG_EMBED_BATCH_SIZE", rag, "embed_batch_size", 100)
        self.rag_retrieval_top_k = _resolve_int("RAG_RETRIEVAL_TOP_K", rag, "retrieval_top_k", 80)
        self.rag_rerank_top_n = _resolve_int("RAG_RERANK_TOP_N", rag, "rerank_top_n", 20)
        self.rag_rerank_model = _resolve_str("RAG_RERANK_MODEL", rag, "rerank_model", "rerank-v3.5")

        # ── AWS ───────────────────────────────────────────────────────────
        self.aws_region = _resolve_str("AWS_DEFAULT_REGION", aws, "region", "us-east-2")
        self.aws_profile = _resolve_str("AWS_PROFILE", aws, "profile", "")
        # Ensure boto3 / pydantic-ai BedrockProvider can discover the region
        if self.aws_region:
            os.environ.setdefault("AWS_DEFAULT_REGION", self.aws_region)

        # ── Search ────────────────────────────────────────────────────────
        self.search_provider = _resolve_str("SEARCH_PROVIDER", search, "provider", "exa")

        # ── Pipeline caps ─────────────────────────────────────────────────
        self.max_scrape_concurrency = _resolve_int("MAX_SCRAPE_CONCURRENCY", pipeline, "max_scrape_concurrency", 20)
        self.page_stale_days = _resolve_int("PAGE_STALE_DAYS", pipeline, "page_stale_days", 30)
        self.max_discover_tool_calls = _resolve_int("MAX_DISCOVER_TOOL_CALLS", pipeline, "max_discover_tool_calls", 200)
        self.max_discover_minutes = _resolve_int("MAX_DISCOVER_MINUTES", pipeline, "max_discover_minutes", 15)
        self.max_qa_iterations = _resolve_int("MAX_QA_ITERATIONS", pipeline, "max_qa_iterations", 2)
        self.max_urls_per_run = _resolve_int("MAX_URLS_PER_RUN", pipeline, "max_urls_per_run", 5000)

    # ── Sub-module config builders ────────────────────────────────────────

    def scraper_config(self):
        """Build a web-scraper ScraperConfig from this master config."""
        from web_scraper import ScraperConfig
        return ScraperConfig(
            base_url=self.scraper_base_url,
            batch_limit=self.scraper_batch_limit,
            poll_interval_s=self.scraper_poll_interval_s,
            client_timeout_s=self.scraper_client_timeout_s,
            strategy=self.scraper_strategy,
            default_timeout_ms=self.scraper_default_timeout_ms,
            default_proxy=self.scraper_default_proxy,
            default_proxy_mode=self.scraper_default_proxy_mode,
            remove_overlays=self.scraper_remove_overlays,
        )

    def extractor_config(self):
        """Build a doc-extractor ExtractorConfig from this master config."""
        from doc_extractor import ExtractorConfig
        return ExtractorConfig(
            max_batch_tokens=self.extractor_max_batch_tokens,
            max_page_tokens=self.extractor_max_page_tokens,
            overlap_tokens=self.extractor_overlap_tokens,
            max_retries=self.extractor_max_retries,
            default_concurrency=self.extractor_default_concurrency,
        )

    def rag_config(self):
        """Build a rag RAGConfig from this master config."""
        from rag import RAGConfig
        return RAGConfig(
            embedding_model=self.rag_embedding_model,
            embedding_dim=self.rag_embedding_dim,
            chunk_tokens=self.rag_chunk_tokens,
            overlap_tokens=self.rag_overlap_tokens,
            token_cap=self.rag_token_cap,
            embed_batch_size=self.rag_embed_batch_size,
            retrieval_top_k=self.rag_retrieval_top_k,
            rerank_top_n=self.rag_rerank_top_n,
            rerank_model=self.rag_rerank_model,
            cohere_api_key=self.cohere_api_key,
        )

    def profile_pipeline_config(self):
        """Build a corp-profile PipelineConfig from this master config."""
        from corp_profile.config import PipelineConfig
        return PipelineConfig(
            enrich=self.profile_enrich,
            web=self.profile_web,
        )

    def profile_enrich_config(self):
        """Build a corp-profile EnrichConfig from this master config."""
        from corp_profile.config import EnrichConfig
        return EnrichConfig(
            model=self.profile_enrich_model or self.profile_model,
            aws_region=self.aws_region or None,
            aws_profile=self.aws_profile or None,
        )

    def profile_web_config(self):
        """Build a corp-profile WebConfig from this master config."""
        from corp_profile.config import WebConfig
        return WebConfig(
            model=self.profile_web_model,
            provider=self.profile_web_provider,
            aws_region=self.aws_region or None,
            aws_profile=self.aws_profile or None,
        )

    def profile_research_config(self):
        """Build a corp-profile ResearchConfig from this master config."""
        from corp_profile.config import ResearchConfig
        return ResearchConfig(
            model=self.profile_research_model,
            provider=self.profile_research_provider,
            aws_region=self.aws_region or None,
            aws_profile=self.aws_profile or None,
        )
