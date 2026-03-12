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
    profile_web_search: bool = False
    profile_web_search_model: str = ""

    # ── web-scraper (config.toml [scraper]) ───────────────────────────────
    scrape_timeout_ms: int = 30_000
    scrape_use_proxy: bool = False
    scrape_batch_limit: int = 10
    scrape_client_timeout_s: float = 120.0
    scrape_strategy: str = "browser"

    # ── doc-extractor (config.toml [extractor]) ───────────────────────────
    extract_max_batch_tokens: int = 65_000
    extract_max_page_tokens: int = 35_000
    extract_overlap_tokens: int = 5_000
    extract_max_retries: int = 2

    # ── rag (config.toml [rag]) ───────────────────────────────────────────
    rag_embedding_model: str = "text-embedding-3-small"
    rag_embedding_dim: int = 1536
    rag_chunk_tokens: int = 512
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
    max_scrape_concurrency: int = 100
    max_extract_concurrency: int = 10
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

        bedrock_default = "bedrock/us.anthropic.claude-opus-4-6-20250219-v1:0"

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

        # ── corp-profile ──────────────────────────────────────────────────
        self.profile_enrich = _resolve_bool("PROFILE_ENRICH", profile, "enrich", False)
        self.profile_web_search = _resolve_bool("PROFILE_WEB_SEARCH", profile, "web_search", False)
        self.profile_web_search_model = _resolve_str("PROFILE_WEB_SEARCH_MODEL", profile, "web_search_model", "")

        # ── web-scraper ───────────────────────────────────────────────────
        self.scrape_timeout_ms = _resolve_int("SCRAPE_TIMEOUT_MS", scraper, "default_timeout_ms", 30_000)
        self.scrape_use_proxy = _resolve_bool("SCRAPE_USE_PROXY", scraper, "default_proxy", False)
        self.scrape_batch_limit = _resolve_int("SCRAPE_BATCH_LIMIT", scraper, "batch_limit", 10)
        self.scrape_client_timeout_s = _resolve_float("SCRAPE_CLIENT_TIMEOUT_S", scraper, "client_timeout_s", 120.0)
        self.scrape_strategy = _resolve_str("SCRAPE_STRATEGY", scraper, "strategy", "browser")

        # ── doc-extractor ─────────────────────────────────────────────────
        self.extract_max_batch_tokens = _resolve_int("EXTRACT_MAX_BATCH_TOKENS", extractor, "max_batch_tokens", 120_000)
        self.extract_max_page_tokens = _resolve_int("EXTRACT_MAX_PAGE_TOKENS", extractor, "max_page_tokens", 60_000)
        self.extract_overlap_tokens = _resolve_int("EXTRACT_OVERLAP_TOKENS", extractor, "overlap_tokens", 5_000)
        self.extract_max_retries = _resolve_int("EXTRACT_MAX_RETRIES", extractor, "max_retries", 2)

        # ── rag ───────────────────────────────────────────────────────────
        self.rag_embedding_model = _resolve_str("RAG_EMBEDDING_MODEL", rag, "embedding_model", "text-embedding-3-small")
        self.rag_embedding_dim = _resolve_int("RAG_EMBEDDING_DIM", rag, "embedding_dim", 1536)
        self.rag_chunk_tokens = _resolve_int("RAG_CHUNK_TOKENS", rag, "chunk_tokens", 512)
        self.rag_embed_batch_size = _resolve_int("RAG_EMBED_BATCH_SIZE", rag, "embed_batch_size", 100)
        self.rag_retrieval_top_k = _resolve_int("RAG_RETRIEVAL_TOP_K", rag, "retrieval_top_k", 80)
        self.rag_rerank_top_n = _resolve_int("RAG_RERANK_TOP_N", rag, "rerank_top_n", 20)
        self.rag_rerank_model = _resolve_str("RAG_RERANK_MODEL", rag, "rerank_model", "rerank-v3.5")

        # ── AWS ───────────────────────────────────────────────────────────
        self.aws_region = _resolve_str("AWS_DEFAULT_REGION", aws, "region", "us-east-2")
        self.aws_profile = _resolve_str("AWS_PROFILE", aws, "profile", "")

        # ── Search ────────────────────────────────────────────────────────
        self.search_provider = _resolve_str("SEARCH_PROVIDER", search, "provider", "exa")

        # ── Pipeline caps ─────────────────────────────────────────────────
        self.max_scrape_concurrency = _resolve_int("MAX_SCRAPE_CONCURRENCY", pipeline, "max_scrape_concurrency", 100)
        self.max_extract_concurrency = _resolve_int("MAX_EXTRACT_CONCURRENCY", pipeline, "max_extract_concurrency", 10)
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
            batch_limit=self.scrape_batch_limit,
            client_timeout_s=self.scrape_client_timeout_s,
            strategy=self.scrape_strategy,
            default_timeout_ms=self.scrape_timeout_ms,
            default_proxy=self.scrape_use_proxy,
        )

    def extractor_config(self):
        """Build a doc-extractor ExtractorConfig from this master config."""
        from doc_extractor import ExtractorConfig
        return ExtractorConfig(
            max_batch_tokens=self.extract_max_batch_tokens,
            max_page_tokens=self.extract_max_page_tokens,
            overlap_tokens=self.extract_overlap_tokens,
            max_retries=self.extract_max_retries,
            default_concurrency=self.max_extract_concurrency,
        )

    def rag_config(self):
        """Build a rag RAGConfig from this master config."""
        from rag import RAGConfig
        return RAGConfig(
            embedding_model=self.rag_embedding_model,
            embedding_dim=self.rag_embedding_dim,
            chunk_tokens=self.rag_chunk_tokens,
            embed_batch_size=self.rag_embed_batch_size,
            retrieval_top_k=self.rag_retrieval_top_k,
            rerank_top_n=self.rag_rerank_top_n,
            rerank_model=self.rag_rerank_model,
            cohere_api_key=self.cohere_api_key,
        )

    def profile_enrich_config(self):
        """Build a corp-profile EnrichConfig from this master config."""
        from corp_profile.enrich import EnrichConfig
        return EnrichConfig(
            model=self.profile_model,
            web_search=self.profile_web_search,
            web_search_model=self.profile_web_search_model or None,
            aws_region=self.aws_region or None,
            aws_profile=self.aws_profile or None,
        )
