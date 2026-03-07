from __future__ import annotations

from dataclasses import dataclass, field
from os import environ

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    corpgraph_db_url: str = field(
        default_factory=lambda: environ.get(
            "CORPGRAPH_DB_URL",
            "postgresql://corpgraph:corpgraph@localhost:5432/corpgraph",
        )
    )
    crawl4ai_api_key: str = field(
        default_factory=lambda: environ.get("CRAWL4AI_API_KEY", "")
    )
    firecrawl_api_key: str = field(
        default_factory=lambda: environ.get("FIRECRAWL_API_KEY", "")
    )
    openai_api_key: str = field(
        default_factory=lambda: environ.get("OPENAI_API_KEY", "")
    )
    cohere_api_key: str = field(
        default_factory=lambda: environ.get("COHERE_API_KEY", "")
    )
    extract_model: str = field(
        default_factory=lambda: environ.get("EXTRACT_MODEL", "openai/gpt-4.1")
    )
    discover_model: str = field(
        default_factory=lambda: environ.get("DISCOVER_MODEL", "openai/gpt-4.1")
    )
    max_scrape_concurrency: int = 100
    max_extract_concurrency: int = 10
    page_stale_days: int = 30
