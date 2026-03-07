"""Agent tools for the discovery stage.

Tools available to the pydantic-ai discovery agent:
- crawl: fetch a single page and return markdown
- fetch_sitemap: parse a sitemap.xml and return URLs
- map_domain: lightweight domain exploration
- save_urls: persist discovered URLs to the database
"""

from __future__ import annotations


async def crawl(url: str) -> str:
    """Fetch a single page via Crawl4AI and return markdown content."""
    raise NotImplementedError("crawl tool not yet implemented")


async def fetch_sitemap(url: str) -> list[str]:
    """Parse a sitemap.xml and return all URLs found."""
    raise NotImplementedError("fetch_sitemap tool not yet implemented")


async def map_domain(domain: str) -> dict:
    """Lightweight domain exploration: robots.txt, sitemap index, sample pages."""
    raise NotImplementedError("map_domain tool not yet implemented")


async def save_urls(urls: list[dict]) -> int:
    """Persist discovered URLs to the database. Returns count saved."""
    raise NotImplementedError("save_urls tool not yet implemented")
