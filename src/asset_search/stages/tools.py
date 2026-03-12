"""Tools for the discover and QA agents."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

import httpx

from ..config import Config
from ..db import get_connection, get_discovered_urls, save_discovered_urls

_config: Config | None = None
_issuer_id: str = ""


def init_tools(config: Config, issuer_id: str) -> None:
    global _config, _issuer_id
    _config = config
    _issuer_id = issuer_id


def _get_conn():
    return get_connection(_config)


async def fetch_sitemap(domain: str) -> list[dict[str, str]]:
    """Fetch and parse sitemaps for a domain.

    Checks robots.txt for sitemap locations, then falls back to common paths.
    Handles sitemap indexes recursively.

    Args:
        domain: The domain to fetch sitemaps for (e.g. "example.com").

    Returns list of dicts with keys: url, lastmod (optional).
    """
    urls: list[dict[str, str]] = []
    sitemap_locs: list[str] = []

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        try:
            resp = await client.get(f"https://{domain}/robots.txt")
            if resp.status_code == 200:
                for line in resp.text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        sitemap_locs.append(line.split(":", 1)[1].strip())
        except Exception:
            pass

        if not sitemap_locs:
            sitemap_locs = [
                f"https://{domain}/sitemap.xml",
                f"https://{domain}/sitemap_index.xml",
            ]

        visited: set[str] = set()
        while sitemap_locs:
            loc = sitemap_locs.pop(0)
            if loc in visited:
                continue
            visited.add(loc)
            try:
                resp = await client.get(loc)
                if resp.status_code != 200:
                    continue
                root = ET.fromstring(resp.text)
                ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
                for sitemap in root.findall("sm:sitemap", ns):
                    child_loc = sitemap.findtext("sm:loc", namespaces=ns)
                    if child_loc:
                        sitemap_locs.append(child_loc)
                for url_elem in root.findall("sm:url", ns):
                    loc_text = url_elem.findtext("sm:loc", namespaces=ns)
                    if loc_text:
                        entry: dict[str, str] = {"url": loc_text}
                        lastmod = url_elem.findtext("sm:lastmod", namespaces=ns)
                        if lastmod:
                            entry["lastmod"] = lastmod
                        urls.append(entry)
            except Exception:
                continue
    return urls


async def crawl_page(url: str) -> dict[str, Any]:
    """Fetch a single page via Crawl4AI Cloud. Lightweight exploration tool.

    Args:
        url: The full URL to fetch and render.

    Returns dict with keys: markdown, links_internal, links_external, metadata, error (on failure).
    """
    assert _config is not None
    async with httpx.AsyncClient(
        base_url="https://api.crawl4ai.com/v1",
        headers={"Authorization": f"Bearer {_config.crawl4ai_api_key}"},
        timeout=30.0,
    ) as client:
        try:
            resp = await client.post("/crawl", json={"urls": url, "magic": True, "timeout": 20000})
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success"):
                return {"markdown": "", "error": data.get("error", "Unknown error")}
            result = data["result"]
            links = result.get("links", {})
            return {
                "markdown": result.get("markdown", ""),
                "links_internal": links.get("internal", []),
                "links_external": links.get("external", []),
                "metadata": result.get("metadata", {}),
            }
        except Exception as e:
            return {"markdown": "", "error": str(e)}


async def map_domain(domain: str, search: str | None = None) -> list[dict[str, str]]:
    """Use Firecrawl /map to discover URLs on a domain.

    Args:
        domain: The domain to map (e.g. "example.com").
        search: Optional search query to filter discovered URLs.

    Returns list of dicts with at least a "url" key.
    """
    assert _config is not None
    import firecrawl.v2 as fc

    try:
        async with fc.AsyncFirecrawlClient(api_key=_config.firecrawl_api_key) as app:
            result = await app.map(
                f"https://{domain}",
                search=search,
            )
            links = getattr(result, "links", None) or []
            return [{"url": u} if isinstance(u, str) else {"url": str(u)} for u in links]
    except Exception:
        return []


async def save_urls(
    issuer_id: str | None = None,
    urls: list[dict[str, Any]] | None = None,
) -> int:
    """Batch upsert URLs to discovered_urls table.

    Args:
        issuer_id: The issuer to save URLs for. Defaults to the current run's issuer.
        urls: List of dicts, each with keys: url (required), category (required), notes (optional).

    Returns count of URLs saved.
    """
    iid = issuer_id or _issuer_id
    urls = urls or []
    if not urls:
        return 0
    conn = _get_conn()
    try:
        existing = get_discovered_urls(conn, iid)
        budget = (_config.max_urls_per_run if _config else 5000) - len(existing)
        if budget <= 0:
            return 0
        urls = urls[:budget]
        return save_discovered_urls(conn, iid, urls)
    finally:
        conn.close()


async def get_saved_urls(issuer_id: str | None = None) -> list[dict[str, Any]]:
    """Read all discovered_urls saved so far for this issuer.

    Args:
        issuer_id: The issuer to query. Defaults to the current run's issuer.

    Returns list of URL dicts from the database.
    """
    iid = issuer_id or _issuer_id
    conn = _get_conn()
    try:
        return get_discovered_urls(conn, iid)
    finally:
        conn.close()
