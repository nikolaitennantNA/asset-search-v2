"""Tools for the discover and QA agents."""

from __future__ import annotations

import asyncio
import re
import xml.etree.ElementTree as ET
from typing import Any

import httpx

from ..config import Config
from ..cost import CostTracker
from ..db import get_connection, get_discovered_urls, save_discovered_urls
from ..models import DiscoveredUrl

_config: Config | None = None
_issuer_id: str = ""
_costs: CostTracker | None = None


def init_tools(config: Config, issuer_id: str, costs: CostTracker | None = None) -> None:
    global _config, _issuer_id, _costs
    _config = config
    _issuer_id = issuer_id
    _costs = costs


def _get_conn():
    return get_connection(_config)


async def fetch_sitemap(domain: str) -> list[dict[str, str]]:
    """Fetch and parse sitemaps for a domain.

    1. Check robots.txt for sitemap locations
    2. Try common XML sitemap paths via plain HTTP
    3. If WAF-blocked, fall back to crawl_page via Crawl4AI
    4. Probe HTML sitemaps at /sitemap and /sitemap.html
    Handles sitemap indexes recursively.

    Args:
        domain: The domain to fetch sitemaps for (e.g. "example.com").

    Returns list of dicts with keys: url, lastmod (optional).
    """
    urls: list[dict[str, str]] = []
    sitemap_locs: list[str] = []
    waf_blocked = False

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        try:
            resp = await client.get(f"https://{domain}/robots.txt")
            if resp.status_code == 200:
                for line in resp.text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        sitemap_locs.append(line.split(":", 1)[1].strip())
            elif resp.status_code == 403:
                waf_blocked = True
        except Exception:
            waf_blocked = True

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
                if resp.status_code == 403:
                    waf_blocked = True
                    continue
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

    # Crawl4AI fallback if WAF-blocked and no URLs found via plain HTTP
    if waf_blocked and not urls:
        result = await crawl_page(f"https://{domain}/sitemap.xml", proxy="auto")
        if result.get("markdown"):
            try:
                root = ET.fromstring(result["markdown"])
                ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
                for url_elem in root.findall("sm:url", ns):
                    loc_text = url_elem.findtext("sm:loc", namespaces=ns)
                    if loc_text:
                        urls.append({"url": loc_text})
            except ET.ParseError:
                pass

    # Probe HTML sitemaps at /sitemap and /sitemap.html
    for path in ("/sitemap", "/sitemap.html"):
        result = await crawl_page(f"https://{domain}{path}")
        md = result.get("markdown", "")
        if md and not result.get("error"):
            for link in result.get("links_internal", []):
                link_url = link if isinstance(link, str) else link.get("url", "")
                if link_url and link_url not in {u["url"] for u in urls}:
                    urls.append({"url": link_url})

    return urls


async def crawl_page(
    url: str,
    browser: bool = False,
    proxy: str | None = None,
) -> dict[str, Any]:
    """Fetch a single page via Crawl4AI Cloud. Lightweight exploration tool.

    Args:
        url: The full URL to fetch and render.
        browser: If True, use browser strategy (full JS rendering). Default is
                 HTTP mode (faster, no JS). Use browser=True to check if a page
                 has JS-rendered content that HTTP mode misses.
        proxy: Proxy mode — "auto" (smart escalation: direct → datacenter →
               residential), "datacenter" (2x credits), "residential" (5x
               credits), or None (direct only, 1x credits). Use "auto" for
               sites that may be WAF-blocked.

    Returns dict with keys: markdown, links_internal, links_external, metadata, error.
    """
    assert _config is not None
    from crawl4ai_cloud import AsyncWebCrawler

    strategy = "browser" if browser else "http"
    # Resolve proxy to documented API format
    proxy_cfg: dict | None = None
    if proxy == "auto":
        proxy_cfg = {"use_proxy": True, "skip_direct": False}
    elif proxy in ("datacenter", "residential"):
        proxy_cfg = {"mode": proxy}

    try:
        async with AsyncWebCrawler(
            api_key=_config.crawl4ai_api_key,
            timeout=30.0,
        ) as crawler:
            result = await crawler.run(
                url,
                config={"word_count_threshold": 10},
                strategy=strategy,
                proxy=proxy_cfg,
                include_fields=["links", "metadata"],
            )
            if not result.success:
                return {"markdown": "", "error": result.error_message or "Unknown error"}
            if _costs:
                credits = result.usage.crawl.credits_used if result.usage else 0
                _costs.track_crawl4ai(1, credits_used=credits)
            markdown = ""
            if result.markdown:
                markdown = result.markdown.raw_markdown or ""
            links = result.links or {}
            return {
                "markdown": markdown,
                "links_internal": links.get("internal", []),
                "links_external": links.get("external", []),
                "metadata": result.metadata or {},
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
            if _costs:
                _costs.track_firecrawl(1)
            links = getattr(result, "links", None) or []
            out = []
            for u in links:
                if isinstance(u, str):
                    out.append({"url": u})
                else:
                    entry: dict[str, str] = {"url": getattr(u, "url", str(u))}
                    if getattr(u, "title", None):
                        entry["title"] = u.title
                    if getattr(u, "description", None):
                        entry["description"] = u.description
                    out.append(entry)
            return out
    except Exception:
        return []


async def save_urls(
    issuer_id: str | None = None,
    urls: list[dict[str, Any]] | None = None,
) -> int:
    """Batch upsert URLs to discovered_urls table.

    Args:
        issuer_id: The issuer to save URLs for. Defaults to the current run's issuer.
        urls: List of URL dicts. Required keys: url, category.
              Optional keys: notes, strategy, proxy_mode, wait_for, js_code,
              scan_full_page, screenshot.
              strategy must be "http" or "browser" (or omitted for pipeline default).
              proxy_mode must be "auto", "datacenter", or "residential" (or omitted).

    Returns count of URLs saved.
    """
    iid = issuer_id or _issuer_id
    urls = urls or []
    if not urls:
        return 0
    # Validate each URL through Pydantic — agent gets clear error on bad values
    validated = []
    for u in urls:
        parsed = DiscoveredUrl(**u)
        validated.append(parsed.model_dump(exclude_none=True))
    conn = _get_conn()
    try:
        existing = get_discovered_urls(conn, iid)
        budget = (_config.max_urls_per_run if _config else 5000) - len(existing)
        if budget <= 0:
            return 0
        validated = validated[:budget]
        return save_discovered_urls(conn, iid, validated)
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


async def probe_urls(urls: list[str]) -> list[dict[str, Any]]:
    """Batch-probe URLs with lightweight HTTP GET.

    Returns metadata for each URL without full page rendering. Use this to
    quickly check which pages exist, their content type, size, and title
    before deciding what to save and how to scrape.

    Args:
        urls: List of URLs to probe (max 100 per call).

    Returns list of dicts with keys per URL:
        - url, status, content_type, content_length, title, server
        - waf_blocked (True if 403)
        - error (on connection failure)
    """
    if not urls:
        return []
    urls = urls[:100]
    async with httpx.AsyncClient(
        timeout=10.0,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; asset-search/2.0)"},
    ) as client:
        tasks = [_probe_one(client, url) for url in urls]
        return list(await asyncio.gather(*tasks))


_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)


async def _probe_one(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    """Probe a single URL — GET with title extraction."""
    try:
        resp = await client.get(url)
        content_type = resp.headers.get("content-type", "")
        ct_clean = content_type.split(";")[0].strip().lower() if content_type else ""
        content_length = 0
        cl_header = resp.headers.get("content-length")
        if cl_header:
            try:
                content_length = int(cl_header)
            except ValueError:
                pass
        if not content_length:
            content_length = len(resp.content)
        title = ""
        if "html" in ct_clean:
            head = resp.text[:4096] if resp.text else ""
            m = _TITLE_RE.search(head)
            if m:
                title = m.group(1).strip()
        return {
            "url": url,
            "status": resp.status_code,
            "content_type": ct_clean or content_type,
            "content_length": content_length,
            "title": title,
            "server": resp.headers.get("server", ""),
            "waf_blocked": resp.status_code == 403,
        }
    except Exception as e:
        return {
            "url": url, "status": 0, "content_type": "", "content_length": 0,
            "title": "", "server": "", "waf_blocked": False, "error": str(e),
        }
