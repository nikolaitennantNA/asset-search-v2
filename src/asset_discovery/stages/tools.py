"""Tools for the discover and QA agents."""

from __future__ import annotations

import asyncio
import re
import xml.etree.ElementTree as ET
from typing import Any, Callable
from urllib.parse import urlparse

import httpx

from ..config import Config
from ..cost import CostTracker
from ..db import get_connection, get_discovered_urls, save_discovered_urls
from ..helpers import normalize_url, strip_tracking_params
from ..models import DiscoveredUrl

_config: Config | None = None
_issuer_id: str = ""
_costs: CostTracker | None = None
_on_event: Callable[[str, dict], None] | None = None


def _emit(event: str, **data: Any) -> None:
    """Emit a display event to the registered callback."""
    if _on_event:
        _on_event(event, data)


def _norm_domain(domain: str) -> str:
    """Normalize domain by stripping www. prefix."""
    return domain.removeprefix("www.")


def init_tools(
    config: Config,
    issuer_id: str,
    costs: CostTracker | None = None,
    on_event: Callable[[str, dict], None] | None = None,
) -> None:
    global _config, _issuer_id, _costs, _on_event
    _config = config
    _issuer_id = issuer_id
    _costs = costs
    _on_event = on_event


def _get_conn():
    return get_connection(_config)


_SITEMAP_PATHS = [
    "sitemap.xml",
    "sitemap_index.xml",
    "sitemap-index.xml",
    "wp-sitemap.xml",
    "sitemap.html",
]


async def _spider_fetch_raw(url: str) -> dict[str, str]:
    """Fetch a single URL via Spider /scrape with raw format. Returns {url, content, error}."""
    assert _config is not None
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                "https://api.spider.cloud/scrape",
                headers={
                    "Authorization": f"Bearer {_config.spider_api_key}",
                    "Content-Type": "application/json",
                },
                json={"url": url, "return_format": "raw"},
            )
            resp.raise_for_status()
            data = resp.json()
            entries = data if isinstance(data, list) else [data]
            for entry in entries:
                content = entry.get("content", "")
                if content:
                    return {"url": url, "content": content, "error": ""}
            return {"url": url, "content": "", "error": "empty response"}
    except Exception as e:
        return {"url": url, "content": "", "error": str(e)}


async def fetch_sitemap(domain: str, sitemap: str | None = None) -> list[dict[str, str]]:
    """Fetch and parse sitemaps for a domain via Spider Cloud.

    Uses Spider /scrape with raw format to get the actual XML through WAF,
    then parses it ourselves. Tries all common sitemap paths in parallel
    plus any paths found in robots.txt.

    If a sitemap index is found, returns the child sitemap entries (with
    type="index") so the agent can decide which to follow up on. Call again
    with sitemap="store-sitemap.xml" to fetch a specific child.

    Args:
        domain: The domain to fetch sitemaps for (e.g. "example.com").
        sitemap: Optional specific sitemap path or full URL to fetch.

    Returns list of dicts:
        - Index entries: {url, lastmod?, type: "index"}
        - Page URLs: {url, lastmod?}
    """
    assert _config is not None
    base = f"https://{domain}"

    # Determine which URLs to fetch
    if sitemap:
        targets = [sitemap if sitemap.startswith("http") else f"{base}/{sitemap}"]
    else:
        targets = [f"{base}/{p}" for p in _SITEMAP_PATHS] + [f"{base}/robots.txt"]

    # Fetch all in parallel via Spider
    raw_results = await asyncio.gather(
        *[_spider_fetch_raw(url) for url in targets],
        return_exceptions=True,
    )

    # Parse robots.txt for additional sitemap paths
    extra_targets: list[str] = []
    seen_targets = set(targets)
    for result in raw_results:
        if isinstance(result, Exception) or not result.get("content"):
            continue
        if "robots.txt" in result["url"]:
            for line in result["content"].splitlines():
                if line.lower().startswith("sitemap:"):
                    sm_url = line.split(":", 1)[1].strip()
                    if sm_url not in seen_targets:
                        extra_targets.append(sm_url)
                        seen_targets.add(sm_url)

    # Fetch any additional sitemaps from robots.txt
    if extra_targets:
        extra_results = await asyncio.gather(
            *[_spider_fetch_raw(url) for url in extra_targets],
            return_exceptions=True,
        )
        raw_results = list(raw_results) + list(extra_results)

    if _costs:
        fetched = sum(1 for r in raw_results if not isinstance(r, Exception) and r.get("content"))
        _costs.track_spider(fetched, cost_usd=0)

    # Parse all XML responses
    indexes: list[dict[str, str]] = []
    urls: list[dict[str, str]] = []
    seen: set[str] = set()

    for result in raw_results:
        if isinstance(result, Exception) or not result.get("content"):
            continue
        if "robots.txt" in result.get("url", ""):
            continue

        content = result["content"]

        # Try XML parse
        try:
            root = ET.fromstring(content)
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            # Sitemap index entries
            for sm in root.findall("sm:sitemap", ns):
                loc = sm.findtext("sm:loc", namespaces=ns)
                if loc and loc not in seen:
                    seen.add(loc)
                    entry: dict[str, str] = {"url": loc, "type": "index"}
                    lastmod = sm.findtext("sm:lastmod", namespaces=ns)
                    if lastmod:
                        entry["lastmod"] = lastmod
                    indexes.append(entry)

            # Actual URL entries
            for url_elem in root.findall("sm:url", ns):
                loc = url_elem.findtext("sm:loc", namespaces=ns)
                if loc and loc not in seen:
                    seen.add(loc)
                    entry = {"url": loc}
                    lastmod = url_elem.findtext("sm:lastmod", namespaces=ns)
                    if lastmod:
                        entry["lastmod"] = lastmod
                    urls.append(entry)
        except ET.ParseError:
            # Not XML — might be HTML sitemap, extract links
            found = re.findall(r'https?://[^\s<>"\')\]]+', content)
            for u in found:
                if domain in u and u not in seen:
                    seen.add(u)
                    urls.append({"url": u})

    # Emit display events
    if sitemap:
        # Child sitemap fetch
        name = sitemap.rsplit("/", 1)[-1] if "/" in sitemap else sitemap
        _emit("sitemap_urls", domain=_norm_domain(domain), sitemap=name,
              count=len(indexes) + len(urls))
    elif indexes:
        _emit("sitemap_indexes", domain=_norm_domain(domain), count=len(indexes))
    elif urls:
        _emit("sitemap_urls", domain=_norm_domain(domain), count=len(urls))

    # Return index entries if found (agent decides which to follow)
    # Otherwise return the actual URLs
    if indexes:
        return indexes
    return urls


async def crawl_page(
    url: str,
    proxy: str | None = None,
) -> dict[str, Any]:
    """Fetch a single page via Spider Cloud. Lightweight exploration tool.

    Uses the same scrape() pipeline as batch scraping: dual-format (markdown + raw),
    images filtered, markdown cleaned, coordinates/addresses extracted from HTML
    and injected at the top of the markdown.

    Args:
        url: The full URL to fetch and render.
        proxy: Proxy type -- "residential", "mobile", "isp", or None.
               Spider's smart mode handles most bot detection automatically.

    Returns dict with keys: markdown, links_internal, links_external, metadata, error.
    """
    assert _config is not None
    from web_scraper import scrape, ScrapeConfig, Usage

    configs = None
    if proxy:
        per_url = ScrapeConfig(
            proxy=proxy if proxy != "auto" else None,
            proxy_enabled=True,
        )
        configs = {url: per_url}

    usage = Usage()
    try:
        pages = await scrape(
            [url], _config.spider_api_key,
            configs=configs,
            usage=usage,
        )
    except Exception as e:
        return {"markdown": "", "error": str(e)}

    if _costs and usage.pages_scraped:
        _costs.track_spider(usage.pages_scraped, cost_usd=usage.total_cost)

    if not pages or not pages[0].success:
        error = "Failed to scrape" if not pages else f"Status {pages[0].status_code}"
        parsed = urlparse(url)
        _emit("crawl_result", domain=_norm_domain(parsed.netloc),
              url=url, path=parsed.path or "/", success=False)
        return {"markdown": "", "error": error}

    page = pages[0]
    parsed = urlparse(url)
    _emit("crawl_result", domain=_norm_domain(parsed.netloc),
          url=url, path=parsed.path or "/", success=True)
    return {
        "markdown": page.markdown,
        "links_internal": page.links_internal,
        "links_external": page.links_external,
        "metadata": page.metadata,
    }


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
            _emit("map_result", domain=_norm_domain(domain), count=len(out))
            return out
    except Exception:
        return []


async def spider_links(
    url: str,
    limit: int = 500,
    depth: int = 10,
    metadata: bool = True,
    sitemap_only: bool = False,
) -> list[dict[str, str]]:
    """Crawl a website via Spider Cloud /links to collect all URLs found.

    Much faster and cheaper than crawling pages individually — only collects
    links without rendering page content. Handles WAF-blocked sites.

    Use sitemap_only=True to just fetch sitemap URLs (used by fetch_sitemap).
    Use sitemap_only=False for full crawl-based link discovery.

    Args:
        url: The starting URL (e.g. "https://example.com").
        limit: Maximum pages to crawl. 0 = unlimited. Default 500.
        depth: Maximum crawl depth. Default 10.
        metadata: Collect page titles and descriptions. Default True.
        sitemap_only: Only return URLs from the sitemap. Default False.

    Returns list of dicts with url (and optional title) keys.
    """
    assert _config is not None
    body: dict = {
        "url": url,
        "limit": limit,
        "depth": depth,
        "metadata": metadata,
        "request": "smart",
    }
    if sitemap_only:
        body["sitemap_only"] = True
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                "https://api.spider.cloud/links",
                headers={
                    "Authorization": f"Bearer {_config.spider_api_key}",
                    "Content-Type": "application/json",
                },
                json=body,
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        return [{"error": str(e)}]

    # Handle both response shapes (array or {data: array})
    entries = data if isinstance(data, list) else data.get("data", [])

    urls: list[dict[str, str]] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        u = entry.get("url", "")
        if u and u not in seen:
            seen.add(u)
            item: dict[str, str] = {"url": u}
            if entry.get("title"):
                item["title"] = entry["title"]
            urls.append(item)

    if _costs:
        _costs.track_spider(len(entries), cost_usd=0)

    _emit("spider_result", domain=_norm_domain(urlparse(url).netloc), count=len(urls))
    return urls


async def group_by_prefix(
    urls: list[str] | None = None,
    depth: int = 2,
) -> dict[str, int]:
    """Group URLs by path prefix and return counts.

    Call with a list of URLs, or with no arguments to group all saved URLs.
    Use to understand the shape of a sitemap or your saved URL list before
    deciding what to keep or prune.

    Args:
        urls: List of URLs to group. If empty/None, groups all saved URLs.
        depth: How many path segments to use for grouping (default 2).
               depth=1: /store/ (485), /news/ (2100)
               depth=2: /store/az/ (23), /store/ca/ (45)

    Returns dict mapping path prefixes to URL counts, sorted by count.
    """
    if not urls:
        saved = await get_saved_urls()
        urls = [u["url"] for u in saved]
    if not urls:
        return {}

    groups: dict[str, int] = {}
    for url in urls:
        path = urlparse(url).path or "/"
        parts = [p for p in path.split("/") if p]
        if len(parts) >= depth:
            prefix = "/" + "/".join(parts[:depth]) + "/"
        elif parts:
            prefix = "/" + "/".join(parts) + "/"
        else:
            prefix = "/"
        groups[prefix] = groups.get(prefix, 0) + 1

    return dict(sorted(groups.items(), key=lambda x: -x[1]))


async def save_sitemap_urls(
    domain: str,
    sitemap: str | None = None,
    category: str = "facility_page",
    notes: str = "",
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> int:
    """Fetch a sitemap and bulk-save URLs from it.

    Use after fetch_sitemap to save many URLs at once. Two filtering modes:

    **Include mode** — only save URLs containing any of the include substrings.
      save_sitemap_urls("sprouts.com", "store-sitemap.xml",
                        include=["/store/"], category="facility_page")

    **Exclude mode** — save all URLs except those containing any exclude substring.
      save_sitemap_urls("lemontreehotels.com",
                        exclude=["/news/", "/blog/", "/careers/"],
                        category="facility_page")

    If neither include nor exclude is set, saves all URLs from the sitemap.
    Don't set both include and exclude — pick whichever is simpler.

    Args:
        domain: Domain to fetch sitemap from.
        sitemap: Specific child sitemap (e.g. "store-sitemap.xml").
        category: Category for all saved URLs.
        notes: Notes to attach to all saved URLs.
        include: Only save URLs containing any of these substrings.
        exclude: Skip URLs containing any of these substrings.

    Returns count of URLs saved.
    """
    # Fetch the sitemap
    results = await fetch_sitemap(domain, sitemap)
    if not results:
        return 0

    # If we got a sitemap index and no child specified, save ALL children's URLs
    if results[0].get("type") == "index":
        if sitemap:
            # Agent asked for a specific child but got an index — shouldn't happen
            return 0
        # Recursively fetch each child sitemap and collect URLs
        all_urls: list[str] = []
        for idx_entry in results:
            child_url = idx_entry.get("url", "")
            if not child_url:
                continue
            child_results = await fetch_sitemap(domain, child_url)
            for entry in child_results:
                if entry.get("type") != "index":
                    url = entry.get("url", "")
                    if url:
                        all_urls.append(url)
        results_flat = [{"url": u} for u in all_urls]
    else:
        results_flat = results

    # Filter
    filtered: list[str] = []
    for entry in results_flat:
        url = entry.get("url", "")
        if not url:
            continue
        if include:
            if any(pat in url for pat in include):
                filtered.append(url)
        elif exclude:
            if not any(pat in url for pat in exclude):
                filtered.append(url)
        else:
            filtered.append(url)

    if not filtered:
        return 0

    # Build URL dicts and delegate to save_urls
    url_dicts: list[dict[str, Any]] = [
        {"url": u, "category": category, "notes": notes}
        for u in filtered
    ]
    count = await save_urls(url_dicts)
    return count


async def save_urls(
    urls: list[dict[str, Any]] | None = None,
) -> int:
    """Batch upsert URLs to discovered_urls table.

    Args:
        urls: List of URL dicts. Required keys: url, category.
              Optional keys: notes, proxy_mode, wait_for, js_code,
              scan_full_page, screenshot.
              proxy_mode must be "auto", "datacenter", or "residential" (or omitted).

    Returns count of URLs saved.
    """
    iid = _issuer_id
    urls = urls or []
    if not urls:
        return 0
    # Normalize, validate, and dedup within the batch
    seen: set[str] = set()
    validated = []
    for u in urls:
        raw_url = u.get("url", "")
        clean = strip_tracking_params(raw_url)
        norm = normalize_url(clean)
        if not norm:
            continue
        u["url"] = norm
        if norm in seen:
            continue
        seen.add(norm)
        parsed = DiscoveredUrl(**u)
        validated.append(parsed.model_dump(exclude_none=True))
    if not validated:
        return 0
    conn = _get_conn()
    try:
        existing = get_discovered_urls(conn, iid)
        existing_urls = {e["url"] for e in existing}
        # Drop URLs already in DB
        new_urls = [v for v in validated if v["url"] not in existing_urls]
        budget = (_config.max_urls_per_run if _config else 5000) - len(existing)
        if budget <= 0 or not new_urls:
            return 0
        new_urls = new_urls[:budget]
        count = save_discovered_urls(conn, iid, new_urls)
        domain = ""
        if new_urls:
            domain = _norm_domain(urlparse(new_urls[0].get("url", "")).netloc)
        _emit("save_result", domain=domain, count=count)
        return count
    finally:
        conn.close()


async def remove_urls(patterns: list[str]) -> int:
    """Remove saved URLs matching any of the given substrings.

    Use at the end of discovery to prune noise URLs you bulk-saved earlier.
    Matches are substring-based — any URL containing any pattern is removed.

    Example: remove_urls(["/news/", "/blog/", "/careers/", "/press-release/"])

    Args:
        patterns: List of substrings. Any saved URL containing any of
                  these substrings will be deleted.

    Returns count of URLs removed.
    """
    iid = _issuer_id
    if not patterns:
        return 0
    conn = _get_conn()
    try:
        existing = get_discovered_urls(conn, iid)
        to_remove = [
            e["url"] for e in existing
            if any(pat in e["url"] for pat in patterns)
        ]
        if not to_remove:
            return 0
        from ..db import delete_discovered_urls
        count = delete_discovered_urls(conn, iid, to_remove)
        _emit("remove_result", count=count, patterns=patterns)
        return count
    finally:
        conn.close()


async def get_saved_urls() -> list[dict[str, Any]]:
    """Read all discovered_urls saved so far for this issuer.

    Returns list of URL dicts from the database.
    """
    iid = _issuer_id
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
        headers={"User-Agent": "Mozilla/5.0 (compatible; asset-discovery/2.0)"},
    ) as client:
        tasks = [_probe_one(client, url) for url in urls]
        results = list(await asyncio.gather(*tasks))
    exist = sum(1 for r in results if 200 <= r.get("status", 0) < 400)
    domain = _norm_domain(urlparse(urls[0]).netloc) if urls else ""
    _emit("probe_result", domain=domain, total=len(urls), exist=exist)
    return results


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


async def spawn_worker(task: str) -> str:
    """Spawn a focused worker agent for a specific subtask.

    Use when a distinct chunk of work can run independently while you continue
    with other domains. Good for:
    - Exploring a subsidiary's website
    - Searching regulatory databases for the company
    - Investigating a specific domain or set of URLs

    The worker has the same tools as you (fetch_sitemap, crawl_page, probe_urls,
    save_urls, etc.) plus web search. It runs independently and saves URLs
    directly to the database.

    Args:
        task: Clear, specific instruction for the worker. Be explicit about
              what domains/URLs to investigate and what to save.

    Returns: Summary of what the worker found and saved.
    """
    assert _config is not None

    from pydantic_ai import Agent, UsageLimits

    from ..config import _to_pydantic_ai_model

    model_str = _to_pydantic_ai_model(_config.discover_model)
    builtin_tools: list = []

    if _config.search_provider == "openai":
        from pydantic_ai import WebSearchTool
        builtin_tools.append(WebSearchTool())
        if model_str.startswith("openai:"):
            model_str = model_str.replace("openai:", "openai-responses:", 1)

    worker = Agent(
        model_str,
        system_prompt=(
            "You are a focused URL discovery worker. Execute the task immediately — "
            "do not plan, do not summarize your approach, do not wait for approval. "
            "Start calling tools right away. Save URLs to the database as you find them."
        ),
        tools=[
            fetch_sitemap, crawl_page, map_domain, spider_links,
            probe_urls, save_urls, get_saved_urls,
        ],
        builtin_tools=builtin_tools,
    )

    try:
        async with asyncio.timeout(120):
            async with worker:
                result = await worker.run(
                    task,
                    usage_limits=UsageLimits(tool_calls_limit=50),
                )
                if _costs and result.usage():
                    _costs.track_pydantic_ai(
                        result.usage(), _config.discover_model, "discover-worker",
                    )
                return result.output
    except (TimeoutError, asyncio.TimeoutError):
        return "Worker timed out after 2 minutes — any URLs saved so far are preserved."
    except Exception as e:
        return f"Worker error: {e} — any URLs saved so far are preserved."
