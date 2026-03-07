"""URL validation, sanitization, and normalization utilities.

Ported from asset-search v1 url_utils.py.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse


def normalize_url(url: str) -> str | None:
    """Clean and validate a raw URL string.

    - Strips whitespace
    - Extracts URL from annotated text
    - Adds https:// scheme when missing
    - Returns None if the URL doesn't parse to a valid netloc
    """
    raw = (url or "").strip()
    if not raw:
        return None

    if " " in raw or "\t" in raw:
        url_match = re.search(r"https?://[^\s)\]>,]+", raw)
        if url_match:
            raw = url_match.group(0).rstrip(".,;:")
        else:
            token = re.split(r"[\s(]", raw)[0].strip()
            if token:
                raw = token
            else:
                return None

    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"

    parsed = urlparse(raw)
    if not parsed.netloc or "." not in parsed.netloc:
        return None
    if "[" in parsed.netloc or "]" in parsed.netloc:
        return None
    if re.search(r"[\s<>{}\[\]|\\^`]", parsed.netloc):
        return None

    return raw


def get_domain(url: str) -> str:
    """Extract the root domain from a URL, stripping www. prefix."""
    try:
        netloc = urlparse(url).netloc.lower()
    except Exception:
        return ""
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


def strip_tracking_params(url: str) -> str:
    """Strip common tracking query parameters from a URL."""
    from urllib.parse import parse_qs, urlencode, urlunparse

    parsed = urlparse(url)
    tracking_params = {
        "utm_source", "utm_medium", "utm_campaign", "utm_term",
        "utm_content", "fbclid", "gclid", "ref", "source",
    }
    params = parse_qs(parsed.query)
    cleaned = {
        k: v for k, v in params.items() if k.lower() not in tracking_params
    }
    cleaned_query = urlencode(cleaned, doseq=True)
    return urlunparse(parsed._replace(query=cleaned_query))
