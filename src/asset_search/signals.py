"""Extract structured signal values from raw HTML.

Parses raw HTML for embedded location data that markdown conversion misses:
coordinates (lat/lng), addresses from JSON-LD, Google Maps links, data
attributes, meta tags, inline JS, and raw text patterns.

Ported from asset-search v1 signal_extraction.py.
"""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from typing import Any

# ---------------------------------------------------------------------------
# Coordinate proximity dedup threshold (~55 m at equator)
# ---------------------------------------------------------------------------

_COORD_DEDUP_THRESHOLD = 0.0005

# SVG stripping pattern -- removes <svg>...</svg> blocks containing path data
# that gets falsely matched as coordinate pairs
_SVG_STRIP_RE = re.compile(r"<svg[^>]*>.*?</svg>", re.DOTALL | re.IGNORECASE)

# ---------------------------------------------------------------------------
# Compiled regex patterns
# ---------------------------------------------------------------------------

_COORD_RE = re.compile(
    r"[-+]?\d{1,3}\.\d{3,8}\s*[,/]\s*[-+]?\d{1,3}\.\d{3,8}"
)

# Google Maps URL patterns
_GMAPS_PATTERNS = [
    re.compile(r"@(-?\d{1,3}\.\d{3,8}),(-?\d{1,3}\.\d{3,8})"),
    re.compile(
        r"(?:destination|[?&]q|[?&]ll|[?&]center)"
        r"=(-?\d{1,3}\.\d{3,8})[,+](-?\d{1,3}\.\d{3,8})"
    ),
    re.compile(r"/place/(-?\d{1,3}\.\d{3,8}),(-?\d{1,3}\.\d{3,8})"),
]

_GMAPS_LINK_RE = re.compile(
    r'(?:href|src)\s*=\s*["\']'
    r'([^"\']*(?:maps\.google|google\.com/maps|goo\.gl/maps)[^"\']*)'
    r'["\']',
    re.IGNORECASE,
)

# data-lat/data-lng attribute patterns
_DATA_COORD_RE = re.compile(
    r'data-(?:lat(?:itude)?)\s*=\s*["\'](-?\d{1,3}\.\d{3,8})["\']'
    r"[^>]*"
    r'data-(?:lng|lon(?:gitude)?)\s*=\s*["\'](-?\d{1,3}\.\d{3,8})["\']',
    re.IGNORECASE | re.DOTALL,
)
_DATA_COORD_REV_RE = re.compile(
    r'data-(?:lng|lon(?:gitude)?)\s*=\s*["\'](-?\d{1,3}\.\d{3,8})["\']'
    r"[^>]*"
    r'data-(?:lat(?:itude)?)\s*=\s*["\'](-?\d{1,3}\.\d{3,8})["\']',
    re.IGNORECASE | re.DOTALL,
)

# Meta geo tags
_META_GEO_POSITION_RE = re.compile(
    r'<meta\s+[^>]*name\s*=\s*["\'](?:geo\.position|ICBM)["\']'
    r'[^>]*content\s*=\s*["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_META_OG_LAT_RE = re.compile(
    r"<meta\s+[^>]*(?:property|name)\s*=\s*[\"']"
    r"(?:og:latitude|place:location:latitude)[\"']"
    r"[^>]*content\s*=\s*[\"'](-?\d{1,3}\.\d{3,8})[\"']",
    re.IGNORECASE,
)
_META_OG_LNG_RE = re.compile(
    r"<meta\s+[^>]*(?:property|name)\s*=\s*[\"']"
    r"(?:og:longitude|place:location:longitude)[\"']"
    r"[^>]*content\s*=\s*[\"'](-?\d{1,3}\.\d{3,8})[\"']",
    re.IGNORECASE,
)

# Inline JS coordinate patterns
_JS_LATLNG_RE = re.compile(
    r"(?:new\s+google\.maps\.LatLng|LatLng|L\.latLng)\s*\(\s*"
    r"(-?\d{1,3}\.\d{3,8})\s*,\s*(-?\d{1,3}\.\d{3,8})\s*\)",
)
_JS_COORD_ARRAY_RE = re.compile(
    r"\[\s*(-?\d{1,3}\.\d{4,8})\s*,\s*(-?\d{1,3}\.\d{4,8})\s*\]"
)
_JS_LAT_LNG_OBJ_RE = re.compile(
    r'["\']?(?:lat(?:itude)?)["\']?\s*:\s*(-?\d{1,3}\.\d{3,8})\s*,'
    r'\s*["\']?(?:lng|lon(?:gitude)?)["\']?\s*:\s*(-?\d{1,3}\.\d{3,8})',
)


# ---------------------------------------------------------------------------
# HTML text extractor
# ---------------------------------------------------------------------------


class _TextExtractor(HTMLParser):
    """Minimal HTML-to-text for <address> tags."""

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return " ".join(self._parts).strip()


def _html_to_text(html_fragment: str) -> str:
    parser = _TextExtractor()
    parser.feed(html_fragment)
    return re.sub(r"\s+", " ", parser.get_text()).strip()


# ---------------------------------------------------------------------------
# Coordinate validation
# ---------------------------------------------------------------------------


def _is_valid_coord(lat: float, lng: float) -> bool:
    """Check if coordinates are geographically valid and non-trivial."""
    if lat < -90 or lat > 90 or lng < -180 or lng > 180:
        return False
    if abs(lat) < 0.01 and abs(lng) < 0.01:
        return False
    return True


# ---------------------------------------------------------------------------
# JSON-LD extraction
# ---------------------------------------------------------------------------


def _extract_jsonld_locations(html: str) -> list[dict]:
    """Extract linked locations from JSON-LD structured data."""
    locations: list[dict] = []
    pattern = re.compile(
        r'<script\s+[^>]*type\s*=\s*["\']application/ld\+json["\'][^>]*>'
        r"(.*?)</script>",
        re.DOTALL | re.IGNORECASE,
    )
    for match in pattern.finditer(html):
        try:
            data = json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            continue
        if isinstance(data, dict) and "@graph" in data:
            items = data["@graph"]
        elif isinstance(data, list):
            items = data
        else:
            items = [data]
        for item in items:
            _extract_jsonld_item(item, locations)
    return locations


def _extract_jsonld_item(
    item: Any, locations: list[dict], depth: int = 0
) -> None:
    """Recursively extract linked locations from a JSON-LD item."""
    if not isinstance(item, dict) or depth > 5:
        return

    item_name = item.get("name", "")

    address = ""
    addr = item.get("address")
    if isinstance(addr, dict):
        parts = []
        for field in (
            "streetAddress",
            "addressLocality",
            "addressRegion",
            "postalCode",
            "addressCountry",
        ):
            val = addr.get(field)
            if val and isinstance(val, str):
                parts.append(val)
        if parts:
            address = ", ".join(parts)
    elif isinstance(addr, str) and addr.strip():
        address = addr.strip()

    lat_f, lng_f = None, None
    geo = item.get("geo")
    if isinstance(geo, dict):
        lat = geo.get("latitude")
        lng = geo.get("longitude")
        if lat is not None and lng is not None:
            try:
                lat_f, lng_f = float(lat), float(lng)
                if not _is_valid_coord(lat_f, lng_f):
                    lat_f, lng_f = None, None
            except (ValueError, TypeError):
                lat_f, lng_f = None, None

    if item_name or address:
        locations.append({
            "name": item_name,
            "address": address,
            "lat": lat_f,
            "lng": lng_f,
            "source": "JSON-LD",
        })

    for key, val in item.items():
        if key.startswith("@") or key in ("geo", "address"):
            continue
        if isinstance(val, dict):
            _extract_jsonld_item(val, locations, depth + 1)
        elif isinstance(val, list):
            for v in val:
                if isinstance(v, dict):
                    _extract_jsonld_item(v, locations, depth + 1)


# ---------------------------------------------------------------------------
# Google Maps coordinates
# ---------------------------------------------------------------------------


def _extract_google_maps_coords(html: str) -> list[dict]:
    """Extract coordinates from Google Maps links and iframes."""
    coords: list[dict] = []
    seen: set[tuple[float, float]] = set()
    for link_match in _GMAPS_LINK_RE.finditer(html):
        url = link_match.group(1)
        for pat in _GMAPS_PATTERNS:
            for m in pat.finditer(url):
                try:
                    lat, lng = float(m.group(1)), float(m.group(2))
                    if _is_valid_coord(lat, lng) and (lat, lng) not in seen:
                        seen.add((lat, lng))
                        coords.append({
                            "lat": lat,
                            "lng": lng,
                            "source": "Google Maps link",
                        })
                except (ValueError, IndexError):
                    pass
    return coords


# ---------------------------------------------------------------------------
# Data attribute locations
# ---------------------------------------------------------------------------


def _extract_data_attr_coords(html: str) -> tuple[list[dict], list[dict]]:
    """Extract locations and coordinates from data-lat/data-lng attributes.

    Returns (locations, coordinates).
    """
    locations: list[dict] = []
    coords: list[dict] = []

    tag_re = re.compile(
        r'<[^>]+data-lat(?:itude)?\s*=\s*["\'][^"\']+["\'][^>]*>',
        re.IGNORECASE,
    )
    for tag_match in tag_re.finditer(html):
        tag = tag_match.group(0)
        lat_m = re.search(
            r'data-lat(?:itude)?\s*=\s*["\'](-?\d{1,3}\.\d{2,8})["\']',
            tag,
            re.IGNORECASE,
        )
        lng_m = re.search(
            r'data-(?:lng|lon(?:gitude)?)\s*=\s*["\'](-?\d{1,3}\.\d{2,8})["\']',
            tag,
            re.IGNORECASE,
        )
        if not lat_m or not lng_m:
            continue
        try:
            lat, lng = float(lat_m.group(1)), float(lng_m.group(1))
            if not _is_valid_coord(lat, lng):
                continue
        except (ValueError, TypeError):
            continue

        name_m = re.search(
            r'data-name\s*=\s*["\']([^"\']+)["\']', tag, re.IGNORECASE
        )
        addr_m = re.search(
            r'data-address\s*=\s*["\']([^"\']+)["\']', tag, re.IGNORECASE
        )
        name = name_m.group(1).strip() if name_m else ""
        address = addr_m.group(1).strip() if addr_m else ""

        if name or address:
            locations.append({
                "name": name,
                "address": address,
                "lat": lat,
                "lng": lng,
                "source": "data attribute",
            })
        else:
            coords.append({
                "lat": lat,
                "lng": lng,
                "source": "data attribute",
            })
    return locations, coords


# ---------------------------------------------------------------------------
# Meta geo tags
# ---------------------------------------------------------------------------


def _extract_meta_geo(html: str) -> list[dict]:
    """Extract coordinates from geo.position, ICBM, og:latitude meta tags."""
    coords: list[dict] = []
    for m in _META_GEO_POSITION_RE.finditer(html):
        parts = re.split(r"[;,]\s*", m.group(1))
        if len(parts) >= 2:
            try:
                lat, lng = float(parts[0].strip()), float(parts[1].strip())
                if _is_valid_coord(lat, lng):
                    coords.append({
                        "lat": lat,
                        "lng": lng,
                        "source": "meta geo.position",
                    })
            except (ValueError, IndexError):
                pass

    lat_match = _META_OG_LAT_RE.search(html)
    lng_match = _META_OG_LNG_RE.search(html)
    if lat_match and lng_match:
        try:
            lat, lng = float(lat_match.group(1)), float(lng_match.group(1))
            if _is_valid_coord(lat, lng):
                coords.append({
                    "lat": lat,
                    "lng": lng,
                    "source": "meta og:latitude/longitude",
                })
        except ValueError:
            pass
    return coords


# ---------------------------------------------------------------------------
# Inline JS coordinates
# ---------------------------------------------------------------------------


def _extract_inline_js_coords(html: str) -> list[dict]:
    """Extract coordinates from inline <script> tags."""
    coords: list[dict] = []
    script_re = re.compile(
        r"<script[^>]*>(.*?)</script>", re.DOTALL | re.IGNORECASE
    )
    for script_match in script_re.finditer(html):
        js_code = script_match.group(1)
        if len(js_code) > 500_000:
            continue

        for m in _JS_LATLNG_RE.finditer(js_code):
            try:
                lat, lng = float(m.group(1)), float(m.group(2))
                if _is_valid_coord(lat, lng):
                    coords.append({
                        "lat": lat,
                        "lng": lng,
                        "source": "inline JS LatLng",
                    })
            except ValueError:
                pass

        for m in _JS_LAT_LNG_OBJ_RE.finditer(js_code):
            try:
                lat, lng = float(m.group(1)), float(m.group(2))
                if _is_valid_coord(lat, lng):
                    coords.append({
                        "lat": lat,
                        "lng": lng,
                        "source": "inline JS object",
                    })
            except ValueError:
                pass

        for m in _JS_COORD_ARRAY_RE.finditer(js_code):
            try:
                a, b = float(m.group(1)), float(m.group(2))
                if _is_valid_coord(a, b) and abs(a) <= 90:
                    coords.append({
                        "lat": a,
                        "lng": b,
                        "source": "inline JS array",
                    })
            except ValueError:
                pass
    return coords


# ---------------------------------------------------------------------------
# Raw HTML coordinate patterns
# ---------------------------------------------------------------------------


def _extract_html_coordinates(html: str) -> list[dict]:
    """Extract coordinates from raw text patterns in HTML."""
    coords: list[dict] = []
    for m in _COORD_RE.finditer(html):
        pair = re.split(r"[,/]\s*", m.group(0))
        if len(pair) >= 2:
            try:
                lat, lng = float(pair[0].strip()), float(pair[1].strip())
                if _is_valid_coord(lat, lng):
                    coords.append({
                        "lat": lat,
                        "lng": lng,
                        "source": "HTML text pattern",
                    })
            except ValueError:
                pass
    return coords


# ---------------------------------------------------------------------------
# Embedded JSON / GeoJSON
# ---------------------------------------------------------------------------


def _extract_embedded_json(html: str) -> tuple[list[dict], list[dict]]:
    """Extract locations and coordinates from <script type="application/json">."""
    locations: list[dict] = []
    coords: list[dict] = []
    pattern = re.compile(
        r'<script\s+[^>]*type\s*=\s*["\']application/json["\'][^>]*>'
        r"(.*?)</script>",
        re.DOTALL | re.IGNORECASE,
    )
    for match in pattern.finditer(html):
        raw = match.group(1)
        if len(raw) > 1_000_000:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            continue
        _walk_json_for_locations(data, locations, coords, depth=0)
    return locations, coords


def _walk_json_for_locations(
    obj: Any,
    locations: list[dict],
    coords: list[dict],
    depth: int = 0,
) -> None:
    """Recursively walk a JSON structure for GeoJSON features."""
    if depth > 10 or obj is None:
        return
    if isinstance(obj, dict):
        geom = obj.get("geometry")
        if isinstance(geom, dict) and geom.get("type") == "Point":
            c = geom.get("coordinates")
            if isinstance(c, list) and len(c) >= 2:
                try:
                    lng, lat = float(c[0]), float(c[1])
                    if _is_valid_coord(lat, lng):
                        name = ""
                        props = obj.get("properties")
                        if isinstance(props, dict):
                            name = (
                                props.get("tooltip")
                                or props.get("name")
                                or props.get("title")
                                or ""
                            )
                        if name:
                            locations.append({
                                "name": str(name)[:100],
                                "address": "",
                                "lat": lat,
                                "lng": lng,
                                "source": "embedded GeoJSON",
                            })
                        else:
                            coords.append({
                                "lat": lat,
                                "lng": lng,
                                "source": "embedded GeoJSON",
                            })
                except (ValueError, TypeError, IndexError):
                    pass
        for val in obj.values():
            if isinstance(val, (dict, list)):
                _walk_json_for_locations(val, locations, coords, depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                _walk_json_for_locations(item, locations, coords, depth + 1)


# ---------------------------------------------------------------------------
# Coordinate deduplication
# ---------------------------------------------------------------------------


def _dedup_coords(coords: list[dict]) -> list[dict]:
    """Deduplicate coordinates within ~55m of each other."""
    if not coords:
        return coords
    result: list[dict] = []
    for coord in coords:
        lat, lng = coord["lat"], coord["lng"]
        is_dup = False
        for existing in result:
            if (
                abs(existing["lat"] - lat) < _COORD_DEDUP_THRESHOLD
                and abs(existing["lng"] - lng) < _COORD_DEDUP_THRESHOLD
            ):
                is_dup = True
                break
        if not is_dup:
            result.append(coord)
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_signals(html: str) -> dict:
    """Extract coordinates and addresses from raw HTML.

    Returns:
        {
            "coordinates": [(lat, lng), ...],
            "addresses": [str, ...],
        }
    """
    if not html:
        return {"coordinates": [], "addresses": []}

    all_locations: list[dict] = []
    all_coords: list[dict] = []

    html_no_svg = _SVG_STRIP_RE.sub("", html)

    # JSON-LD (richest source)
    all_locations.extend(_extract_jsonld_locations(html))

    # data-lat/data-lng attributes
    data_locs, data_coords = _extract_data_attr_coords(html_no_svg)
    all_locations.extend(data_locs)
    all_coords.extend(data_coords)

    # Embedded JSON/GeoJSON
    json_locs, json_coords = _extract_embedded_json(html)
    all_locations.extend(json_locs)
    all_coords.extend(json_coords)

    # Google Maps links/iframes
    all_coords.extend(_extract_google_maps_coords(html))

    # Meta geo tags
    all_coords.extend(_extract_meta_geo(html))

    # Inline JS
    all_coords.extend(_extract_inline_js_coords(html))

    # Raw HTML text patterns (lowest priority)
    all_coords.extend(_extract_html_coordinates(html_no_svg))

    # Dedup coordinates
    all_coords = _dedup_coords(all_coords)

    # Flatten to tuples and address strings
    coordinates = [(c["lat"], c["lng"]) for c in all_coords]
    for loc in all_locations:
        if loc.get("lat") is not None and loc.get("lng") is not None:
            coordinates.append((loc["lat"], loc["lng"]))

    addresses = [
        loc["address"] for loc in all_locations if loc.get("address")
    ]

    return {
        "coordinates": coordinates,
        "addresses": addresses,
    }


def inject_signals(markdown: str, signals: dict) -> str:
    """Inject extracted signals as a header at the top of markdown."""
    if not signals:
        return markdown

    coords = signals.get("coordinates", [])
    addresses = signals.get("addresses", [])

    if not coords and not addresses:
        return markdown

    lines = ["## Extracted Location Signals\n"]

    if addresses:
        lines.append("**Addresses:**")
        for addr in addresses:
            lines.append(f"- {addr}")
        lines.append("")

    if coords:
        lines.append("**Coordinates:**")
        for lat, lng in coords:
            lines.append(f"- ({lat:.6f}, {lng:.6f})")
        lines.append("")

    lines.append("---\n")
    return "\n".join(lines) + markdown
