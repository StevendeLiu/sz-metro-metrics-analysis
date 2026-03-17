"""Reverse-geocode station coordinates to Shenzhen administrative districts.

Uses Nominatim (OpenStreetMap) — free, no API key required.

Coordinate system note
----------------------
Amap data uses GCJ-02 ("Mars coordinates"), while Nominatim expects WGS-84.
The ``gcj02_to_wgs84`` conversion is applied before every lookup.

Rate limiting
-------------
Nominatim enforces a 1 req/s policy.  Results are cached in
``data/raw/station_districts.csv`` so subsequent runs skip already-resolved
stations.
"""

from __future__ import annotations

import asyncio
import csv
import logging
import math
import time
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

_NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
_CACHE_FILE = "station_districts.csv"
_RATE_LIMIT_SLEEP = 1.1  # seconds between requests (Nominatim policy: ≥ 1 s)

# Shenzhen district Chinese → canonical name mapping
# (Nominatim may return English or variant spellings)
_DISTRICT_ALIASES: dict[str, str] = {
    "Futian": "福田区",
    "Luohu": "罗湖区",
    "Nanshan": "南山区",
    "Yantian": "盐田区",
    "Bao'an": "宝安区",
    "Baoan": "宝安区",
    "Longhua": "龙华区",
    "Longgang": "龙岗区",
    "Pingshan": "坪山区",
    "Guangming": "光明区",
    "Dapeng": "大鹏新区",
    "福田区": "福田区",
    "罗湖区": "罗湖区",
    "南山区": "南山区",
    "盐田区": "盐田区",
    "宝安区": "宝安区",
    "龙华区": "龙华区",
    "龙岗区": "龙岗区",
    "坪山区": "坪山区",
    "光明区": "光明区",
    "大鹏新区": "大鹏新区",
}


# ── GCJ-02 → WGS-84 conversion ───────────────────────────────────────────────

_A = 6378245.0
_EE = 0.00669342162296594323


def _transform_lat(x: float, y: float) -> float:
    ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * math.sqrt(abs(x))
    ret += (20.0 * math.sin(6.0 * x * math.pi) + 20.0 * math.sin(2.0 * x * math.pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(y * math.pi) + 40.0 * math.sin(y / 3.0 * math.pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(y / 12.0 * math.pi) + 320.0 * math.sin(y * math.pi / 30.0)) * 2.0 / 3.0
    return ret


def _transform_lon(x: float, y: float) -> float:
    ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * math.sqrt(abs(x))
    ret += (20.0 * math.sin(6.0 * x * math.pi) + 20.0 * math.sin(2.0 * x * math.pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(x * math.pi) + 40.0 * math.sin(x / 3.0 * math.pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(x / 12.0 * math.pi) + 300.0 * math.sin(x / 30.0 * math.pi)) * 2.0 / 3.0
    return ret


def gcj02_to_wgs84(gcj_lon: float, gcj_lat: float) -> tuple[float, float]:
    """Convert GCJ-02 coordinates to WGS-84.

    Args:
        gcj_lon: Longitude in GCJ-02 (Amap).
        gcj_lat: Latitude  in GCJ-02 (Amap).

    Returns:
        ``(wgs_lon, wgs_lat)`` in WGS-84.
    """
    d_lat = _transform_lat(gcj_lon - 105.0, gcj_lat - 35.0)
    d_lon = _transform_lon(gcj_lon - 105.0, gcj_lat - 35.0)
    rad_lat = gcj_lat / 180.0 * math.pi
    magic = math.sin(rad_lat)
    magic = 1 - _EE * magic * magic
    sqrt_magic = math.sqrt(magic)
    d_lat = d_lat * 180.0 / ((_A * (1 - _EE)) / (magic * sqrt_magic) * math.pi)
    d_lon = d_lon * 180.0 / (_A / sqrt_magic * math.cos(rad_lat) * math.pi)
    return gcj_lon - d_lon, gcj_lat - d_lat


# ── Nominatim lookup ──────────────────────────────────────────────────────────

def _extract_district(address: dict) -> str:
    """Pull district from a Nominatim address dict.

    For Shenzhen, Nominatim maps 区-level divisions to the ``"city"`` key
    (e.g., ``"罗湖区"``).  We check ``"city"`` first, then fall back to
    ``"city_district"`` and ``"county"`` for edge cases.

    Args:
        address: The ``address`` sub-dict from a Nominatim reverse response.

    Returns:
        Canonical district name, or ``"未知"`` if not determinable.
    """
    for key in ("city", "city_district", "county", "state_district", "suburb"):
        value = address.get(key, "")
        if not value:
            continue
        # Direct canonical match
        canonical = _DISTRICT_ALIASES.get(value)
        if canonical:
            return canonical
        # Substring match (handles "Futian District", "宝安区深圳" etc.)
        for alias, canon in _DISTRICT_ALIASES.items():
            if alias in value:
                return canon
    return "未知"


async def _lookup_one(
    client: httpx.AsyncClient,
    station_id: str,
    gcj_lon: float,
    gcj_lat: float,
) -> tuple[str, str]:
    """Reverse-geocode one station.  Returns ``(station_id, district)``."""
    wgs_lon, wgs_lat = gcj02_to_wgs84(gcj_lon, gcj_lat)
    params = {
        "lat": f"{wgs_lat:.6f}",
        "lon": f"{wgs_lon:.6f}",
        "format": "jsonv2",
        "accept-language": "zh-CN,zh",
        "zoom": 10,  # city_district level
    }
    try:
        resp = await client.get(_NOMINATIM_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        district = _extract_district(data.get("address", {}))
    except Exception as exc:
        logger.warning("Nominatim failed for %s: %s", station_id, exc)
        district = "未知"
    return station_id, district


async def geocode_districts(
    stations: list[dict],
    raw_dir: Path,
) -> dict[str, str]:
    """Resolve district for every station via Nominatim, using a cache file.

    Already-resolved stations (present in ``station_districts.csv``) are
    skipped.  New results are appended and the file is updated atomically.

    Args:
        stations: Deduplicated list of station dicts (need ``longitude``,
                  ``latitude``, ``station_id``).
        raw_dir: Directory for the cache file ``station_districts.csv``.

    Returns:
        Full ``station_id → district`` mapping (cached + newly resolved).
    """
    cache_path = raw_dir / _CACHE_FILE

    # Load existing cache
    cached: dict[str, str] = {}
    if cache_path.exists():
        with cache_path.open(encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                cached[row["station_id"]] = row["district"]
        logger.info("Loaded %d cached district entries.", len(cached))

    # Deduplicate stations by station_id
    seen: set[str] = set()
    unique: list[dict] = []
    for s in stations:
        if s["station_id"] not in seen:
            seen.add(s["station_id"])
            unique.append(s)

    # Find stations not yet resolved
    pending = [
        s for s in unique
        if s["station_id"] not in cached
        and s.get("longitude") and s.get("latitude")
    ]

    if not pending:
        logger.info("All %d stations already geocoded.", len(cached))
        return cached

    logger.info(
        "Geocoding %d stations via Nominatim (rate-limited to 1 req/s) …",
        len(pending),
    )

    headers = {
        "User-Agent": "SZ-Metro-Metrics/1.0 (educational project)",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }

    new_results: dict[str, str] = {}
    async with httpx.AsyncClient(headers=headers, timeout=15.0) as client:
        for i, s in enumerate(pending):
            sid = s["station_id"]
            try:
                gcj_lon = float(s["longitude"])
                gcj_lat = float(s["latitude"])
            except (ValueError, TypeError):
                new_results[sid] = "未知"
                continue

            _, district = await _lookup_one(client, sid, gcj_lon, gcj_lat)
            new_results[sid] = district

            if (i + 1) % 10 == 0:
                logger.info("  Geocoded %d/%d …", i + 1, len(pending))

            # Nominatim rate limit: 1 request/second
            await asyncio.sleep(_RATE_LIMIT_SLEEP)

    # Merge and persist
    cached.update(new_results)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["station_id", "district"])
        writer.writeheader()
        for sid, district in cached.items():
            writer.writerow({"station_id": sid, "district": district})

    logger.info(
        "Geocoding complete — %d resolved, %d total. Saved → %s",
        len(new_results), len(cached), cache_path,
    )

    # Log unknown count
    unknown = sum(1 for d in cached.values() if d == "未知")
    if unknown:
        logger.warning("%d stations could not be resolved to a district.", unknown)

    return cached
