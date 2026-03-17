"""API fetching functions targeting the Amap (高德地图) subway endpoint.

Environment variables (set in .env at project root):

    AMAP_SUBWAY_URL       Amap subway service base URL
                          (default: https://map.amap.com/service/subway)
    SZ_METRO_CITY_CODE    City code for Shenzhen (default: 4403)
    SZ_METRO_CITY_NAME    City pinyin name     (default: shenzhen)
    AMAP_API_KEY          Optional key for Amap routing API (precise travel times).
                          If absent, times are estimated from coordinates.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import httpx
from dotenv import load_dotenv

from .client import get_json
from .exceptions import ScraperError

load_dotenv()

logger = logging.getLogger(__name__)

_AMAP_SUBWAY_URL: str = os.getenv(
    "AMAP_SUBWAY_URL", "https://map.amap.com/service/subway"
)
_CITY_CODE: str = os.getenv("SZ_METRO_CITY_CODE", "4403")
_CITY_NAME: str = os.getenv("SZ_METRO_CITY_NAME", "shenzhen")
_AMAP_API_KEY: str = os.getenv("AMAP_API_KEY", "")
_AMAP_ROUTING_URL: str = (
    "https://restapi.amap.com/v3/direction/transit/integrated"
)


async def fetch_topology(client: httpx.AsyncClient) -> dict[str, Any]:
    """Fetch the full subway topology JSON from Amap.

    Args:
        client: Shared async HTTP client.

    Returns:
        Raw Amap subway payload (keys: ``s``, ``i``, ``l``).

    Raises:
        ScraperError: On network failure or unexpected payload.
    """
    timestamp_ms = int(time.time() * 1000)
    url = (
        f"{_AMAP_SUBWAY_URL}"
        f"?_{timestamp_ms}"
        f"&srhdata={_CITY_CODE}_drw_{_CITY_NAME}.json"
    )
    logger.info("Fetching subway topology from %s", url)
    payload: Any = await get_json(client, url)
    if "l" not in payload:
        raise ScraperError(f"Unexpected topology payload (missing 'l'): {list(payload)}")
    return payload


async def fetch_route_duration(
    client: httpx.AsyncClient,
    origin: str,
    destination: str,
) -> float | None:
    """Query Amap routing API for the transit travel time between two coordinates.

    Args:
        client: Shared async HTTP client.
        origin: ``"longitude,latitude"`` of the departure station.
        destination: ``"longitude,latitude"`` of the arrival station.

    Returns:
        Travel time in minutes, or ``None`` if the API returns no route.

    Raises:
        ScraperError: On network failure. Returns ``None`` on unexpected response.
    """
    if not _AMAP_API_KEY:
        return None

    params: dict[str, Any] = {
        "origin": origin,
        "destination": destination,
        "city": "深圳",
        "key": _AMAP_API_KEY,
        "output": "json",
    }
    try:
        payload: Any = await get_json(client, _AMAP_ROUTING_URL, params=params)
        # Pick the first transit route, first segment duration
        routes = payload.get("route", {}).get("transits", [])
        if routes:
            duration_seconds = float(routes[0]["duration"])
            return round(duration_seconds / 60, 2)
    except (ScraperError, KeyError, IndexError, TypeError, ValueError) as exc:
        logger.warning("Routing API failed for %s→%s: %s", origin, destination, exc)
    return None
