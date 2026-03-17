"""Parsers for Amap subway topology payloads and segment-time estimation."""

from __future__ import annotations

import logging
import math
from typing import Any

from .exceptions import ScraperError
from .models import Segment, Station

logger = logging.getLogger(__name__)

# Assumed average metro speed (km/h) and dwell time (minutes) used when
# no routing API key is available.
_METRO_SPEED_KMH: float = 35.0
_DWELL_MINUTES: float = 0.5


def _haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Return the great-circle distance in kilometres between two GCJ-02 points.

    Args:
        lon1: Longitude of point A in decimal degrees.
        lat1: Latitude  of point A in decimal degrees.
        lon2: Longitude of point B in decimal degrees.
        lat2: Latitude  of point B in decimal degrees.

    Returns:
        Distance in kilometres.
    """
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _estimate_duration(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Estimate travel time in minutes between two stations from their coordinates.

    Args:
        lon1: Longitude of the departure station.
        lat1: Latitude  of the departure station.
        lon2: Longitude of the arrival station.
        lat2: Latitude  of the arrival station.

    Returns:
        Estimated travel time in minutes (rounded to 2 decimal places).
    """
    dist_km = _haversine_km(lon1, lat1, lon2, lat2)
    travel_min = (dist_km / _METRO_SPEED_KMH) * 60
    return round(travel_min + _DWELL_MINUTES, 2)


def _parse_coord(sl: str) -> tuple[float, float] | None:
    """Parse a ``"longitude,latitude"`` string into a float tuple.

    Args:
        sl: Raw coordinate string from Amap (e.g. ``"114.118666,22.532083"``).

    Returns:
        ``(longitude, latitude)`` or ``None`` on parse error.
    """
    try:
        lon_str, lat_str = sl.split(",")
        return float(lon_str.strip()), float(lat_str.strip())
    except (ValueError, AttributeError):
        return None


def parse_stations(payload: dict[str, Any]) -> list[Station]:
    """Parse an Amap subway topology payload into ``Station`` objects.

    Args:
        payload: Raw Amap JSON (must contain key ``"l"``).

    Returns:
        Flat list of ``Station`` instances (one per station-line pair).

    Raises:
        ScraperError: If the ``"l"`` key is missing or malformed.
    """
    try:
        lines: list[dict[str, Any]] = payload["l"]
    except (KeyError, TypeError) as exc:
        raise ScraperError(f"Missing 'l' in topology payload: {exc}") from exc

    stations: list[Station] = []
    for line in lines:
        line_id: str = str(line.get("ls", ""))
        line_name: str = line.get("ln", "")
        for raw in line.get("st", []):
            coord = _parse_coord(raw.get("sl", ""))
            lon, lat = coord if coord else (None, None)

            # Transfer line IDs: pipe-separated in "r", excluding the current line.
            raw_r: str = raw.get("r", "")
            transfer_ids = [t for t in raw_r.split("|") if t and t != line_id]
            is_transfer = str(raw.get("t", "0")) == "1" or len(transfer_ids) > 0

            stations.append(
                Station(
                    station_id=str(raw["sid"]),
                    name=raw.get("n", ""),
                    line_id=line_id,
                    line_name=line_name,
                    longitude=lon,
                    latitude=lat,
                    is_transfer=is_transfer,
                    transfer_line_ids=transfer_ids,
                )
            )

    logger.info(
        "Parsed %d stations across %d lines.", len(stations), len(lines)
    )
    return stations


def build_segments(
    payload: dict[str, Any],
    route_durations: dict[tuple[str, str], float] | None = None,
) -> list[Segment]:
    """Build directed ``Segment`` objects from station ordering within each line.

    Adjacent stations (consecutive in Amap's ``st`` list) are treated as
    connected by a segment. Travel time is taken from *route_durations* if
    available, otherwise estimated from station coordinates.

    Args:
        payload: Raw Amap topology JSON (must contain key ``"l"``).
        route_durations: Optional mapping of ``(from_sid, to_sid)`` → minutes
                         sourced from the Amap routing API.

    Returns:
        List of directed ``Segment`` instances (both directions for each pair).

    Raises:
        ScraperError: If the ``"l"`` key is missing or malformed.
    """
    try:
        lines: list[dict[str, Any]] = payload["l"]
    except (KeyError, TypeError) as exc:
        raise ScraperError(f"Missing 'l' in topology payload: {exc}") from exc

    segments: list[Segment] = []
    route_durations = route_durations or {}

    for line in lines:
        line_id: str = str(line.get("ls", ""))
        station_list: list[dict[str, Any]] = line.get("st", [])

        for i in range(len(station_list) - 1):
            a, b = station_list[i], station_list[i + 1]
            sid_a, sid_b = str(a["sid"]), str(b["sid"])

            # Try routing API result first, then estimate from coordinates.
            if (sid_a, sid_b) in route_durations:
                dur = route_durations[(sid_a, sid_b)]
            else:
                coord_a = _parse_coord(a.get("sl", ""))
                coord_b = _parse_coord(b.get("sl", ""))
                if coord_a and coord_b:
                    dur = _estimate_duration(*coord_a, *coord_b)
                else:
                    dur = 2.0  # Absolute fallback: 2 minutes

            segments.append(
                Segment(from_station_id=sid_a, to_station_id=sid_b,
                        line_id=line_id, duration_minutes=dur)
            )
            segments.append(
                Segment(from_station_id=sid_b, to_station_id=sid_a,
                        line_id=line_id, duration_minutes=dur)
            )

    logger.info(
        "Built %d directed segments across %d lines.",
        len(segments), len(lines),
    )
    return segments
