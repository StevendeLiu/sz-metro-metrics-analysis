"""Coordinate-based district classifier for Shenzhen Metro stations.

Assigns each station to a Shenzhen administrative district using rectangular
bounding boxes with priority rules (higher priority wins on overlap).  A
``data/raw/station_districts.csv`` override file takes precedence for any
station listed there, allowing manual correction of edge cases.

District boundaries are approximate (GCJ-02 system) and based on the
geographic distribution of metro stations across Shenzhen.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_OVERRIDE_FILE = "station_districts.csv"


@dataclass(frozen=True)
class _DistrictBox:
    name: str
    min_lon: float
    max_lon: float
    min_lat: float
    max_lat: float
    priority: int  # Higher = more specific; wins when boxes overlap

    def contains(self, lon: float, lat: float) -> bool:
        return (
            self.min_lon <= lon <= self.max_lon
            and self.min_lat <= lat <= self.max_lat
        )


# Approximate district bounding boxes (GCJ-02).
# Priority: 10 = most specific; 5 = large/fallback region.
_BOXES: list[_DistrictBox] = [
    _DistrictBox("光明区",   113.845, 113.980, 22.695, 22.820, 10),
    _DistrictBox("宝安区",   113.755, 114.005, 22.530, 22.830,  5),
    _DistrictBox("南山区",   113.870, 114.030, 22.440, 22.580,  8),
    _DistrictBox("福田区",   113.940, 114.085, 22.495, 22.615,  9),
    _DistrictBox("罗湖区",   114.075, 114.225, 22.490, 22.640,  9),
    _DistrictBox("盐田区",   114.160, 114.355, 22.465, 22.640, 10),
    _DistrictBox("龙华区",   113.965, 114.110, 22.580, 22.790,  8),
    _DistrictBox("龙岗区",   113.890, 114.285, 22.570, 22.820,  5),
    _DistrictBox("坪山区",   114.230, 114.450, 22.620, 22.830, 10),
]

# Centroids for nearest-centroid fallback (lon, lat)
_CENTROIDS: dict[str, tuple[float, float]] = {
    b.name: ((b.min_lon + b.max_lon) / 2, (b.min_lat + b.max_lat) / 2)
    for b in _BOXES
}


def _classify_by_coord(lon: float, lat: float) -> str:
    """Return the district name for a GCJ-02 coordinate.

    Selects the highest-priority bounding box that contains the point.
    Falls back to the nearest centroid if no box matches.

    Args:
        lon: Longitude in GCJ-02.
        lat: Latitude  in GCJ-02.

    Returns:
        District name string.
    """
    matches = [b for b in _BOXES if b.contains(lon, lat)]
    if matches:
        return max(matches, key=lambda b: b.priority).name

    # Fallback: nearest centroid (Euclidean in degree space — fine for small area)
    return min(
        _CENTROIDS,
        key=lambda name: (lon - _CENTROIDS[name][0]) ** 2
        + (lat - _CENTROIDS[name][1]) ** 2,
    )


def assign_districts(
    stations: list[dict],
    raw_dir: Path | None = None,
) -> dict[str, str]:
    """Build a mapping of ``station_id → district_name``.

    Manual overrides from ``<raw_dir>/station_districts.csv`` (columns:
    ``station_id``, ``district``) take precedence over coordinate inference.

    Args:
        stations: Deduplicated station dicts (from ``stations.csv``).
        raw_dir: Directory to look for ``station_districts.csv`` overrides.

    Returns:
        Dict mapping each ``station_id`` to its assigned district name.
    """
    # Load manual overrides first
    overrides: dict[str, str] = {}
    if raw_dir:
        override_path = raw_dir / _OVERRIDE_FILE
        if override_path.exists():
            with override_path.open(encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    overrides[row["station_id"]] = row["district"]
            logger.info(
                "Loaded %d district overrides from %s.", len(overrides), override_path
            )

    # Deduplicate stations by station_id
    seen: set[str] = set()
    unique: list[dict] = []
    for s in stations:
        if s["station_id"] not in seen:
            seen.add(s["station_id"])
            unique.append(s)

    result: dict[str, str] = {}
    fallback_count = 0
    for s in unique:
        sid = s["station_id"]
        if sid in overrides:
            result[sid] = overrides[sid]
            continue
        try:
            lon, lat = float(s["longitude"]), float(s["latitude"])
            result[sid] = _classify_by_coord(lon, lat)
        except (ValueError, TypeError):
            result[sid] = "未知"
            fallback_count += 1

    if fallback_count:
        logger.warning("%d stations had no valid coordinates.", fallback_count)

    dist_counts: dict[str, int] = {}
    for d in result.values():
        dist_counts[d] = dist_counts.get(d, 0) + 1
    logger.info(
        "District assignment: %s",
        ", ".join(f"{k}={v}" for k, v in sorted(dist_counts.items())),
    )
    return result
