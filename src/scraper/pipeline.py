"""Main scraping pipeline: fetch → parse → persist."""

from __future__ import annotations

import asyncio
import csv
import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from tqdm.asyncio import tqdm

from .client import build_client
from .fetcher import fetch_route_duration, fetch_topology
from .models import RawMetroData, Segment, Station
from .parser import build_segments, parse_stations

load_dotenv()

logger = logging.getLogger(__name__)

_DEFAULT_OUTPUT_DIR = Path("data/raw")
_AMAP_API_KEY: str = os.getenv("AMAP_API_KEY", "")


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def _save_stations_csv(stations: list[Station], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "station_id", "name", "line_id", "line_name",
                "district", "longitude", "latitude",
                "is_transfer", "transfer_line_ids",
            ],
        )
        writer.writeheader()
        for s in stations:
            row = s.model_dump()
            row["transfer_line_ids"] = "|".join(s.transfer_line_ids)
            writer.writerow(row)
    logger.info("Saved %d stations → %s", len(stations), path)


def _save_segments_csv(segments: list[Segment], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["from_station_id", "to_station_id", "line_id", "duration_minutes"],
        )
        writer.writeheader()
        writer.writerows(s.model_dump() for s in segments)
    logger.info("Saved %d segments → %s", len(segments), path)


def _save_json(data: RawMetroData, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data.model_dump_json(indent=2), encoding="utf-8")
    logger.info("Saved raw data → %s", path)


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

async def _fetch_route_durations(
    client: Any,
    payload: dict[str, Any],
) -> dict[tuple[str, str], float]:
    """Query Amap routing API for every adjacent station pair (if API key set).

    Args:
        client: Shared async HTTP client.
        payload: Raw Amap topology JSON.

    Returns:
        Mapping ``(from_sid, to_sid)`` → travel time in minutes.
        Empty dict if no API key is configured.
    """
    if not _AMAP_API_KEY:
        logger.info(
            "AMAP_API_KEY not set — travel times will be estimated from coordinates."
        )
        return {}

    # Collect adjacent pairs (station_id, coord_str) per line
    pairs: list[tuple[str, str, str, str]] = []  # (sid_a, coord_a, sid_b, coord_b)
    for line in payload.get("l", []):
        st_list = line.get("st", [])
        for i in range(len(st_list) - 1):
            a, b = st_list[i], st_list[i + 1]
            if a.get("sl") and b.get("sl"):
                pairs.append((str(a["sid"]), a["sl"], str(b["sid"]), b["sl"]))

    logger.info(
        "Querying Amap routing API for %d adjacent pairs …", len(pairs)
    )

    async def _query(sid_a: str, coord_a: str, sid_b: str, coord_b: str) -> tuple[tuple[str, str], float | None]:
        dur = await fetch_route_duration(client, coord_a, coord_b)
        return (sid_a, sid_b), dur

    tasks = [_query(*p) for p in pairs]
    results = await tqdm.gather(*tasks, desc="Routing queries")

    return {k: v for k, v in results if v is not None}


async def run(output_dir: Path = _DEFAULT_OUTPUT_DIR) -> RawMetroData:
    """Execute the full scraping pipeline.

    Steps:
        1. Fetch subway topology from Amap.
        2. Parse station list.
        3. If ``AMAP_API_KEY`` is set, query routing API for precise travel
           times; otherwise estimate from station coordinates.
        4. Build directed segment list.
        5. Persist to CSV and JSON under *output_dir*.

    Args:
        output_dir: Directory where output files are written.

    Returns:
        Assembled ``RawMetroData`` instance.

    Raises:
        ScraperError: On unrecoverable network or parsing failures.
    """
    async with build_client() as client:
        # 1. Topology
        logger.info("Step 1/3 — fetching subway topology …")
        payload = await fetch_topology(client)

        # 2. Parse stations
        logger.info("Step 2/3 — parsing stations …")
        stations: list[Station] = parse_stations(payload)

        # 3. Travel times (routing API or coordinate estimation)
        logger.info("Step 3/3 — computing segment travel times …")
        route_durations = await _fetch_route_durations(client, payload)

    # 4. Build segments
    segments: list[Segment] = build_segments(payload, route_durations)

    # 5. Persist
    data = RawMetroData(stations=stations, segments=segments)
    _save_stations_csv(stations, output_dir / "stations.csv")
    _save_segments_csv(segments, output_dir / "segments.csv")
    _save_json(data, output_dir / "metro_raw.json")

    logger.info(
        "Done — %d stations, %d directed segment edges.",
        len(stations), len(segments),
    )
    return data


def main() -> None:
    """CLI entry point: ``python -m src.scraper``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
