"""Aggregator pipeline: matrix → district metrics → complexity classification."""

from __future__ import annotations

import asyncio
import csv
import logging
from pathlib import Path

import pandas as pd

from .classifier import assign_districts
from .geocode import geocode_districts
from .metrics import (
    classify_complexity,
    district_metrics,
    pairwise_district_matrix,
    station_accessibility,
)

logger = logging.getLogger(__name__)

_DEFAULT_RAW_DIR = Path("data/raw")
_DEFAULT_PROCESSED_DIR = Path("data/processed")


# ── I/O helpers ───────────────────────────────────────────────────────────────

def _load_stations(raw_dir: Path) -> list[dict]:
    path = raw_dir / "stations.csv"
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _save_csv(df: pd.DataFrame, path: Path, **kwargs) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, **kwargs)
    logger.info("Saved → %s  (%d rows)", path, len(df))


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(
    raw_dir: Path = _DEFAULT_RAW_DIR,
    processed_dir: Path = _DEFAULT_PROCESSED_DIR,
    use_geocoding: bool = True,
) -> dict[str, pd.DataFrame]:
    """Execute the full aggregation pipeline.

    Steps:
        1. Load ``travel_time_matrix.csv`` from *processed_dir*.
        2. Assign districts to stations:
           - If *use_geocoding* is True and ``station_districts.csv`` is
             missing or incomplete, run Nominatim reverse-geocoding.
           - Falls back to coordinate-based bounding-box classifier.
        3. Compute per-station accessibility (mean/median travel time).
        4. Classify stations into complexity tiers (Simple/Moderate/Complex/Remote).
        5. Aggregate into district-level summary metrics.
        6. Build pairwise district × district travel-time matrix.
        7. Persist all outputs to *processed_dir*.

    Args:
        raw_dir: Directory containing scraper outputs and optional overrides.
        processed_dir: Directory containing ``travel_time_matrix.csv``; also
                       used as the destination for aggregator outputs.
        use_geocoding: Whether to run Nominatim geocoding for missing districts.

    Returns:
        Dict with keys ``"station_metrics"``, ``"district_metrics"``,
        ``"pairwise_matrix"``.
    """
    matrix_path = processed_dir / "travel_time_matrix.csv"
    if not matrix_path.exists():
        raise FileNotFoundError(
            f"Travel-time matrix not found: {matrix_path}\n"
            "Run the engine first: python -m src.engine"
        )

    # 1. Load matrix
    logger.info("Loading travel-time matrix from %s …", matrix_path)
    matrix = pd.read_csv(matrix_path, index_col=0, dtype=str)
    matrix.index = matrix.index.astype(str)
    matrix.columns = matrix.columns.astype(str)
    matrix = matrix.astype(float)
    logger.info("Matrix shape: %s", matrix.shape)

    # 2. Load stations and assign districts
    stations = _load_stations(raw_dir)

    if use_geocoding:
        logger.info("Resolving districts via Nominatim geocoding …")
        station_district = asyncio.run(geocode_districts(stations, raw_dir))
    else:
        station_district = assign_districts(stations, raw_dir)

    # 3. Station accessibility
    logger.info("Computing station accessibility metrics …")
    station_acc = station_accessibility(matrix, station_district)

    # 4. Complexity classification
    station_acc = classify_complexity(station_acc)

    # 5. District-level summary
    dist_metrics = district_metrics(station_acc)

    # 6. Pairwise district matrix
    pair_matrix = pairwise_district_matrix(matrix, station_district)

    # 7. Persist
    _save_csv(station_acc, processed_dir / "station_metrics.csv", index=False)
    _save_csv(dist_metrics, processed_dir / "district_metrics.csv", index=False)
    _save_csv(pair_matrix, processed_dir / "district_pairwise_matrix.csv")

    # Summary log
    logger.info(
        "Aggregation complete — %d stations, %d districts.",
        len(station_acc), len(dist_metrics),
    )
    complexity_counts = station_acc["complexity"].value_counts().to_dict()
    logger.info("Complexity: %s", complexity_counts)

    return {
        "station_metrics": station_acc,
        "district_metrics": dist_metrics,
        "pairwise_matrix": pair_matrix,
    }


def main() -> None:
    """CLI entry point: ``python -m src.aggregator``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    run()


if __name__ == "__main__":
    main()
