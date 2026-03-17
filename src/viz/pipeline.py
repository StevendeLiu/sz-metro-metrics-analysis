"""Viz pipeline: load processed data → render all charts → save PNGs."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .district_chart import (
    plot_accessibility_scatter,
    plot_complexity_distribution,
    plot_district_bar,
    plot_district_boxplot,
)
from .heatmap import plot_district_heatmap, plot_station_heatmap

logger = logging.getLogger(__name__)

_DEFAULT_PROCESSED_DIR = Path("data/processed")
_DEFAULT_OUTPUT_DIR = Path("data/figures")


def run(
    processed_dir: Path = _DEFAULT_PROCESSED_DIR,
    output_dir: Path = _DEFAULT_OUTPUT_DIR,
) -> None:
    """Render all charts from aggregator outputs.

    Reads:
        ``travel_time_matrix.csv``, ``station_metrics.csv``,
        ``district_metrics.csv``, ``district_pairwise_matrix.csv``
        from *processed_dir*.

    Writes to *output_dir*:
        - ``station_heatmap.png``       — station × station travel-time matrix
        - ``district_heatmap.png``      — district × district mean-time matrix
        - ``district_bar.png``          — mean ± std accessibility bar chart
        - ``district_boxplot.png``      — per-station distribution by district
        - ``complexity_distribution.png`` — complexity tier stacked bar chart
        - ``accessibility_scatter.png`` — mean vs median scatter per station

    Args:
        processed_dir: Directory containing aggregator CSV outputs.
        output_dir: Destination directory for PNG files.
    """
    # ── Load ──────────────────────────────────────────────────────────────────
    def _req(name: str) -> Path:
        p = processed_dir / name
        if not p.exists():
            raise FileNotFoundError(
                f"Required file missing: {p}\n"
                "Run the aggregator first: python -m src.aggregator"
            )
        return p

    logger.info("Loading data from %s …", processed_dir)
    matrix = pd.read_csv(_req("travel_time_matrix.csv"), index_col=0, dtype=str)
    matrix.index = matrix.index.astype(str)
    matrix.columns = matrix.columns.astype(str)
    matrix = matrix.astype(float)

    station_metrics = pd.read_csv(_req("station_metrics.csv"))
    station_metrics["station_id"] = station_metrics["station_id"].astype(str)

    district_metrics = pd.read_csv(_req("district_metrics.csv"))

    pairwise = pd.read_csv(_req("district_pairwise_matrix.csv"), index_col=0)

    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Rendering charts to %s …", output_dir)

    # ── Charts ────────────────────────────────────────────────────────────────
    plot_station_heatmap(
        matrix, station_metrics,
        output_dir / "station_heatmap.png",
    )
    plot_district_heatmap(
        pairwise,
        output_dir / "district_heatmap.png",
    )
    plot_district_bar(
        district_metrics,
        output_dir / "district_bar.png",
    )
    plot_district_boxplot(
        station_metrics,
        output_dir / "district_boxplot.png",
    )
    plot_complexity_distribution(
        station_metrics,
        output_dir / "complexity_distribution.png",
    )
    plot_accessibility_scatter(
        station_metrics,
        output_dir / "accessibility_scatter.png",
    )

    logger.info("All charts saved to %s", output_dir)


def main() -> None:
    """CLI entry point: ``python -m src.viz``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    run()


if __name__ == "__main__":
    main()
