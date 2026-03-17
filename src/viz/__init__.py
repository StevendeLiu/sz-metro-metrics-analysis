"""Viz module for SZ-Metro-Metrics.

Provides visualization utilities for accessibility metrics.

Responsibilities:
    - Render travel-time heatmaps (station × station matrix, district × district).
    - Plot district-level accessibility bar/box charts.
    - Complexity tier distribution charts.
    - Mean vs median accessibility scatter plot.

Usage::

    from src.viz.pipeline import run
    run()
"""

from .heatmap import plot_district_heatmap, plot_station_heatmap
from .district_chart import (
    plot_district_bar,
    plot_district_boxplot,
    plot_complexity_distribution,
    plot_accessibility_scatter,
)
from .pipeline import run

__all__ = [
    "run",
    "plot_station_heatmap",
    "plot_district_heatmap",
    "plot_district_bar",
    "plot_district_boxplot",
    "plot_complexity_distribution",
    "plot_accessibility_scatter",
]
