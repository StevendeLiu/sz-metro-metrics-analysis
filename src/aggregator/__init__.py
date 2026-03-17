"""Aggregator module for SZ-Metro-Metrics.

Groups travel-time matrix results by administrative district and
computes accessibility metrics and complexity classifications.

Responsibilities:
    - Reverse-geocode station coordinates to Shenzhen districts (Nominatim).
    - Calculate mean/median travel times per station and district.
    - Classify complexity levels (Simple, Moderate, Complex, Remote)
      based on quantile thresholds.
    - Build pairwise district × district travel-time matrix.
    - Output results to CSV backends.

Usage::

    from src.aggregator.pipeline import run

    results = run()
    print(results["district_metrics"])
"""

from .classifier import assign_districts
from .geocode import geocode_districts
from .metrics import (
    classify_complexity,
    district_metrics,
    pairwise_district_matrix,
    station_accessibility,
)
from .pipeline import run

__all__ = [
    "run",
    "assign_districts",
    "geocode_districts",
    "station_accessibility",
    "district_metrics",
    "pairwise_district_matrix",
    "classify_complexity",
]
