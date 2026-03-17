"""Accessibility metrics and complexity classification per district."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Complexity tier labels (ordered from most to least accessible)
COMPLEXITY_LABELS = ["Simple", "Moderate", "Complex", "Remote"]


def station_accessibility(
    matrix: pd.DataFrame,
    station_district: dict[str, str],
) -> pd.DataFrame:
    """Compute per-station mean travel time to all reachable stations.

    Args:
        matrix: Square travel-time DataFrame (station_id index/columns).
        station_district: Mapping ``station_id → district_name``.

    Returns:
        DataFrame with columns:
        ``station_id``, ``district``, ``mean_to_all``, ``median_to_all``,
        ``reachable_count``.
    """
    matrix = matrix.copy().astype(float)
    np.fill_diagonal(matrix.values, np.nan)

    records = []
    for sid in matrix.index:
        row = matrix.loc[sid].dropna()
        if row.empty:
            continue
        records.append({
            "station_id": str(sid),
            "district": station_district.get(str(sid), "未知"),
            "mean_to_all": round(float(row.mean()), 4),
            "median_to_all": round(float(row.median()), 4),
            "reachable_count": int(len(row)),
        })

    df = pd.DataFrame(records)
    logger.info("Station accessibility computed for %d stations.", len(df))
    return df


def district_metrics(station_acc: pd.DataFrame) -> pd.DataFrame:
    """Aggregate station accessibility into district-level summary metrics.

    Args:
        station_acc: Output of :func:`station_accessibility`.

    Returns:
        DataFrame indexed by ``district`` with columns:
        ``station_count``, ``mean_accessibility``, ``median_accessibility``,
        ``std_accessibility``, ``min_accessibility``, ``max_accessibility``.
    """
    grp = station_acc.groupby("district")["mean_to_all"]
    df = pd.DataFrame({
        "station_count": station_acc.groupby("district")["station_id"].count(),
        "mean_accessibility":   grp.mean().round(4),
        "median_accessibility": grp.median().round(4),
        "std_accessibility":    grp.std().round(4),
        "min_accessibility":    grp.min().round(4),
        "max_accessibility":    grp.max().round(4),
    }).reset_index().rename(columns={"index": "district"})

    logger.info("District metrics computed for %d districts.", len(df))
    return df


def pairwise_district_matrix(
    matrix: pd.DataFrame,
    station_district: dict[str, str],
) -> pd.DataFrame:
    """Build a district × district mean-travel-time matrix.

    Each cell ``(D_i, D_j)`` is the mean of all station-to-station travel
    times from any station in D_i to any station in D_j.

    Args:
        matrix: Square travel-time DataFrame.
        station_district: Mapping ``station_id → district_name``.

    Returns:
        Square DataFrame indexed/columned by district name.
    """
    matrix = matrix.copy().astype(float)
    np.fill_diagonal(matrix.values, np.nan)

    # Remap index/columns to district names
    idx_to_district = {str(sid): district for sid, district in station_district.items()}

    districts = sorted(set(idx_to_district.values()))
    result = pd.DataFrame(np.nan, index=districts, columns=districts)

    for d_from in districts:
        src_ids = [c for c in matrix.index if idx_to_district.get(str(c)) == d_from]
        for d_to in districts:
            dst_ids = [c for c in matrix.columns if idx_to_district.get(str(c)) == d_to]
            if not src_ids or not dst_ids:
                continue
            block = matrix.loc[src_ids, dst_ids].values.flatten()
            valid = block[~np.isnan(block)]
            if len(valid) > 0:
                result.loc[d_from, d_to] = round(float(np.mean(valid)), 4)

    logger.info(
        "Pairwise district matrix computed (%d × %d).", len(districts), len(districts)
    )
    return result


def classify_complexity(
    station_acc: pd.DataFrame,
    quantiles: tuple[float, float, float] = (0.25, 0.50, 0.75),
) -> pd.DataFrame:
    """Add a complexity tier to each station based on quantile thresholds.

    Tiers (ascending travel time = decreasing accessibility):
        - **Simple**   : ≤ Q1  (most accessible)
        - **Moderate** : Q1 < x ≤ Q2
        - **Complex**  : Q2 < x ≤ Q3
        - **Remote**   : > Q3  (least accessible)

    Args:
        station_acc: Output of :func:`station_accessibility` (must contain
                     column ``mean_to_all``).
        quantiles: Three quantile cut-points (default: 25/50/75th percentiles).

    Returns:
        Copy of *station_acc* with an added ``complexity`` column.
    """
    q1, q2, q3 = station_acc["mean_to_all"].quantile(list(quantiles))
    logger.info(
        "Complexity quantile thresholds — Q1: %.2f, Q2: %.2f, Q3: %.2f min",
        q1, q2, q3,
    )

    def _tier(val: float) -> str:
        if val <= q1:
            return "Simple"
        if val <= q2:
            return "Moderate"
        if val <= q3:
            return "Complex"
        return "Remote"

    result = station_acc.copy()
    result["complexity"] = result["mean_to_all"].apply(_tier)
    counts = result["complexity"].value_counts()
    logger.info("Complexity distribution: %s", counts.to_dict())
    return result
