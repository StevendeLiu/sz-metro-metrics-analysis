"""Travel-time heatmap visualizations."""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

matplotlib.rcParams["font.family"] = ["Noto Sans CJK JP", "Droid Sans Fallback",
                                       "WenQuanYi Micro Hei", "DejaVu Sans"]
matplotlib.rcParams["axes.unicode_minus"] = False

logger = logging.getLogger(__name__)

# District order for consistent axis layout (center → periphery)
_DISTRICT_ORDER = ["福田区", "罗湖区", "南山区", "龙华区", "龙岗区",
                   "宝安区", "光明区", "盐田区", "坪山区"]


def _sorted_station_index(
    station_metrics: pd.DataFrame,
    matrix: pd.DataFrame,
) -> list[str]:
    """Return station IDs sorted by district order then mean accessibility.

    Args:
        station_metrics: DataFrame with ``station_id``, ``district``,
                         ``mean_to_all`` columns.
        matrix: Square travel-time DataFrame (used to filter to valid IDs).

    Returns:
        Ordered list of station IDs present in the matrix.
    """
    valid = set(matrix.index.astype(str))
    sm = station_metrics[station_metrics["station_id"].isin(valid)].copy()

    # Map districts to order index; unknown districts go last
    order_map = {d: i for i, d in enumerate(_DISTRICT_ORDER)}
    sm["_order"] = sm["district"].map(order_map).fillna(len(_DISTRICT_ORDER))
    sm = sm.sort_values(["_order", "mean_to_all"])
    return sm["station_id"].tolist()


def plot_station_heatmap(
    matrix: pd.DataFrame,
    station_metrics: pd.DataFrame,
    output_path: Path,
    figsize: tuple[int, int] = (14, 12),
    cmap: str = "YlOrRd",
) -> None:
    """Render a station × station travel-time heatmap grouped by district.

    Stations are sorted by district (center → periphery) then by mean
    accessibility within each district.  District boundaries are marked with
    white grid lines.

    Args:
        matrix: Square travel-time DataFrame (station_id index/columns).
        station_metrics: Output of aggregator with district assignments.
        output_path: Where to save the PNG file.
        figsize: Figure size in inches.
        cmap: Matplotlib colormap name.
    """
    order = _sorted_station_index(station_metrics, matrix)
    mat = matrix.loc[order, order].astype(float)
    np.fill_diagonal(mat.values, 0)

    # District boundary tick positions
    district_col = station_metrics.set_index("station_id")["district"]
    districts_in_order = [district_col.get(sid, "未知") for sid in order]
    boundary_positions: list[int] = []
    for i in range(1, len(districts_in_order)):
        if districts_in_order[i] != districts_in_order[i - 1]:
            boundary_positions.append(i)

    fig, ax = plt.subplots(figsize=figsize)
    sns.heatmap(
        mat,
        ax=ax,
        cmap=cmap,
        xticklabels=False,
        yticklabels=False,
        cbar_kws={"label": "行程时间 (分钟)", "shrink": 0.8},
        vmin=0,
        vmax=mat.values[mat.values > 0].max() if (mat.values > 0).any() else 1,
    )

    # Draw district boundary lines
    for pos in boundary_positions:
        ax.axhline(pos, color="white", linewidth=1.2)
        ax.axvline(pos, color="white", linewidth=1.2)

    # District labels on axes
    prev = 0
    seen_districts: list[str] = []
    label_positions: list[float] = []
    for pos in boundary_positions + [len(order)]:
        mid = (prev + pos) / 2
        district_name = districts_in_order[prev]
        if district_name not in seen_districts:
            seen_districts.append(district_name)
            label_positions.append(mid)
        prev = pos

    ax.set_yticks(label_positions)
    ax.set_yticklabels(seen_districts, fontsize=9, rotation=0)
    ax.set_xticks(label_positions)
    ax.set_xticklabels(seen_districts, fontsize=9, rotation=45, ha="right")

    ax.set_title("深圳地铁站间行程时间矩阵\n(按行政区排序，单位：分钟)", fontsize=13, pad=12)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved station heatmap → %s", output_path)


def plot_district_heatmap(
    pairwise_matrix: pd.DataFrame,
    output_path: Path,
    figsize: tuple[int, int] = (9, 7),
    cmap: str = "YlOrRd",
) -> None:
    """Render a district × district mean travel-time heatmap.

    Args:
        pairwise_matrix: Square DataFrame indexed/columned by district name.
        output_path: Where to save the PNG file.
        figsize: Figure size in inches.
        cmap: Matplotlib colormap name.
    """
    # Reorder rows/columns by _DISTRICT_ORDER
    present = [d for d in _DISTRICT_ORDER if d in pairwise_matrix.index]
    extra = [d for d in pairwise_matrix.index if d not in present]
    ordered = present + extra
    mat = pairwise_matrix.loc[ordered, ordered].astype(float)

    # Mask diagonal for color scaling (within-district commutes are short)
    mask_diag = np.eye(len(mat), dtype=bool)

    fig, ax = plt.subplots(figsize=figsize)
    sns.heatmap(
        mat,
        ax=ax,
        cmap=cmap,
        annot=True,
        fmt=".0f",
        annot_kws={"size": 9},
        linewidths=0.5,
        linecolor="white",
        cbar_kws={"label": "均值行程时间 (分钟)", "shrink": 0.8},
        mask=mask_diag,
        vmin=mat.values[~mask_diag].min(),
        vmax=mat.values[~mask_diag].max(),
    )
    # Overlay diagonal values manually in a neutral color
    for i in range(len(mat)):
        ax.text(
            i + 0.5, i + 0.5,
            f"{mat.iloc[i, i]:.0f}",
            ha="center", va="center", fontsize=9, color="gray",
        )

    ax.set_title("深圳各行政区间地铁均值行程时间 (分钟)", fontsize=12, pad=10)
    ax.set_xlabel("")
    ax.set_ylabel("")
    plt.xticks(rotation=30, ha="right", fontsize=10)
    plt.yticks(rotation=0, fontsize=10)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved district heatmap → %s", output_path)
