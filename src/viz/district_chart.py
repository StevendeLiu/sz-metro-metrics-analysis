"""District-level accessibility bar, box, and complexity charts."""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import seaborn as sns

matplotlib.rcParams["font.family"] = ["Noto Sans CJK JP", "Droid Sans Fallback",
                                       "WenQuanYi Micro Hei", "DejaVu Sans"]
matplotlib.rcParams["axes.unicode_minus"] = False

logger = logging.getLogger(__name__)

_DISTRICT_ORDER = ["福田区", "罗湖区", "南山区", "龙华区", "龙岗区",
                   "宝安区", "光明区", "盐田区", "坪山区"]

_COMPLEXITY_COLORS = {
    "Simple":   "#2ecc71",
    "Moderate": "#f1c40f",
    "Complex":  "#e67e22",
    "Remote":   "#e74c3c",
}


def _district_order_key(district: str) -> int:
    try:
        return _DISTRICT_ORDER.index(district)
    except ValueError:
        return len(_DISTRICT_ORDER)


def plot_district_bar(
    district_metrics: pd.DataFrame,
    output_path: Path,
    figsize: tuple[int, int] = (11, 6),
) -> None:
    """Horizontal bar chart of mean and median district accessibility.

    Bars are sorted from most to least accessible (ascending mean travel time).
    Error bars show ±1 standard deviation.

    Args:
        district_metrics: Output of :func:`aggregator.metrics.district_metrics`.
        output_path: Destination PNG path.
        figsize: Figure size in inches.
    """
    df = district_metrics.copy()
    df["_order"] = df["district"].apply(_district_order_key)
    df = df.sort_values("mean_accessibility")

    fig, ax = plt.subplots(figsize=figsize)
    y = np.arange(len(df))

    ax.barh(
        y, df["mean_accessibility"],
        xerr=df["std_accessibility"],
        color="#3498db", alpha=0.85,
        error_kw={"elinewidth": 1.2, "capsize": 4, "ecolor": "#2c3e50"},
        label="均值",
    )
    ax.scatter(
        df["median_accessibility"], y,
        color="#e74c3c", zorder=5, s=60, label="中位数",
    )

    ax.set_yticks(y)
    ax.set_yticklabels(df["district"], fontsize=11)
    ax.set_xlabel("行程时间 (分钟)", fontsize=11)
    ax.set_title("深圳各行政区地铁可达性\n(均值 ± 标准差，红点为中位数)", fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(axis="x", linestyle="--", alpha=0.5)

    # Annotate mean values
    for i, (_, row) in enumerate(df.iterrows()):
        ax.text(
            row["mean_accessibility"] + row["std_accessibility"] + 0.5,
            i, f"{row['mean_accessibility']:.1f}",
            va="center", fontsize=9, color="#2c3e50",
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved district bar chart → %s", output_path)


def plot_district_boxplot(
    station_metrics: pd.DataFrame,
    output_path: Path,
    figsize: tuple[int, int] = (12, 6),
) -> None:
    """Box-plot distribution of per-station accessibility by district.

    Each box shows the full distribution of mean travel times for stations
    within a district, colored by dominant complexity tier.

    Args:
        station_metrics: Output of :func:`aggregator.metrics.classify_complexity`.
        output_path: Destination PNG path.
        figsize: Figure size in inches.
    """
    df = station_metrics.copy()
    df["_order"] = df["district"].apply(_district_order_key)

    # Sort districts by median accessibility
    district_medians = df.groupby("district")["mean_to_all"].median().sort_values()
    ordered_districts = district_medians.index.tolist()

    fig, ax = plt.subplots(figsize=figsize)

    parts = ax.boxplot(
        [df[df["district"] == d]["mean_to_all"].values for d in ordered_districts],
        vert=True,
        patch_artist=True,
        labels=ordered_districts,
        medianprops={"color": "#e74c3c", "linewidth": 2},
        whiskerprops={"linewidth": 1.2},
        capprops={"linewidth": 1.5},
        flierprops={"marker": "o", "markersize": 4, "alpha": 0.5},
    )

    # Color boxes by dominant complexity tier
    for patch, district in zip(parts["boxes"], ordered_districts):
        dominant = (
            df[df["district"] == district]["complexity"]
            .value_counts().idxmax()
        )
        patch.set_facecolor(_COMPLEXITY_COLORS.get(dominant, "#bdc3c7"))
        patch.set_alpha(0.8)

    ax.set_ylabel("平均行程时间 (分钟)", fontsize=11)
    ax.set_title("各行政区内站点可达性分布\n(箱体颜色对应主导复杂度等级)", fontsize=12)
    ax.tick_params(axis="x", labelrotation=20)
    ax.grid(axis="y", linestyle="--", alpha=0.5)

    # Legend
    legend_patches = [
        mpatches.Patch(color=c, label=t, alpha=0.8)
        for t, c in _COMPLEXITY_COLORS.items()
    ]
    ax.legend(handles=legend_patches, title="复杂度等级", fontsize=9,
              title_fontsize=9, loc="upper left")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved district boxplot → %s", output_path)


def plot_complexity_distribution(
    station_metrics: pd.DataFrame,
    output_path: Path,
    figsize: tuple[int, int] = (13, 5),
) -> None:
    """Stacked bar chart of complexity tier distribution per district.

    Args:
        station_metrics: DataFrame with ``district`` and ``complexity`` columns.
        output_path: Destination PNG path.
        figsize: Figure size in inches.
    """
    df = station_metrics.copy()
    df["_order"] = df["district"].apply(_district_order_key)
    df = df.sort_values("_order")

    # Pivot: district × complexity → count
    pivot = (
        df.groupby(["district", "complexity"])
        .size()
        .unstack(fill_value=0)
    )
    # Ensure all tiers present and in order
    for tier in ["Simple", "Moderate", "Complex", "Remote"]:
        if tier not in pivot.columns:
            pivot[tier] = 0
    pivot = pivot[["Simple", "Moderate", "Complex", "Remote"]]

    # Reorder rows
    present = [d for d in _DISTRICT_ORDER if d in pivot.index]
    extra = [d for d in pivot.index if d not in present]
    pivot = pivot.loc[present + extra]

    # Normalize to percentage
    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0) * 100

    fig, axes = plt.subplots(1, 2, figsize=figsize)

    # Left: absolute count
    pivot.plot(
        kind="bar", ax=axes[0], stacked=True,
        color=[_COMPLEXITY_COLORS[t] for t in pivot.columns],
        edgecolor="white", linewidth=0.5,
    )
    axes[0].set_title("各区复杂度等级站点数量", fontsize=11)
    axes[0].set_ylabel("站点数量", fontsize=10)
    axes[0].tick_params(axis="x", labelrotation=30)
    axes[0].legend(title="复杂度", fontsize=9, title_fontsize=9)
    axes[0].grid(axis="y", linestyle="--", alpha=0.4)

    # Right: percentage
    pivot_pct.plot(
        kind="bar", ax=axes[1], stacked=True,
        color=[_COMPLEXITY_COLORS[t] for t in pivot_pct.columns],
        edgecolor="white", linewidth=0.5,
    )
    axes[1].set_title("各区复杂度等级比例 (%)", fontsize=11)
    axes[1].set_ylabel("比例 (%)", fontsize=10)
    axes[1].tick_params(axis="x", labelrotation=30)
    axes[1].legend(title="复杂度", fontsize=9, title_fontsize=9)
    axes[1].grid(axis="y", linestyle="--", alpha=0.4)
    axes[1].set_ylim(0, 105)

    fig.suptitle("深圳地铁各行政区站点复杂度分布", fontsize=13, y=1.01)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved complexity distribution chart → %s", output_path)


def plot_accessibility_scatter(
    station_metrics: pd.DataFrame,
    output_path: Path,
    figsize: tuple[int, int] = (11, 7),
) -> None:
    """Scatter plot: mean vs median accessibility per station, colored by complexity.

    Args:
        station_metrics: DataFrame with ``mean_to_all``, ``median_to_all``,
                         ``complexity``, ``district`` columns.
        output_path: Destination PNG path.
        figsize: Figure size in inches.
    """
    df = station_metrics.copy()

    fig, ax = plt.subplots(figsize=figsize)
    for tier in ["Simple", "Moderate", "Complex", "Remote"]:
        sub = df[df["complexity"] == tier]
        ax.scatter(
            sub["mean_to_all"], sub["median_to_all"],
            c=_COMPLEXITY_COLORS[tier], label=tier,
            alpha=0.75, s=40, edgecolors="none",
        )

    # y = x reference line
    lim_min = min(df["mean_to_all"].min(), df["median_to_all"].min()) - 2
    lim_max = max(df["mean_to_all"].max(), df["median_to_all"].max()) + 2
    ax.plot([lim_min, lim_max], [lim_min, lim_max],
            "k--", linewidth=0.8, alpha=0.5, label="均值 = 中位数")

    ax.set_xlabel("平均行程时间 (分钟)", fontsize=11)
    ax.set_ylabel("中位行程时间 (分钟)", fontsize=11)
    ax.set_title("各站点均值与中位行程时间对比\n(颜色代表复杂度等级)", fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(linestyle="--", alpha=0.4)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved accessibility scatter → %s", output_path)
