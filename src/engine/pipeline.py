"""Engine pipeline: load raw data → build graph → solve APSP → persist.

Data precision note
-------------------
Segment travel times in ``data/raw/segments.csv`` come from the scraper:

* **With** ``AMAP_API_KEY`` set at scrape time → precise times from Amap's
  routing API (recommended for production runs).
* **Without** ``AMAP_API_KEY`` → coordinate-based haversine estimates
  (sufficient for relative accessibility comparisons).

The engine is agnostic to the source of precision; it consumes whatever
``segments.csv`` was produced.  To upgrade precision, re-run the scraper with
a valid key, then re-run the engine pipeline.
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path

import networkx as nx
import pandas as pd

from .graph import DEFAULT_TRANSFER_PENALTY, ExtraEdge, build_graph
from .solver import compute_matrix, matrix_stats

logger = logging.getLogger(__name__)

_DEFAULT_RAW_DIR = Path("data/raw")
_DEFAULT_OUT_DIR = Path("data/processed")
_MANUAL_TRANSFERS_FILE = "manual_transfers.csv"


# ── I/O helpers ───────────────────────────────────────────────────────────────

def _load_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _load_manual_transfers(raw_dir: Path) -> list[ExtraEdge]:
    """Load manually defined cross-network transfer edges from CSV.

    Expects a file ``manual_transfers.csv`` in *raw_dir* with columns:
    ``from_station_id``, ``to_station_id``, ``transfer_minutes``, ``notes``.
    Missing file is silently ignored (returns empty list).

    Args:
        raw_dir: Directory that may contain ``manual_transfers.csv``.

    Returns:
        Bidirectional list of :class:`ExtraEdge` objects.
    """
    path = raw_dir / _MANUAL_TRANSFERS_FILE
    if not path.exists():
        return []

    edges: list[ExtraEdge] = []
    for row in _load_csv(path):
        minutes = float(row["transfer_minutes"])
        notes = row.get("notes", "")
        edges.append(ExtraEdge(row["from_station_id"], row["to_station_id"],
                               minutes, {"edge_type": "manual_transfer", "notes": notes}))
        edges.append(ExtraEdge(row["to_station_id"], row["from_station_id"],
                               minutes, {"edge_type": "manual_transfer", "notes": notes}))

    logger.info("Loaded %d manual transfer edges from %s.", len(edges), path)
    return edges


def _save_matrix_csv(matrix: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    matrix.to_csv(path, float_format="%.4f")
    logger.info("Saved travel-time matrix → %s", path)


def _save_graph_json(G: nx.DiGraph, path: Path) -> None:
    """Persist graph topology as node-link JSON for downstream inspection."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = nx.node_link_data(G)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Saved graph topology → %s", path)


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run(
    raw_dir: Path = _DEFAULT_RAW_DIR,
    output_dir: Path = _DEFAULT_OUT_DIR,
    transfer_penalty: float = DEFAULT_TRANSFER_PENALTY,
    extra_edges: list[ExtraEdge] | None = None,
) -> tuple[nx.DiGraph, pd.DataFrame]:
    """Execute the graph-construction and APSP pipeline.

    Steps:
        1. Load ``stations.csv`` and ``segments.csv`` from *raw_dir*.
        2. Build weighted directed graph with transfer penalty edges.
        3. Run all-pairs Dijkstra to produce a travel-time matrix.
        4. Persist matrix CSV and graph JSON to *output_dir*.

    Args:
        raw_dir: Directory containing scraper output CSV files.
        output_dir: Destination directory for engine outputs.
        transfer_penalty: Minutes charged for switching lines at a transfer
                          station (default: :data:`DEFAULT_TRANSFER_PENALTY`).
        extra_edges: Optional extra weighted edges for Bus/HSR extension.

    Returns:
        Tuple of ``(graph, matrix)`` for use by downstream modules.

    Raises:
        FileNotFoundError: If required CSV files are missing.
        GraphDisconnectedError: If the graph contains unreachable pairs.
    """
    # 1. Load
    stations_path = raw_dir / "stations.csv"
    segments_path = raw_dir / "segments.csv"
    for p in (stations_path, segments_path):
        if not p.exists():
            raise FileNotFoundError(
                f"Required data file not found: {p}\n"
                "Run the scraper first: python -m src.scraper"
            )

    logger.info("Loading raw data from %s …", raw_dir)
    stations = _load_csv(stations_path)
    segments = _load_csv(segments_path)
    logger.info(
        "Loaded %d station rows, %d segment rows.",
        len(stations), len(segments),
    )

    # 2. Load manual transfers and merge with caller-supplied extra edges
    manual = _load_manual_transfers(raw_dir)
    all_extra = list(extra_edges or []) + manual

    # 3. Build graph
    logger.info(
        "Building graph (transfer_penalty=%.1f min, extra_edges=%d) …",
        transfer_penalty, len(all_extra),
    )
    G = build_graph(
        stations=stations,
        segments=segments,
        transfer_penalty=transfer_penalty,
        extra_edges=all_extra,
    )

    # 4. APSP
    matrix = compute_matrix(G)

    # 5. Stats
    stats = matrix_stats(matrix)
    logger.info(
        "Matrix stats — min: %.2f min, max: %.2f min, "
        "mean: %.2f min, median: %.2f min",
        stats["min"], stats["max"], stats["mean"], stats["median"],
    )

    # 6. Persist
    _save_matrix_csv(matrix, output_dir / "travel_time_matrix.csv")
    _save_graph_json(G, output_dir / "graph.json")

    return G, matrix


def main() -> None:
    """CLI entry point: ``python -m src.engine``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    run()


if __name__ == "__main__":
    main()
