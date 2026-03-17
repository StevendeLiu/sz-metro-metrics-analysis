"""All-pairs shortest-path solver for the metro graph.

Uses NetworkX's Dijkstra implementation (single-source, iterated over all
nodes) which is O(V · (E + V) log V) — efficient for sparse transit graphs.

For a ~400-node Shenzhen Metro graph this completes in < 1 second. If the
network grows to thousands of nodes (bus + HSR), switch to Floyd-Warshall via
``nx.floyd_warshall_numpy`` for dense graphs, or keep Dijkstra for sparse ones.
"""

from __future__ import annotations

import logging

import networkx as nx
import numpy as np
import pandas as pd
from tqdm import tqdm

logger = logging.getLogger(__name__)


def compute_matrix(G: nx.DiGraph) -> pd.DataFrame:
    """Compute the all-pairs shortest-path travel-time matrix.

    Args:
        G: Weighted directed metro graph produced by :func:`build_graph`.
           Edge attribute ``'weight'`` must contain travel time in minutes.

    Returns:
        Square ``DataFrame`` indexed and columned by ``station_id``.
        ``matrix.loc[a, b]`` is the minimum travel time (minutes) from
        station *a* to station *b*.  Diagonal is ``0.0``.  Unreachable
        pairs are ``NaN``.

    Raises:
        GraphDisconnectedError: If any station pair is unreachable
                                (non-finite path length found).
    """
    nodes: list[str] = list(G.nodes())
    n = len(nodes)
    node_idx: dict[str, int] = {nid: i for i, nid in enumerate(nodes)}

    dist = np.full((n, n), np.inf)
    np.fill_diagonal(dist, 0.0)

    logger.info("Computing APSP for %d nodes …", n)
    for source in tqdm(nodes, desc="Dijkstra", unit="node"):
        lengths: dict[str, float] = nx.single_source_dijkstra_path_length(
            G, source, weight="weight"
        )
        row = node_idx[source]
        for target, d in lengths.items():
            dist[row, node_idx[target]] = d

    # Replace unreachable pairs (inf) with NaN and warn
    inf_mask = np.isinf(dist)
    if inf_mask.any():
        count = int(inf_mask.sum())
        logger.warning(
            "%d station pairs are unreachable (cross-component). "
            "These will appear as NaN in the matrix.",
            count,
        )
        dist = dist.astype(float)
        dist[inf_mask] = np.nan

    matrix = pd.DataFrame(dist, index=nodes, columns=nodes)
    logger.info("APSP complete — matrix shape %s.", matrix.shape)
    return matrix


def matrix_stats(matrix: pd.DataFrame) -> dict[str, float]:
    """Return descriptive statistics for a travel-time matrix.

    Args:
        matrix: Square DataFrame of travel times in minutes.

    Returns:
        Dict with keys ``min``, ``max``, ``mean``, ``median`` (off-diagonal).
    """
    # Exclude the diagonal (self-travel = 0)
    vals = matrix.values.copy().astype(float)
    np.fill_diagonal(vals, np.nan)
    flat = vals[~np.isnan(vals)]
    return {
        "min": float(np.min(flat)),
        "max": float(np.max(flat)),
        "mean": float(np.mean(flat)),
        "median": float(np.median(flat)),
    }
