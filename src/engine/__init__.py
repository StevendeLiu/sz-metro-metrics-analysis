"""Engine module for SZ-Metro-Metrics.

Constructs the metro graph and computes all-pairs shortest paths
using Dijkstra's algorithm.

Responsibilities:
    - Build a weighted directed graph from station/segment data.
    - Apply transfer-time penalty weights at interchange nodes.
    - Produce a full travel-time matrix (station × station).
    - Accept arbitrary weighted edges to support Bus/HSR extensions.

Raises:
    GraphDisconnectedError: If the graph contains unreachable node pairs.

Usage::

    from src.engine.pipeline import run

    graph, matrix = run()
    # matrix.loc["440300024063031", "440300024076020"] → travel time in minutes
"""

from .exceptions import GraphDisconnectedError
from .graph import DEFAULT_TRANSFER_PENALTY, ExtraEdge, build_graph
from .pipeline import run
from .solver import compute_matrix, matrix_stats

__all__ = [
    "GraphDisconnectedError",
    "ExtraEdge",
    "DEFAULT_TRANSFER_PENALTY",
    "build_graph",
    "compute_matrix",
    "matrix_stats",
    "run",
]
