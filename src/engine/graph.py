"""Metro graph construction from scraped station and segment data.

Graph model
-----------
* **Node** : unique ``station_id`` (one node per physical station-line instance).
  When two rows in the CSV share the same ``station_id``, they map to a single
  node (Amap already merged them).
* **Segment edge** : directed, ``weight = duration_minutes`` (from scraper CSV —
  either precise Amap routing-API values or coordinate-based estimates).
* **Transfer edge** : bidirectional penalty between every pair of *distinct*
  ``station_id`` values that share the same station *name* (same physical stop,
  different line instances).

Extensibility
-------------
``build_graph`` accepts an arbitrary list of extra weighted edges so that Bus or
HSR segments can be added in future without changing the API.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field

import networkx as nx

from .exceptions import GraphDisconnectedError

logger = logging.getLogger(__name__)

# Default transfer walk/platform-change penalty (minutes).
DEFAULT_TRANSFER_PENALTY: float = 4.0


@dataclass
class ExtraEdge:
    """An arbitrary weighted directed edge for graph extensibility.

    Attributes:
        from_id: Source node identifier.
        to_id:   Destination node identifier.
        weight:  Edge weight in minutes.
        attrs:   Additional metadata stored on the edge.
    """

    from_id: str
    to_id: str
    weight: float
    attrs: dict = field(default_factory=dict)


def build_graph(
    stations: list[dict],
    segments: list[dict],
    transfer_penalty: float = DEFAULT_TRANSFER_PENALTY,
    extra_edges: list[ExtraEdge] | None = None,
    strict: bool = False,
) -> nx.DiGraph:
    """Construct a weighted directed metro graph.

    Args:
        stations: List of station dicts with keys ``station_id``, ``name``,
                  ``line_id``, ``line_name``, ``longitude``, ``latitude``,
                  ``is_transfer``, ``transfer_line_ids``.
        segments: List of segment dicts with keys ``from_station_id``,
                  ``to_station_id``, ``line_id``, ``duration_minutes``.
        transfer_penalty: Minutes added for changing lines at a transfer node.
        extra_edges: Optional additional weighted edges (e.g. bus, HSR).
        strict: If ``True``, raise :class:`GraphDisconnectedError` when the
                graph is not weakly connected.  If ``False`` (default), log a
                warning instead — useful when standalone lines such as the
                Pingshan Yunba are present in the data.

    Returns:
        Directed graph where ``G[u][v]['weight']`` is travel time in minutes.

    Raises:
        GraphDisconnectedError: Only when *strict=True* and the graph is not
                                weakly connected.
    """
    G: nx.DiGraph = nx.DiGraph()

    # ── 1. Nodes ──────────────────────────────────────────────────────────────
    # Deduplicate by station_id (multiple CSV rows may share the same id).
    seen_ids: set[str] = set()
    for s in stations:
        sid = s["station_id"]
        if sid in seen_ids:
            continue
        seen_ids.add(sid)
        G.add_node(
            sid,
            name=s["name"],
            line_id=s["line_id"],
            line_name=s["line_name"],
            longitude=float(s["longitude"]) if s.get("longitude") else None,
            latitude=float(s["latitude"]) if s.get("latitude") else None,
            is_transfer=str(s.get("is_transfer", "False")).lower() == "true",
        )

    logger.info("Graph: %d unique station nodes.", G.number_of_nodes())

    # ── 2. Segment edges ──────────────────────────────────────────────────────
    for seg in segments:
        G.add_edge(
            seg["from_station_id"],
            seg["to_station_id"],
            weight=float(seg["duration_minutes"]),
            line_id=seg["line_id"],
            edge_type="segment",
        )

    logger.info("Graph: %d segment edges added.", G.number_of_edges())

    # ── 3. Transfer edges ─────────────────────────────────────────────────────
    # Group *distinct* station_ids by station name.
    name_to_ids: dict[str, set[str]] = defaultdict(set)
    for s in stations:
        name_to_ids[s["name"]].add(s["station_id"])

    transfer_edge_count = 0
    for name, id_set in name_to_ids.items():
        if len(id_set) < 2:
            continue
        ids = list(id_set)
        for i, a in enumerate(ids):
            for b in ids[i + 1 :]:
                G.add_edge(
                    a, b,
                    weight=transfer_penalty,
                    edge_type="transfer",
                    transfer_name=name,
                )
                G.add_edge(
                    b, a,
                    weight=transfer_penalty,
                    edge_type="transfer",
                    transfer_name=name,
                )
                transfer_edge_count += 2

    logger.info("Graph: %d transfer edges added (penalty=%.1f min).",
                transfer_edge_count, transfer_penalty)

    # ── 4. Extra edges (Bus, HSR, etc.) ───────────────────────────────────────
    for e in extra_edges or []:
        attrs = {k: v for k, v in e.attrs.items() if k != "edge_type"}
        G.add_edge(e.from_id, e.to_id, weight=e.weight,
                   edge_type=e.attrs.get("edge_type", "extra"), **attrs)

    # ── 5. Connectivity check ─────────────────────────────────────────────────
    if not nx.is_weakly_connected(G):
        components = list(nx.weakly_connected_components(G))
        n_comp = len(components)
        details = "; ".join(
            f"component {i + 1}: {len(c)} nodes "
            f"(e.g. {G.nodes[next(iter(c))]['name']})"
            for i, c in enumerate(components)
        )
        msg = (
            f"Metro graph has {n_comp} weakly connected components. "
            f"{details}. "
            "Cross-component travel times will be NaN in the matrix."
        )
        if strict:
            raise GraphDisconnectedError(msg)
        logger.warning(msg)

    logger.info(
        "Graph ready — %d nodes, %d edges (weakly connected).",
        G.number_of_nodes(), G.number_of_edges(),
    )
    return G
