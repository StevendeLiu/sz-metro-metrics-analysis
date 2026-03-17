"""SZ-Metro-Metrics: Shenzhen Metro Accessibility Analyzer.

A data engineering and analysis pipeline to calculate, aggregate,
and visualize travel time complexity between Shenzhen Metro stations
and administrative districts.

Modules:
    scraper: Async data collection from Metro APIs/web sources.
    engine: Graph construction and shortest-path computation.
    aggregator: District-level metric aggregation and categorization.
    viz: Visualization utilities (heatmaps, charts, map overlays).
"""
