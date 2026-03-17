"""Scraper module for SZ-Metro-Metrics.

Handles asynchronous data collection from Shenzhen Metro APIs and
web sources. All network requests implement exponential backoff retry.

Responsibilities:
    - Fetch station data (name, line, transfer info).
    - Extract segment travel durations.
    - Validate and serialize raw data to CSV/JSON.

Raises:
    ScraperError: On unrecoverable network or parsing failures.

Usage::

    import asyncio
    from src.scraper.pipeline import run

    data = asyncio.run(run())
"""

from .exceptions import ScraperError
from .models import RawMetroData, Segment, Station
from .pipeline import run

__all__ = ["ScraperError", "Station", "Segment", "RawMetroData", "run"]
