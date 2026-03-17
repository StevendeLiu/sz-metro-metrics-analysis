"""Pydantic data models for the scraper module."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Station(BaseModel):
    """A single metro station."""

    station_id: str
    name: str
    line_id: str
    line_name: str
    district: str | None = None
    longitude: float | None = None
    latitude: float | None = None
    is_transfer: bool = False
    transfer_line_ids: list[str] = Field(default_factory=list)


class Segment(BaseModel):
    """A directed travel segment between two adjacent stations."""

    from_station_id: str
    to_station_id: str
    line_id: str
    duration_minutes: float


class RawMetroData(BaseModel):
    """Container for a complete scrape run."""

    stations: list[Station]
    segments: list[Segment]
