"""Custom exceptions for the engine module."""


class GraphDisconnectedError(Exception):
    """Raised when the metro graph contains unreachable station pairs."""
