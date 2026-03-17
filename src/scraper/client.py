"""Async HTTP client with exponential backoff retry."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from .exceptions import ScraperError

logger = logging.getLogger(__name__)

_DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Mobile Safari/537.36"
    ),
    "Accept": "application/json",
}

_MAX_RETRIES = 4
_BASE_DELAY = 1.0  # seconds


async def _request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs: Any,
) -> httpx.Response:
    """Send *method* request to *url*, retrying on transient errors.

    Args:
        client: Shared ``httpx.AsyncClient`` instance.
        method: HTTP method string (e.g. ``"GET"``).
        url: Full URL to request.
        **kwargs: Extra keyword arguments forwarded to ``client.request``.

    Returns:
        Successful ``httpx.Response``.

    Raises:
        ScraperError: After exhausting all retry attempts.
    """
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            response = await client.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except (httpx.HTTPStatusError, httpx.TransportError) as exc:
            last_exc = exc
            delay = _BASE_DELAY * (2**attempt)
            logger.warning(
                "Request failed (attempt %d/%d): %s — retrying in %.1fs",
                attempt + 1,
                _MAX_RETRIES,
                exc,
                delay,
            )
            await asyncio.sleep(delay)

    raise ScraperError(
        f"All {_MAX_RETRIES} attempts failed for {url}"
    ) from last_exc


def build_client(
    base_url: str = "",
    timeout: float = 20.0,
    extra_headers: dict[str, str] | None = None,
) -> httpx.AsyncClient:
    """Create a shared async HTTP client.

    Args:
        base_url: Optional base URL prefix applied to all requests.
        timeout: Per-request timeout in seconds.
        extra_headers: Headers merged on top of the default set.

    Returns:
        Configured ``httpx.AsyncClient``.
    """
    headers = {**_DEFAULT_HEADERS, **(extra_headers or {})}
    return httpx.AsyncClient(
        base_url=base_url,
        headers=headers,
        timeout=timeout,
        follow_redirects=True,
    )


async def get_json(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any] | None = None,
) -> Any:
    """Perform a GET request and return the parsed JSON body.

    Args:
        client: Shared ``httpx.AsyncClient`` instance.
        url: Absolute or relative URL.
        params: Optional query parameters.

    Returns:
        Decoded JSON payload (``dict`` or ``list``).

    Raises:
        ScraperError: On network failure or non-2xx response.
    """
    response = await _request_with_retry(client, "GET", url, params=params)
    return response.json()
