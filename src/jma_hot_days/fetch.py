"""Polite HTTP client for JMA pages with rate-limit, retry, and on-disk caching."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from threading import Lock

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

USER_AGENT = (
    "jma-hot-days/0.1 (research script; contact: gerogero7429@gmail.com) "
    "httpx/" + httpx.__version__
)
DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)
MIN_INTERVAL_SEC = 1.0  # politeness floor between requests

_last_request_at: float = 0.0
_rate_lock = Lock()


def _wait_for_rate_limit() -> None:
    global _last_request_at
    with _rate_lock:
        now = time.monotonic()
        delta = now - _last_request_at
        if delta < MIN_INTERVAL_SEC:
            time.sleep(MIN_INTERVAL_SEC - delta)
        _last_request_at = time.monotonic()


@retry(
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1.5, min=1.5, max=20.0),
    reraise=True,
)
def _request(client: httpx.Client, url: str) -> bytes:
    _wait_for_rate_limit()
    resp = client.get(url)
    if resp.status_code >= 500:
        raise httpx.HTTPStatusError(
            f"server {resp.status_code}", request=resp.request, response=resp
        )
    resp.raise_for_status()
    return resp.content


def make_client() -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": USER_AGENT, "Accept-Language": "ja,en;q=0.7"},
        timeout=DEFAULT_TIMEOUT,
        follow_redirects=True,
        http2=False,
    )


def fetch_cached(
    url: str,
    cache_path: Path,
    client: httpx.Client,
    force: bool = False,
) -> bytes:
    """Fetch ``url`` with on-disk caching to ``cache_path``.

    Returns raw bytes (the page is mostly Shift_JIS; decode at parse time).
    """
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.exists() and not force:
        return cache_path.read_bytes()
    logger.debug("GET %s", url)
    data = _request(client, url)
    cache_path.write_bytes(data)
    return data
