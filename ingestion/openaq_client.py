"""Thin REST client for the OpenAQ v3 API (https://docs.openaq.org).

Handles auth (X-API-Key header), pagination, and retry/backoff on 429
(rate limit) and 5xx responses.
"""

import logging
import time

import requests

logger = logging.getLogger(__name__)


class OpenAQClient:
    def __init__(self, api_key: str, base_url: str = "https://api.openaq.org/v3", timeout: float = 15.0):
        if not api_key:
            raise ValueError("OpenAQ API key is required (set OPENAQ_API_KEY in .env)")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": api_key})

    def _get(self, path: str, params: dict | None = None, max_retries: int = 5) -> dict:
        url = f"{self.base_url}{path}"
        backoff = 1.0
        for attempt in range(1, max_retries + 1):
            resp = self.session.get(url, params=params, timeout=self.timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", backoff))
                logger.warning(
                    "Rate limited on %s (attempt %d/%d) - sleeping %.1fs", path, attempt, max_retries, retry_after
                )
                time.sleep(retry_after)
                backoff = min(backoff * 2, 60)
                continue
            if 500 <= resp.status_code < 600:
                logger.warning(
                    "Server error %d on %s (attempt %d/%d) - retrying in %.1fs",
                    resp.status_code, path, attempt, max_retries, backoff,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
            resp.raise_for_status()
        raise RuntimeError(f"Exceeded {max_retries} retries for GET {path}")

    def list_locations(self, iso: str, limit: int, page_size: int = 100) -> list[dict]:
        """Paginate GET /v3/locations?iso=... up to `limit` results."""
        results: list[dict] = []
        page = 1
        while len(results) < limit:
            remaining = limit - len(results)
            data = self._get(
                "/locations",
                params={"iso": iso, "limit": min(page_size, remaining), "page": page},
            )
            batch = data.get("results", [])
            if not batch:
                break
            results.extend(batch)
            if len(batch) < min(page_size, remaining):
                break
            page += 1
        return results[:limit]

    def get_latest(self, location_id: int) -> list[dict]:
        """GET /v3/locations/{id}/latest -> latest value per sensor."""
        data = self._get(f"/locations/{location_id}/latest")
        return data.get("results", [])

    def list_parameters(self) -> list[dict]:
        data = self._get("/parameters")
        return data.get("results", [])
