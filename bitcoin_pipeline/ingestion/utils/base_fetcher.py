"""
Base Fetcher
------------
Abstract base class for all data source fetchers.
Provides: retry logic, rate limiting, structured logging, error handling.
"""

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class BaseFetcher(ABC):
    """
    All source fetchers inherit from this.
    Handles the boring-but-critical stuff: retries, timeouts, rate limits.
    """

    BASE_URL: str = ""
    DEFAULT_TIMEOUT: int = 30

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        """Session with automatic retry on transient errors."""
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=2,          # wait 2s, 4s, 8s between retries
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def get(self, endpoint: str, params: dict | None = None) -> dict:
        """
        Make a GET request. Raises on non-2xx.
        Logs request/response for debugging.
        """
        url = f"{self.BASE_URL}{endpoint}"
        logger.debug(f"GET {url} | params={params}")

        try:
            response = self.session.get(
                url,
                params=params,
                timeout=self.DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
            logger.debug(f"Response {response.status_code} | {len(response.content)} bytes")
            return response.json()

        except requests.exceptions.HTTPError as e:
            # 429 = rate limited — log clearly so Airflow alerts make sense
            if e.response.status_code == 429:
                logger.warning(f"Rate limited by {self.BASE_URL}. Consider increasing interval.")
            raise

    def rate_limit_sleep(self, seconds: float) -> None:
        """Explicit sleep between API calls — needed for free tier limits."""
        logger.debug(f"Rate limit sleep: {seconds}s")
        time.sleep(seconds)

    @abstractmethod
    def fetch(self, **kwargs) -> Any:
        """
        Each source implements this. Returns a pandas DataFrame.
        Airflow tasks will call .fetch() then pass result to S3Writer.
        """
        raise NotImplementedError