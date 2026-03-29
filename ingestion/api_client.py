"""
ingestion/api_client.py
HTTP client for the API-Football v3 REST API.

Features
--------
- Structured logging
- Per-minute rate limiting (token-bucket style)
- Retry with exponential back-off on transient failures (5xx / timeout)
"""

import logging
import time
from threading import Lock
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.config import BASE_URL, HEADERS, MAX_RETRIES, REQUESTS_PER_MINUTE

logger = logging.getLogger(__name__)


class _RateLimiter:
    """Simple token-bucket rate limiter (thread-safe)."""

    def __init__(self, calls_per_minute: int) -> None:
        self._interval = 60.0 / calls_per_minute  # seconds between calls
        self._lock = Lock()
        self._last_call: float = 0.0

    def wait(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last_call
            sleep_for = self._interval - elapsed
            if sleep_for > 0:
                logger.debug("Rate limiter sleeping %.2fs", sleep_for)
                time.sleep(sleep_for)
            self._last_call = time.monotonic()


def _build_session(max_retries: int) -> requests.Session:
    """Create a requests Session with retry logic for transient errors."""
    session = requests.Session()

    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=2,          # 2s, 4s, 8s …
        status_forcelist={429, 500, 502, 503, 504},
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


class FootballAPIClient:
    """
    Thin wrapper around the API-Football v3 HTTP API.

    Usage
    -----
    >>> client = FootballAPIClient()
    >>> fixtures = client.get_fixtures(league_id=71, season=2025)
    """

    def __init__(
        self,
        base_url: str = BASE_URL,
        headers: dict | None = None,
        requests_per_minute: int = REQUESTS_PER_MINUTE,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = headers or HEADERS
        self._rate_limiter = _RateLimiter(requests_per_minute)
        self._session = _build_session(max_retries)
        logger.info(
            "FootballAPIClient initialised — base_url=%s rpm=%d",
            self._base_url,
            requests_per_minute,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, endpoint: str, params: dict | None = None) -> dict[str, Any]:
        """
        Perform a rate-limited GET request and return the parsed JSON body.
        Raises RuntimeError on non-2xx responses after all retries.
        """
        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        self._rate_limiter.wait()

        logger.debug("GET %s  params=%s", url, params)
        response = self._session.get(
            url,
            headers=self._headers,
            params=params or {},
            timeout=30,
        )

        if not response.ok:
            logger.error(
                "HTTP %d for %s — body: %s",
                response.status_code,
                url,
                response.text[:500],
            )
            response.raise_for_status()

        body: dict = response.json()

        api_errors = body.get("errors")
        if api_errors:
            logger.warning("API-level errors for %s: %s", url, api_errors)

        logger.debug(
            "Response results=%d for %s",
            body.get("results", "?"),
            url,
        )
        return body

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    def get_fixtures(self, league_id: int, season: int) -> list[dict]:
        """Return all fixtures for a league/season."""
        body = self._get(
            "fixtures",
            params={"league": league_id, "season": season},
        )
        return body.get("response", [])

    def get_standings(self, league_id: int, season: int) -> list[dict]:
        """Return standings (table) for a league/season."""
        body = self._get(
            "standings",
            params={"league": league_id, "season": season},
        )
        return body.get("response", [])

    def get_teams(self, league_id: int, season: int) -> list[dict]:
        """Return all teams participating in a league/season."""
        body = self._get(
            "teams",
            params={"league": league_id, "season": season},
        )
        return body.get("response", [])

    def get_team_statistics(
        self,
        team_id: int,
        league_id: int,
        season: int,
    ) -> dict:
        """Return aggregated statistics for a team in a given league/season."""
        body = self._get(
            "teams/statistics",
            params={
                "team":   team_id,
                "league": league_id,
                "season": season,
            },
        )
        return body.get("response", {})

    def get_head_to_head(
        self,
        team1_id: int,
        team2_id: int,
        last: int = 10,
    ) -> list[dict]:
        """
        Return head-to-head fixture history between two teams.

        Parameters
        ----------
        team1_id, team2_id : team IDs
        last               : how many recent matches to fetch (default 10)
        """
        body = self._get(
            "fixtures/headtohead",
            params={
                "h2h":  f"{team1_id}-{team2_id}",
                "last": last,
            },
        )
        return body.get("response", [])

    def get_odds(
        self,
        fixture_id: int | None = None,
        league_id: int | None = None,
        season: int | None = None,
        bookmaker_id: int | None = None,
        bet_id: int | None = None,
        next_fixtures: int | None = None,
    ) -> list[dict]:
        """
        Return odds.  Accepts either a single fixture_id or a league/season
        pair (with optional 'next' N fixtures filter).
        """
        params: dict[str, Any] = {}
        if fixture_id is not None:
            params["fixture"] = fixture_id
        if league_id is not None:
            params["league"] = league_id
        if season is not None:
            params["season"] = season
        if bookmaker_id is not None:
            params["bookmaker"] = bookmaker_id
        if bet_id is not None:
            params["bet"] = bet_id
        if next_fixtures is not None:
            params["next"] = next_fixtures

        body = self._get("odds", params=params)
        return body.get("response", [])
