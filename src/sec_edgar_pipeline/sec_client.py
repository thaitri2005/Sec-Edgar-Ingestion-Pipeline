from __future__ import annotations

import email.utils
import random
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Iterator

import requests

from sec_edgar_pipeline.config import DownloadConfig, SecConfig


class SecRequestError(RuntimeError):
    def __init__(self, message: str, attempts: int, status_code: int | None = None) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class StreamResponse:
    response: requests.Response
    attempts: int


class RateLimiter:
    def __init__(self, minimum_interval: float) -> None:
        self.minimum_interval = minimum_interval
        self._lock = threading.Lock()
        self._last_request = 0.0

    def acquire(self) -> None:
        with self._lock:
            wait_seconds = self.minimum_interval - (
                time.monotonic() - self._last_request
            )
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            self._last_request = time.monotonic()


class SecClient:
    def __init__(self, sec_config: SecConfig, download_config: DownloadConfig) -> None:
        self.download_config = download_config
        self.headers = {
            "User-Agent": sec_config.user_agent,
            "Accept-Encoding": "gzip, deflate",
        }
        self.rate_limiter = RateLimiter(download_config.request_delay_seconds)
        self._local = threading.local()

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update(self.headers)
            self._local.session = session
        return session

    def get_text(self, url: str) -> tuple[str, int]:
        stream_response = self.open_stream(url)
        try:
            return stream_response.response.text, stream_response.attempts
        finally:
            stream_response.response.close()

    def open_stream(self, url: str) -> StreamResponse:
        total_attempts = self.download_config.retry_limit + 1

        for attempt in range(1, total_attempts + 1):
            self.rate_limiter.acquire()
            try:
                response = self._session().get(
                    url,
                    timeout=self.download_config.request_timeout_seconds,
                    stream=True,
                )
            except requests.RequestException as error:
                if attempt < total_attempts:
                    self._sleep_before_retry(attempt, None)
                    continue
                raise SecRequestError(str(error), attempt) from error

            if response.status_code == 200:
                return StreamResponse(response=response, attempts=attempt)

            status_code = response.status_code
            retry_after = response.headers.get("Retry-After")
            response.close()
            retryable = status_code in {403, 408, 429} or 500 <= status_code < 600
            if retryable and attempt < total_attempts:
                self._sleep_before_retry(attempt, retry_after)
                continue
            raise SecRequestError(
                f"SEC request failed with HTTP {status_code}: {url}",
                attempt,
                status_code,
            )

        raise AssertionError("Retry loop terminated unexpectedly")

    @staticmethod
    def iter_content(response: requests.Response) -> Iterator[bytes]:
        yield from response.iter_content(chunk_size=1024 * 1024)

    def _sleep_before_retry(self, attempt: int, retry_after: str | None) -> None:
        delay = self._retry_after_seconds(retry_after)
        if delay is None:
            delay = self.download_config.backoff_base_seconds * (2 ** (attempt - 1))
            delay += random.uniform(0, min(1.0, delay * 0.1))
        time.sleep(delay)

    @staticmethod
    def _retry_after_seconds(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                retry_at = email.utils.parsedate_to_datetime(value)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=UTC)
                return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())
            except (TypeError, ValueError):
                return None
