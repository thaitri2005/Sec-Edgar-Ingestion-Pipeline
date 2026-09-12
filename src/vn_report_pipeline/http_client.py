from __future__ import annotations

import hashlib
import logging
import os
import random
import time
from pathlib import Path
from typing import Any

import requests

from vn_report_pipeline.config import DownloadConfig


RETRYABLE = {403, 408, 429, 500, 502, 503, 504}


class HttpClient:
    def __init__(self, config: DownloadConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "Vietnam annual-report research pipeline/0.1"}
        )

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(self.config.retry_limit + 1):
            try:
                response = self.session.request(
                    method, url, timeout=self.config.request_timeout_seconds, **kwargs
                )
                if response.status_code not in RETRYABLE:
                    response.raise_for_status()
                    return response
                last_error = requests.HTTPError(
                    f"HTTP {response.status_code} for {url}", response=response
                )
                response.close()
            except requests.RequestException as error:
                last_error = error
                if (
                    getattr(error.response, "status_code", None) not in RETRYABLE
                    and error.response is not None
                ):
                    raise
            if attempt < self.config.retry_limit:
                retry_after = None
                if (
                    isinstance(last_error, requests.HTTPError)
                    and last_error.response is not None
                ):
                    retry_after = last_error.response.headers.get("Retry-After")
                delay = (
                    float(retry_after)
                    if retry_after and retry_after.isdigit()
                    else (
                        self.config.backoff_base_seconds * (2**attempt)
                        + random.random()
                    )
                )
                time.sleep(delay)
        assert last_error is not None
        raise last_error

    def get_json(self, url: str) -> dict[str, Any]:
        response = self._request("GET", url)
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Expected JSON object from {url}")
        return payload

    def download(
        self,
        url: str,
        destination: Path,
        expected_size: int,
        expected_hash: str,
        algorithm: str,
        resume: bool,
    ) -> tuple[int, str, int]:
        logger = logging.getLogger("vn_report_pipeline")
        destination.parent.mkdir(parents=True, exist_ok=True)
        part = destination.with_name(destination.name + ".part")
        if destination.is_file():
            digest = _checksum(destination, algorithm)
            if destination.stat().st_size == expected_size and digest == expected_hash:
                return expected_size, digest, 0
            destination.unlink()

        network_attempts = 0
        last_error: Exception | None = None
        started = time.monotonic()
        starting_size = part.stat().st_size if resume and part.is_file() else 0
        last_report = started
        for body_attempt in range(self.config.retry_limit + 1):
            existing = part.stat().st_size if resume and part.is_file() else 0
            if existing > expected_size:
                part.unlink()
                existing = 0
            headers = {"Range": f"bytes={existing}-"} if existing else {}
            try:
                response = self._request("GET", url, headers=headers, stream=True)
                network_attempts += 1
                if existing and response.status_code != 206:
                    response.close()
                    part.unlink(missing_ok=True)
                    existing = 0
                    response = self._request("GET", url, stream=True)
                    network_attempts += 1
                if existing:
                    content_range = response.headers.get("Content-Range", "")
                    if not content_range.startswith(f"bytes {existing}-"):
                        response.close()
                        part.unlink(missing_ok=True)
                        raise ValueError(
                            "Server returned an incompatible Content-Range"
                        )

                mode = "ab" if existing else "wb"
                try:
                    with part.open(mode) as handle:
                        for chunk in response.iter_content(
                            self.config.chunk_size_bytes
                        ):
                            if chunk:
                                handle.write(chunk)
                                now = time.monotonic()
                                if now - last_report >= 30:
                                    current = handle.tell()
                                    transferred = max(0, current - starting_size)
                                    rate = transferred / max(0.001, now - started)
                                    logger.info(
                                        "Downloading %s: %.2f%% (%s/%s bytes), %.2f MiB/s",
                                        destination.name,
                                        100 * current / expected_size,
                                        f"{current:,}",
                                        f"{expected_size:,}",
                                        rate / (1024 * 1024),
                                    )
                                    last_report = now
                        handle.flush()
                        os.fsync(handle.fileno())
                finally:
                    response.close()

                actual_size = part.stat().st_size
                if actual_size < expected_size:
                    raise requests.exceptions.ChunkedEncodingError(
                        f"Response ended prematurely: {actual_size}/{expected_size} bytes"
                    )
                if actual_size > expected_size:
                    part.unlink(missing_ok=True)
                    raise ValueError(
                        f"Downloaded size exceeds expectation: {actual_size} > {expected_size}"
                    )
                digest = _checksum(part, algorithm)
                if digest.lower() != expected_hash.lower():
                    part.unlink(missing_ok=True)
                    raise ValueError(f"Downloaded {algorithm} mismatch")
                os.replace(part, destination)
                logger.info(
                    "Download complete for %s: %s bytes, verified %s",
                    destination.name,
                    f"{actual_size:,}",
                    algorithm,
                )
                return actual_size, digest, network_attempts
            except (requests.RequestException, OSError) as error:
                last_error = error
                if body_attempt >= self.config.retry_limit:
                    raise
                time.sleep(
                    self.config.backoff_base_seconds * (2**body_attempt)
                    + random.random()
                )
        assert last_error is not None
        raise last_error


def _checksum(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
