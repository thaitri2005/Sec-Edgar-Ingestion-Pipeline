from __future__ import annotations

import pytest

from sec_edgar_pipeline.config import DownloadConfig, SecConfig
from sec_edgar_pipeline.sec_client import SecClient, SecRequestError


class FakeResponse:
    def __init__(self, status_code: int, retry_after: str | None = None) -> None:
        self.status_code = status_code
        self.headers = {"Retry-After": retry_after} if retry_after else {}

    def close(self) -> None:
        pass


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = iter(responses)
        self.calls = 0

    def get(self, *_args, **_kwargs) -> FakeResponse:
        self.calls += 1
        return next(self.responses)


def client_with_session(session: FakeSession) -> SecClient:
    client = SecClient(
        SecConfig("Research Team test@example.com"),
        DownloadConfig(retry_limit=1, request_delay_seconds=0.1),
    )
    client._local.session = session
    client.rate_limiter.acquire = lambda: None
    client._sleep_before_retry = lambda *_: None
    return client


def test_sec_client_retries_429() -> None:
    session = FakeSession([FakeResponse(429, "0"), FakeResponse(200)])
    client = client_with_session(session)

    result = client.open_stream("https://example.test/filing.txt")

    assert result.attempts == 2
    assert session.calls == 2


def test_sec_client_does_not_retry_404() -> None:
    session = FakeSession([FakeResponse(404)])
    client = client_with_session(session)

    with pytest.raises(SecRequestError) as captured:
        client.open_stream("https://example.test/missing.txt")

    assert captured.value.status_code == 404
    assert session.calls == 1
