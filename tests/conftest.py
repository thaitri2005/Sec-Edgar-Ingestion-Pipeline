from __future__ import annotations

import logging
from pathlib import Path

import pytest

from sec_edgar_pipeline.config import (
    AppConfig,
    DiscoveryConfig,
    DownloadConfig,
    LoggingConfig,
    SecConfig,
    StorageConfig,
)


@pytest.fixture
def app_config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        storage=StorageConfig(root_directory=tmp_path / "SEC_DATA"),
        download=DownloadConfig(
            max_workers=2,
            batch_size=2,
            retry_limit=1,
            request_delay_seconds=0.1,
            request_timeout_seconds=1,
            backoff_base_seconds=0.01,
            checksum_algorithm="sha256",
        ),
        discovery=DiscoveryConfig(start_year=2023, end_year=2024),
        sec=SecConfig(user_agent="Test Research test@example.com"),
        logging=LoggingConfig(progress_every=1),
    )


@pytest.fixture
def logger() -> logging.Logger:
    test_logger = logging.getLogger("sec_edgar_pipeline.tests")
    test_logger.handlers.clear()
    test_logger.addHandler(logging.NullHandler())
    return test_logger

