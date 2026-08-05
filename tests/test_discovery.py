from __future__ import annotations

from pathlib import Path

import pytest

from sec_edgar_pipeline.discovery import (
    DiscoveryService,
    load_cik_file,
    parse_master_index,
    validate_master_index_text,
)
from sec_edgar_pipeline.metadata import MetadataRepository
from sec_edgar_pipeline.models import DiscoverySelection, RunStatus


MASTER_INDEX = """Description line
CIK|Company Name|Form Type|Date Filed|Filename
320193|APPLE INC|10-K|2023-11-03|edgar/data/320193/0000320193-23-000106.txt
320193|APPLE INC|10-K/A|2023-11-17|edgar/data/320193/0000320193-23-000110.txt
320193|APPLE INC|10-Q|2023-08-04|edgar/data/320193/0000320193-23-000077.txt
789019|MICROSOFT CORP|10-K|2023-07-27|edgar/data/789019/0000950170-23-035122.txt
"""


def test_parse_master_index_includes_amendments_and_filters_ciks() -> None:
    filings = list(
        parse_master_index(MASTER_INDEX, "10-K", frozenset({"0000320193"}))
    )

    assert [filing.filing_type for filing in filings] == ["10-K", "10-K/A"]
    assert all(filing.cik == "0000320193" for filing in filings)
    assert filings[0].submission_txt_url.endswith("0000320193-23-000106.txt")
    assert filings[0].filing_url.endswith("0000320193-23-000106-index.html")


def test_parse_master_index_all_ciks() -> None:
    filings = list(parse_master_index(MASTER_INDEX, "10-K", frozenset()))

    assert len(filings) == 3
    assert {filing.cik for filing in filings} == {"0000320193", "0000789019"}


def test_load_cik_file_normalizes_and_deduplicates(tmp_path: Path) -> None:
    path = tmp_path / "ciks.csv"
    path.write_text("cik\n320193\n0000320193\n789019\n", encoding="utf-8")

    assert load_cik_file(path) == frozenset({"0000320193", "0000789019"})


def test_master_index_validation_rejects_sec_block_page() -> None:
    with pytest.raises(ValueError, match="block"):
        validate_master_index_text("<html>Access Denied</html>")


def test_failed_discovery_remains_resumable(app_config, logger) -> None:
    class FailingClient:
        def get_text(self, _url):
            raise RuntimeError("temporary SEC failure")

    selection = DiscoverySelection(
        "10-K",
        2023,
        2023,
        "FILE",
        frozenset({"0000320193"}),
        "discovery-fingerprint",
    )
    with MetadataRepository(app_config.database_path) as repository:
        repository.initialize()
        service = DiscoveryService(app_config, repository, FailingClient(), logger)

        with pytest.raises(RuntimeError, match="temporary SEC failure"):
            service.discover(selection)

        run = repository.find_resumable_run(selection.fingerprint)
        assert run is not None
        assert run["status"] == RunStatus.DISCOVERING
