from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import PurePosixPath

import pytest

from sec_edgar_pipeline.downloader import DownloadService, FilingDownloader
from sec_edgar_pipeline.metadata import MetadataRepository
from sec_edgar_pipeline.models import DiscoverySelection, DiscoveredFiling, RunStatus, Status
from sec_edgar_pipeline.operations import verify_run
from sec_edgar_pipeline.sec_client import SecRequestError, StreamResponse
from sec_edgar_pipeline.storage import LocalStorageBackend


class FakeResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def iter_content(self, chunk_size: int):
        for offset in range(0, len(self.content), chunk_size):
            yield self.content[offset : offset + chunk_size]

    def close(self) -> None:
        pass


@dataclass
class FakeClient:
    payloads: dict[str, bytes]

    def __post_init__(self) -> None:
        self.calls: Counter[str] = Counter()

    def open_stream(self, url: str) -> StreamResponse:
        self.calls[url] += 1
        if url not in self.payloads:
            raise SecRequestError(f"Missing fixture: {url}", 1, 404)
        return StreamResponse(FakeResponse(self.payloads[url]), 1)

    @staticmethod
    def iter_content(response: FakeResponse):
        yield from response.iter_content(1024)


def make_filing(index: int) -> DiscoveredFiling:
    accession = f"0000320193-23-{index:06d}"
    root = f"https://example.test/{accession}"
    return DiscoveredFiling(
        accession_number=accession,
        cik="0000320193",
        company_name="APPLE INC",
        base_form="10-K",
        filing_type="10-K/A" if index % 2 else "10-K",
        filing_date="2023-11-03",
        report_date=None,
        filing_url=f"{root}-index.html",
        submission_txt_url=f"{root}.txt",
    )


def txt_payload(filing: DiscoveredFiling) -> bytes:
    primary = f"primary-{filing.accession_number}.htm"
    return f"""<SEC-DOCUMENT>{filing.accession_number}.txt
ACCESSION NUMBER: {filing.accession_number}
CONFORMED PERIOD OF REPORT: 20230930
<DOCUMENT>
<TYPE>{filing.filing_type}
<FILENAME>{primary}
<TEXT><html>embedded</html></TEXT>
</DOCUMENT>
""".encode()


def fixture_payloads(filings: list[DiscoveredFiling]) -> dict[str, bytes]:
    payloads: dict[str, bytes] = {}
    for filing in filings:
        payloads[filing.submission_txt_url] = txt_payload(filing)
        html_url = filing.submission_txt_url.rsplit("/", 1)[0]
        html_url += f"/primary-{filing.accession_number}.htm"
        payloads[html_url] = b"<!doctype html><html><body>Primary filing</body></html>"
    return payloads


def create_run(repository, app_config, filings):
    selection = DiscoverySelection("10-K", 2023, 2023, "FILE", frozenset({"0000320193"}), "fp")
    run_id = repository.create_run(selection, app_config.download.batch_size, app_config.snapshot())
    for ordinal, discovered in enumerate(filings):
        repository.add_discovered_filing(
            run_id,
            discovered,
            ordinal,
            app_config.download.batch_size,
        )
    repository.complete_discovery(run_id)
    return run_id


def test_automatic_batches_download_and_verify(app_config, logger) -> None:
    storage = LocalStorageBackend(app_config.storage.root_directory)
    storage.ensure_layout()
    filings = [make_filing(index) for index in range(5)]
    client = FakeClient(fixture_payloads(filings))

    with MetadataRepository(app_config.database_path) as repository:
        repository.initialize()
        run_id = create_run(repository, app_config, filings)
        service = DownloadService(
            app_config,
            repository,
            FilingDownloader(app_config, client, storage, logger),
            logger,
        )

        assert service.download_run(run_id) == RunStatus.SUCCESS
        assert repository.batch_numbers(run_id) == [0, 1, 2]
        assert repository.run_summary(run_id)["successful"] == 5
        report = verify_run(repository, storage, run_id, full_checksum=True)
        assert report.valid

        calls_before = client.calls.copy()
        assert service.download_run(run_id) == RunStatus.SUCCESS
        assert client.calls == calls_before


def test_partial_download_retries_only_missing_html(app_config, logger) -> None:
    storage = LocalStorageBackend(app_config.storage.root_directory)
    storage.ensure_layout()
    filing = make_filing(1)
    payloads = fixture_payloads([filing])
    html_url = next(url for url in payloads if url.endswith(".htm"))
    html_payload = payloads.pop(html_url)
    client = FakeClient(payloads)

    with MetadataRepository(app_config.database_path) as repository:
        repository.initialize()
        run_id = create_run(repository, app_config, [filing])
        service = DownloadService(
            app_config,
            repository,
            FilingDownloader(app_config, client, storage, logger),
            logger,
        )

        assert service.download_run(run_id) == RunStatus.PARTIAL
        assert repository.run_summary(run_id)["failed"] == 1
        assert client.calls[filing.submission_txt_url] == 1

        client.payloads[html_url] = html_payload
        repository.reset_failed(run_id)
        assert service.download_run(run_id) == RunStatus.SUCCESS
        assert client.calls[filing.submission_txt_url] == 1
        assert client.calls[html_url] == 2


def test_verify_marks_missing_file_failed(app_config, logger) -> None:
    storage = LocalStorageBackend(app_config.storage.root_directory)
    storage.ensure_layout()
    filing = make_filing(2)
    client = FakeClient(fixture_payloads([filing]))

    with MetadataRepository(app_config.database_path) as repository:
        repository.initialize()
        run_id = create_run(repository, app_config, [filing])
        service = DownloadService(
            app_config,
            repository,
            FilingDownloader(app_config, client, storage, logger),
            logger,
        )
        assert service.download_run(run_id) == RunStatus.SUCCESS
        item = repository.download_items(run_id, 0)
        assert item == []
        row = repository.connection.execute(
            "SELECT local_path FROM artifacts WHERE accession_number = ? AND kind = 'HTML'",
            (filing.accession_number,),
        ).fetchone()
        storage.remove(PurePosixPath(row["local_path"]))

        report = verify_run(repository, storage, run_id, mark_failed=True)

        assert not report.valid
        assert report.marked_failed == 1
        assert repository.get_run(run_id)["status"] == RunStatus.PARTIAL
        status = repository.connection.execute(
            "SELECT status FROM filings WHERE accession_number = ?",
            (filing.accession_number,),
        ).fetchone()["status"]
        assert status == Status.FAILED


def test_interrupted_run_resumes(app_config, logger) -> None:
    storage = LocalStorageBackend(app_config.storage.root_directory)
    storage.ensure_layout()
    filing = make_filing(3)
    client = FakeClient(fixture_payloads([filing]))
    real_downloader = FilingDownloader(app_config, client, storage, logger)

    class InterruptOnce:
        def __init__(self) -> None:
            self.interrupted = False

        def download(self, *args, **kwargs):
            if not self.interrupted:
                self.interrupted = True
                raise KeyboardInterrupt
            return real_downloader.download(*args, **kwargs)

    with MetadataRepository(app_config.database_path) as repository:
        repository.initialize()
        run_id = create_run(repository, app_config, [filing])
        interrupted_service = DownloadService(
            app_config,
            repository,
            InterruptOnce(),
            logger,
        )

        with pytest.raises(KeyboardInterrupt):
            interrupted_service.download_run(run_id)
        assert repository.get_run(run_id)["status"] == RunStatus.INTERRUPTED

        resumed_service = DownloadService(app_config, repository, real_downloader, logger)
        assert resumed_service.download_run(run_id) == RunStatus.SUCCESS
        assert verify_run(repository, storage, run_id).valid
