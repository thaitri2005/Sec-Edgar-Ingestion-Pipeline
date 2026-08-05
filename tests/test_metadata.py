from __future__ import annotations

from sec_edgar_pipeline.metadata import MetadataRepository
from sec_edgar_pipeline.models import DiscoverySelection, DiscoveredFiling, RunStatus, Status


def filing(index: int) -> DiscoveredFiling:
    accession = f"0000320193-23-{index:06d}"
    return DiscoveredFiling(
        accession_number=accession,
        cik="0000320193",
        company_name="APPLE INC",
        base_form="10-K",
        filing_type="10-K",
        filing_date="2023-11-03",
        report_date=None,
        filing_url=f"https://example.test/{accession}-index.html",
        submission_txt_url=f"https://example.test/{accession}.txt",
    )


def test_repository_assigns_batches_and_recovers_running(app_config) -> None:
    with MetadataRepository(app_config.database_path) as repository:
        repository.initialize()
        selection = DiscoverySelection("10-K", 2023, 2023, "FILE", frozenset({"0000320193"}), "fp")
        run_id = repository.create_run(selection, 2, app_config.snapshot())
        for index in range(5):
            repository.add_discovered_filing(run_id, filing(index), index, 2)
        repository.complete_discovery(run_id)

        assert repository.batch_numbers(run_id) == [0, 1, 2]
        first = repository.download_items(run_id, 0)
        repository.mark_running((first[0]["accession_number"],))
        assert repository.recover_stale_running(run_id) == 1
        recovered = repository.download_items(run_id, 0)
        assert recovered[0]["status"] == Status.PENDING
        assert repository.get_run(run_id)["status"] == RunStatus.PENDING


def test_duplicate_accession_is_not_added_twice(app_config) -> None:
    with MetadataRepository(app_config.database_path) as repository:
        repository.initialize()
        selection = DiscoverySelection("10-K", 2023, 2023, "ALL", frozenset(), "fp")
        run_id = repository.create_run(selection, 2, app_config.snapshot())

        assert repository.add_discovered_filing(run_id, filing(1), 0, 2)
        assert not repository.add_discovered_filing(run_id, filing(1), 0, 2)
        assert repository.run_summary(run_id)["discovered"] == 1

