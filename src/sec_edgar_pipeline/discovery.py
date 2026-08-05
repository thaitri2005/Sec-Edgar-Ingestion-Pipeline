from __future__ import annotations

import csv
import hashlib
import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from sec_edgar_pipeline.config import AppConfig, effective_end_year
from sec_edgar_pipeline.logging_config import log_event
from sec_edgar_pipeline.metadata import MetadataRepository
from sec_edgar_pipeline.models import DiscoverySelection, DiscoveredFiling, RunStatus
from sec_edgar_pipeline.sec_client import SecClient
from sec_edgar_pipeline.validator import (
    expanded_forms,
    normalize_base_form,
    normalize_cik,
    validate_accession,
)


MASTER_INDEX_URL = (
    "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
)


def validate_master_index_text(text: str) -> None:
    lowered = text.lower()
    if any(
        marker in lowered
        for marker in (
            "undeclared automated tool",
            "request rate threshold exceeded",
            "access denied",
            "request has been blocked",
        )
    ):
        raise ValueError("SEC block or access-denied page returned for master index")
    if not any(line.strip().startswith("CIK|") for line in text.splitlines()):
        raise ValueError("SEC master index header was not found")


def load_cik_file(path: Path) -> frozenset[str]:
    if not path.exists():
        raise ValueError(f"CIK file does not exist: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "cik" not in reader.fieldnames:
            raise ValueError("CIK CSV must contain a column named 'cik'")
        ciks: set[str] = set()
        for row_number, row in enumerate(reader, start=2):
            raw_cik = str(row.get("cik") or "").strip()
            if not raw_cik:
                continue
            try:
                ciks.add(normalize_cik(raw_cik))
            except ValueError as error:
                raise ValueError(f"Invalid CIK on CSV row {row_number}: {raw_cik}") from error
    if not ciks:
        raise ValueError(f"CIK file contains no CIK values: {path}")
    return frozenset(ciks)


def build_selection(
    config: AppConfig,
    form: str,
    start_year: int | None,
    end_year: int | None,
    cik_file: Path | None,
    all_ciks: bool,
) -> DiscoverySelection:
    base_form = normalize_base_form(form)
    selected_start = start_year if start_year is not None else config.discovery.start_year
    selected_end = end_year if end_year is not None else effective_end_year(config)
    current_year = datetime.now(UTC).year
    if selected_start < 1993:
        raise ValueError("Start year cannot be earlier than 1993")
    if selected_end < selected_start:
        raise ValueError("End year cannot be earlier than start year")
    if selected_end > current_year:
        raise ValueError(f"End year cannot be later than {current_year}")

    effective_cik_file = cik_file or config.discovery.cik_file
    effective_all_ciks = all_ciks or config.discovery.all_ciks
    if bool(effective_cik_file) == effective_all_ciks:
        raise ValueError("Select exactly one CIK mode: --cik-file or --all-ciks")

    if effective_all_ciks:
        cik_mode = "ALL"
        target_ciks: frozenset[str] = frozenset()
        cik_identity = "ALL"
    else:
        assert effective_cik_file is not None
        target_ciks = load_cik_file(effective_cik_file)
        cik_mode = "FILE"
        cik_identity = hashlib.sha256(
            "\n".join(sorted(target_ciks)).encode("ascii")
        ).hexdigest()

    fingerprint_payload = {
        "base_form": base_form,
        "start_year": selected_start,
        "end_year": selected_end,
        "cik_mode": cik_mode,
        "cik_identity": cik_identity,
    }
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return DiscoverySelection(
        base_form=base_form,
        start_year=selected_start,
        end_year=selected_end,
        cik_mode=cik_mode,
        target_ciks=target_ciks,
        fingerprint=fingerprint,
    )


def parse_master_index(
    text: str,
    base_form: str,
    target_ciks: frozenset[str],
) -> Iterator[DiscoveredFiling]:
    allowed_forms = set(expanded_forms(base_form))
    header_seen = False

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not header_seen:
            if line.startswith("CIK|"):
                header_seen = True
            continue
        if not line:
            continue
        parts = line.split("|", 4)
        if len(parts) != 5:
            continue
        raw_cik, company_name, filing_type, filing_date, archive_path = parts
        if filing_type not in allowed_forms:
            continue
        try:
            cik = normalize_cik(raw_cik)
        except ValueError:
            continue
        if target_ciks and cik not in target_ciks:
            continue

        archive_filename = archive_path.rsplit("/", 1)[-1]
        accession_number = archive_filename.removesuffix(".txt")
        try:
            validate_accession(accession_number)
        except ValueError:
            continue
        accession_clean = accession_number.replace("-", "")
        filing_directory = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_clean}"
        )
        yield DiscoveredFiling(
            accession_number=accession_number,
            cik=cik,
            company_name=company_name.strip(),
            base_form=base_form,
            filing_type=filing_type,
            filing_date=filing_date,
            report_date=None,
            filing_url=f"{filing_directory}/{accession_number}-index.html",
            submission_txt_url=f"https://www.sec.gov/Archives/{archive_path}",
        )


class DiscoveryService:
    def __init__(
        self,
        config: AppConfig,
        repository: MetadataRepository,
        client: SecClient,
        logger: logging.Logger,
    ) -> None:
        self.config = config
        self.repository = repository
        self.client = client
        self.logger = logger

    def discover(self, selection: DiscoverySelection, force_new: bool = False) -> int:
        existing = None if force_new else self.repository.find_resumable_run(selection.fingerprint)
        if existing is not None and existing["status"] != RunStatus.DISCOVERING:
            log_event(
                self.logger,
                logging.INFO,
                "discovery_resumed",
                f"Using existing run {existing['id']}",
                run_id=existing["id"],
                status=existing["status"],
            )
            return int(existing["id"])

        run_id = (
            int(existing["id"])
            if existing is not None
            else self.repository.create_run(
                selection,
                self.config.download.batch_size,
                self.config.snapshot(),
            )
        )
        ordinal = 0
        discovered = 0
        current_year = datetime.now(UTC).year
        current_quarter = ((datetime.now(UTC).month - 1) // 3) + 1

        try:
            for year in range(selection.start_year, selection.end_year + 1):
                final_quarter = current_quarter if year == current_year else 4
                for quarter in range(1, final_quarter + 1):
                    url = MASTER_INDEX_URL.format(year=year, quarter=quarter)
                    log_event(
                        self.logger,
                        logging.INFO,
                        "discovery_quarter_started",
                        f"Collecting {selection.base_form} index for {year} Q{quarter}",
                        run_id=run_id,
                        year=year,
                        quarter=quarter,
                    )
                    text, attempts = self.client.get_text(url)
                    validate_master_index_text(text)
                    quarter_count = 0
                    for filing in parse_master_index(
                        text,
                        selection.base_form,
                        selection.target_ciks,
                    ):
                        inserted = self.repository.add_discovered_filing(
                            run_id,
                            filing,
                            ordinal,
                            self.config.download.batch_size,
                        )
                        if selection.cik_mode == "ALL":
                            self.repository.add_run_targets(run_id, (filing.cik,))
                        ordinal += 1
                        if inserted:
                            discovered += 1
                            quarter_count += 1
                    self.repository.connection.commit()
                    log_event(
                        self.logger,
                        logging.INFO,
                        "discovery_quarter_completed",
                        f"Found {quarter_count} new {selection.base_form} filings in {year} Q{quarter}",
                        run_id=run_id,
                        year=year,
                        quarter=quarter,
                        discovered=quarter_count,
                        request_attempts=attempts,
                    )
            self.repository.complete_discovery(run_id)
        except Exception as error:
            self.repository.set_run_status(run_id, RunStatus.DISCOVERING, str(error))
            raise

        summary = self.repository.run_summary(run_id)
        log_event(
            self.logger,
            logging.INFO,
            "discovery_completed",
            f"Discovery completed: {summary['discovered']} filings, "
            f"{summary['target_companies']} unique companies",
            run_id=run_id,
            discovered=summary["discovered"],
            unique_companies=summary["target_companies"],
            newly_discovered=discovered,
        )
        return run_id
