from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePosixPath


class Status(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    NOT_AVAILABLE = "NOT_AVAILABLE"


class ArtifactKind(StrEnum):
    TXT = "TXT"
    HTML = "HTML"


class RunStatus(StrEnum):
    DISCOVERING = "DISCOVERING"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    INTERRUPTED = "INTERRUPTED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class DiscoverySelection:
    base_form: str
    start_year: int
    end_year: int
    cik_mode: str
    target_ciks: frozenset[str]
    fingerprint: str


@dataclass(frozen=True, slots=True)
class DiscoveredFiling:
    accession_number: str
    cik: str
    company_name: str
    base_form: str
    filing_type: str
    filing_date: str
    report_date: str | None
    filing_url: str
    submission_txt_url: str


@dataclass(frozen=True, slots=True)
class StoredFile:
    relative_path: PurePosixPath
    file_size: int
    checksum: str | None


@dataclass(frozen=True, slots=True)
class ArtifactResult:
    kind: ArtifactKind
    status: Status
    source_filename: str | None
    url: str
    local_path: str | None
    file_size: int | None
    checksum: str | None
    retry_count: int
    error: str | None = None


@dataclass(frozen=True, slots=True)
class FilingDownloadResult:
    accession_number: str
    cik: str
    report_date: str | None
    artifacts: tuple[ArtifactResult, ...]
    error: str | None = None
