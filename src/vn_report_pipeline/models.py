from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePosixPath


class StageStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    NOT_REQUIRED = "NOT_REQUIRED"
    NEEDS_OCR = "NEEDS_OCR"


class RunStatus(StrEnum):
    CATALOGING = "CATALOGING"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    INTERRUPTED = "INTERRUPTED"
    FAILED = "FAILED"


class ArtifactKind(StrEnum):
    ARCHIVE = "ARCHIVE"
    PDF = "PDF"
    PAGES_NATIVE = "PAGES_NATIVE"
    PAGES_OCR = "PAGES_OCR"
    TEXT = "TEXT"


class Stage(StrEnum):
    DOWNLOAD = "download"
    EXTRACTION = "extraction"
    OCR = "ocr"
    NORMALIZATION = "normalization"


@dataclass(frozen=True, slots=True)
class StoredFile:
    relative_path: PurePosixPath
    file_size: int
    checksum: str


@dataclass(frozen=True, slots=True)
class CatalogDocument:
    document_id: str
    source_record_id: str
    ticker: str
    source_ticker: str
    report_year: int
    archive_name: str
    archive_member: str
    source_filename: str
    expected_size: int
    expected_sha256: str
    source_status: str
    source_notes: str | None


@dataclass(frozen=True, slots=True)
class PageRecord:
    page_number: int
    text: str
    method: str
    character_count: int
    printable_ratio: float
    replacement_ratio: float
    quality_score: float
    needs_ocr: bool
    width: float
    height: float
    error: str | None = None
