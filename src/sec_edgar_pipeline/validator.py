from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePath, PurePosixPath

from sec_edgar_pipeline.models import ArtifactKind
from sec_edgar_pipeline.storage import StorageBackend


BASE_FORMS = ("10-K", "10-Q", "8-K")
ACCESSION_PATTERN = re.compile(r"^\d{10}-\d{2}-\d{6}$")
BLOCK_PAGE_MARKERS = (
    b"your request originates from an undeclared automated tool",
    b"request rate threshold exceeded",
    b"access denied",
    b"sec.gov | your request has been blocked",
)


@dataclass(frozen=True, slots=True)
class ValidationResult:
    valid: bool
    error: str | None = None


@dataclass(frozen=True, slots=True)
class PrimaryDocumentMetadata:
    filename: str
    report_date: str | None
    is_html: bool


def normalize_cik(value: str | int) -> str:
    text = str(value).strip()
    if not text.isdigit() or len(text) > 10:
        raise ValueError(f"Invalid CIK: {value!r}")
    return text.zfill(10)


def normalize_base_form(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in BASE_FORMS:
        raise ValueError(f"Unsupported filing form: {value}")
    return normalized


def expanded_forms(base_form: str) -> tuple[str, str]:
    normalized = normalize_base_form(base_form)
    return normalized, f"{normalized}/A"


def validate_accession(accession_number: str) -> None:
    if not ACCESSION_PATTERN.fullmatch(accession_number):
        raise ValueError(f"Invalid accession number: {accession_number}")


def validate_artifact(
    storage: StorageBackend,
    relative_path: PurePosixPath,
    kind: ArtifactKind,
    accession_number: str,
) -> ValidationResult:
    expected_suffix = ".txt" if kind == ArtifactKind.TXT else ".htm"
    if relative_path.suffix.lower() != expected_suffix:
        return ValidationResult(False, f"Expected {expected_suffix} extension")
    if not storage.exists(relative_path):
        return ValidationResult(False, "File does not exist")
    if storage.size(relative_path) <= 0:
        return ValidationResult(False, "File is empty")

    prefix = storage.read_prefix(relative_path, 256 * 1024).lower()
    if any(marker in prefix for marker in BLOCK_PAGE_MARKERS):
        return ValidationResult(False, "SEC block or access-denied page detected")

    if kind == ArtifactKind.TXT:
        if b"<sec-document" not in prefix and b"<document>" not in prefix:
            return ValidationResult(False, "TXT does not look like an EDGAR submission")
        compact_accession = accession_number.replace("-", "").encode()
        if accession_number.encode() not in prefix and compact_accession not in prefix:
            return ValidationResult(False, "TXT does not contain the expected accession")
    else:
        html_prefix = prefix.lstrip()
        if html_prefix.startswith(b"\xef\xbb\xbf"):
            html_prefix = html_prefix[3:].lstrip()
        if html_prefix.startswith(b"<document>"):
            return ValidationResult(False, "Primary HTML still has an SEC document wrapper")
        if not html_prefix.startswith((b"<!doctype", b"<?xml", b"<!--")):
            tag = re.match(rb"<([a-z][a-z0-9:-]*)(?:\s|/?>)", html_prefix)
            if tag is None:
                return ValidationResult(False, "Primary document does not begin with an HTML tag")
            if tag.group(1) in {
                b"sec-document",
                b"text",
                b"type",
                b"sequence",
                b"filename",
                b"description",
            }:
                return ValidationResult(False, "Primary HTML begins with an SEC SGML tag")

    return ValidationResult(True)


def _tag_value(line: bytes, tag: bytes) -> str | None:
    upper = line.upper()
    marker = b"<" + tag + b">"
    index = upper.find(marker)
    if index < 0:
        return None
    value = line[index + len(marker) :].strip()
    closing = b"</" + tag + b">"
    closing_index = value.upper().find(closing)
    if closing_index >= 0:
        value = value[:closing_index]
    return value.decode("latin-1", errors="replace").strip()


def extract_submission_metadata(
    storage: StorageBackend,
    txt_path: PurePosixPath,
    filing_type: str,
) -> PrimaryDocumentMetadata:
    exact_filename: str | None = None
    fallback_html_filename: str | None = None
    fallback_filename: str | None = None
    report_date: str | None = None
    current_type: str | None = None

    with storage.open_binary(txt_path) as handle:
        for line in handle:
            upper = line.upper()
            if report_date is None and b"CONFORMED PERIOD OF REPORT:" in upper:
                raw_date = line.split(b":", 1)[1].strip().decode("ascii", errors="ignore")
                if len(raw_date) == 8 and raw_date.isdigit():
                    report_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

            type_value = _tag_value(line, b"TYPE")
            if type_value is not None:
                current_type = type_value.upper()

            filename_value = _tag_value(line, b"FILENAME")
            if filename_value:
                safe_name = PurePath(filename_value).name
                fallback_filename = fallback_filename or safe_name
                if safe_name.lower().endswith((".htm", ".html")):
                    fallback_html_filename = fallback_html_filename or safe_name
                if current_type == filing_type.upper() and exact_filename is None:
                    exact_filename = safe_name

    primary_filename = exact_filename or fallback_html_filename or fallback_filename
    if primary_filename is None:
        raise ValueError("No primary document found in complete submission TXT")
    return PrimaryDocumentMetadata(
        filename=primary_filename,
        report_date=report_date,
        is_html=primary_filename.lower().endswith((".htm", ".html")),
    )

