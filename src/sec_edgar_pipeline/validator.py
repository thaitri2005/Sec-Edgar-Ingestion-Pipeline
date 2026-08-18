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
    b"sec.gov | your request has been blocked",
)
LEADING_HTML_ENTITY_PATTERN = re.compile(
    rb"(?:&(?:#[0-9]+|#x[0-9a-f]+|[a-z][a-z0-9]+);\s*)+",
    re.IGNORECASE,
)
HTML_START_SEARCH_LIMIT = 512
HTML_START_PATTERN = re.compile(
    rb"<!doctype\b|<\?xml\b|<!--|</?([a-z][a-z0-9:-]*)(?:\s|/?>)",
    re.IGNORECASE,
)
SEC_SGML_TAGS = {
    b"document",
    b"sec-document",
    b"text",
    b"type",
    b"sequence",
    b"filename",
    b"description",
}


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
        html_prefix = LEADING_HTML_ENTITY_PATTERN.sub(b"", html_prefix, count=1).lstrip()
        start = HTML_START_PATTERN.search(html_prefix[:HTML_START_SEARCH_LIMIT])
        if start is None:
            return ValidationResult(False, "Primary document does not begin with an HTML tag")
        html_prefix = html_prefix[start.start() :]
        tag_name = start.group(1)
        if tag_name == b"document":
            return ValidationResult(False, "Primary HTML still has an SEC document wrapper")
        if tag_name in SEC_SGML_TAGS:
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
    current_is_exact = False
    awaiting_exact_text = False
    exact_is_html: bool | None = None
    target_type = filing_type.upper()

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
                current_is_exact = False
                awaiting_exact_text = False

            filename_value = _tag_value(line, b"FILENAME")
            if filename_value:
                safe_name = PurePath(filename_value).name
                fallback_filename = fallback_filename or safe_name
                if safe_name.lower().endswith((".htm", ".html")):
                    fallback_html_filename = fallback_html_filename or safe_name
                if current_type == target_type and exact_filename is None:
                    exact_filename = safe_name
                    exact_is_html = safe_name.lower().endswith((".htm", ".html"))
                    current_is_exact = True

            if current_is_exact:
                text_index = upper.find(b"<TEXT>")
                if text_index >= 0:
                    payload = line[text_index + len(b"<TEXT>") :].lstrip().lower()
                    if payload.startswith(b"application/x-xfdl"):
                        exact_is_html = False
                    awaiting_exact_text = not payload
                elif awaiting_exact_text:
                    payload = line.strip().lower()
                    if payload:
                        if payload.startswith(b"application/x-xfdl"):
                            exact_is_html = False
                        awaiting_exact_text = False

    primary_filename = exact_filename or fallback_html_filename or fallback_filename
    if primary_filename is None:
        raise ValueError("No primary document found in complete submission TXT")
    is_html = primary_filename.lower().endswith((".htm", ".html"))
    if primary_filename == exact_filename and exact_is_html is not None:
        is_html = exact_is_html
    return PrimaryDocumentMetadata(
        filename=primary_filename,
        report_date=report_date,
        is_html=is_html,
    )

