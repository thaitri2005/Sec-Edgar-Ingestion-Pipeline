from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
import unicodedata
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from vn_report_pipeline.config import AppConfig
from vn_report_pipeline.logging_config import log_event
from vn_report_pipeline.metadata import MetadataRepository
from vn_report_pipeline.models import ArtifactKind, Stage, StageStatus
from vn_report_pipeline.storage import LocalStorage


class ProcessingService:
    def __init__(
        self, config: AppConfig, repository: MetadataRepository, storage: LocalStorage
    ) -> None:
        self.config = config
        self.repository = repository
        self.storage = storage
        self.logger = logging.getLogger("vn_report_pipeline")

    def extract(self, run_id: int, limit: int | None = None) -> tuple[int, int]:
        self.repository.recover_stale(run_id)
        rows = [
            r
            for r in self.repository.documents(run_id, Stage.EXTRACTION)
            if r["download_status"] == StageStatus.SUCCESS
            and r["extraction_status"] == StageStatus.PENDING
        ]
        return self._run_documents(
            run_id, Stage.EXTRACTION, rows[:limit] if limit else rows, self._extract_one
        )

    def ocr(self, run_id: int, limit: int | None = None) -> tuple[int, int]:
        self.repository.recover_stale(run_id)
        if not self.config.ocr.enabled:
            raise ValueError("OCR is disabled in configuration")
        rows = [
            r
            for r in self.repository.documents(run_id, Stage.OCR)
            if r["ocr_status"] == StageStatus.PENDING
        ]
        return self._run_documents(
            run_id, Stage.OCR, rows[:limit] if limit else rows, self._ocr_one
        )

    def normalize(self, run_id: int, limit: int | None = None) -> tuple[int, int]:
        self.repository.recover_stale(run_id)
        rows = [
            r
            for r in self.repository.documents(run_id, Stage.NORMALIZATION)
            if r["extraction_status"] in {StageStatus.SUCCESS, StageStatus.NEEDS_OCR}
            and r["ocr_status"] in {StageStatus.SUCCESS, StageStatus.NOT_REQUIRED}
            and r["normalization_status"] != StageStatus.SUCCESS
        ]
        return self._run_documents(
            run_id,
            Stage.NORMALIZATION,
            rows[:limit] if limit else rows,
            self._normalize_one,
        )

    def _run_documents(
        self, run_id: int, stage: Stage, rows, operation
    ) -> tuple[int, int]:
        total = len(rows)
        label = "OCR" if stage == Stage.OCR else stage.value.capitalize()
        success = failed = 0
        started = time.monotonic()
        last_progress = started
        self.logger.info(
            "%s started: %s pending documents for run %s", label, total, run_id
        )
        if not total:
            return 0, 0
        for index, row in enumerate(rows, 1):
            document_started = time.monotonic()
            execution = self.repository.record_stage_start(
                run_id,
                row["document_id"],
                stage,
                self._profile(stage),
                self.config.snapshot(),
            )
            try:
                result = operation(row)
                status = result.pop("status", StageStatus.SUCCESS)
                self.repository.record_stage_result(
                    execution, row["document_id"], stage, status, **result
                )
                success += 1
            except Exception as error:
                self.repository.record_stage_result(
                    execution, row["document_id"], stage, StageStatus.FAILED, str(error)
                )
                failed += 1
                log_event(
                    self.logger,
                    logging.ERROR,
                    f"{label} failed for {row['document_id']}: {error}",
                    run_id=run_id,
                    stage=stage.value,
                    document_id=row["document_id"],
                    ticker=row["ticker"],
                    report_year=row["report_year"],
                    error=str(error),
                    duration_seconds=round(time.monotonic() - document_started, 3),
                )
            now = time.monotonic()
            if (
                index == 1
                or index == total
                or index % self.config.logging.progress_every == 0
                or now - last_progress >= 30
            ):
                elapsed = max(now - started, 0.001)
                rate = index / elapsed
                eta = (total - index) / rate if rate else 0
                message = (
                    "%s progress: %s/%s (%.1f%%), successful %s, failed %s, "
                    "%.2f documents/s, elapsed %s, ETA %s; last %s %s (%s)"
                    % (
                        label,
                        f"{index:,}",
                        f"{total:,}",
                        index / total * 100,
                        f"{success:,}",
                        f"{failed:,}",
                        rate,
                        _duration(elapsed),
                        _duration(eta),
                        row["ticker"],
                        row["report_year"],
                        row["document_id"],
                    )
                )
                log_event(
                    self.logger,
                    logging.INFO,
                    message,
                    run_id=run_id,
                    stage=stage.value,
                    processed=index,
                    total=total,
                    successful=success,
                    failed=failed,
                    percent=round(index / total * 100, 3),
                    elapsed_seconds=round(elapsed, 3),
                    rate_documents_per_second=round(rate, 4),
                    eta_seconds=round(eta, 3),
                    document_id=row["document_id"],
                    ticker=row["ticker"],
                    report_year=row["report_year"],
                )
                last_progress = now
        return success, failed

    def _extract_one(self, row: dict[str, Any]) -> dict[str, Any]:
        pymupdf = _pymupdf()
        pdf = self.repository.artifact(row["document_id"], ArtifactKind.PDF)
        if pdf is None:
            raise ValueError("Verified PDF artifact is missing")
        pages: list[dict[str, Any]] = []
        with pymupdf.open(self.storage.resolve(pdf["local_path"])) as document:
            if document.page_count < 1:
                raise ValueError("PDF has no pages")
            last_page_progress = time.monotonic()
            for index, page in enumerate(document):
                text = page.get_text("text", sort=True)
                quality = _quality(
                    text, self.config.extraction.minimum_characters_per_page
                )
                pages.append(
                    _page_dict(row, index + 1, text, "native", page.rect, quality)
                )
                now = time.monotonic()
                if now - last_page_progress >= 30:
                    self.logger.info(
                        "Extraction document %s: page %s/%s",
                        row["document_id"],
                        index + 1,
                        document.page_count,
                    )
                    last_page_progress = now
        relative = self.storage.pages_path(
            self.config.extraction.native_profile,
            row["ticker"],
            row["report_year"],
            row["document_id"],
        )
        stored = self.storage.write_atomic(relative, _jsonl_chunks(pages))
        self.repository.save_artifact(
            row["document_id"],
            ArtifactKind.PAGES_NATIVE,
            self.config.extraction.native_profile,
            relative.as_posix(),
            stored.file_size,
            stored.checksum,
        )
        self.repository.save_pages(
            row["document_id"], self.config.extraction.native_profile, pages
        )
        low = sum(bool(page["needs_ocr"]) for page in pages)
        status = StageStatus.NEEDS_OCR if low else StageStatus.SUCCESS
        return {"status": status, "page_count": len(pages), "low_quality_pages": low}

    def _ocr_one(self, row: dict[str, Any]) -> dict[str, Any]:
        pymupdf = _pymupdf()
        pdf = self.repository.artifact(row["document_id"], ArtifactKind.PDF)
        native = self.repository.artifact(
            row["document_id"],
            ArtifactKind.PAGES_NATIVE,
            self.config.extraction.native_profile,
        )
        if pdf is None or native is None:
            raise ValueError("PDF or native page artifact is missing")
        pages = _read_jsonl(self.storage.resolve(native["local_path"]))
        by_page = {int(page["page_number"]): page for page in pages}
        ocr_total = sum(bool(page["needs_ocr"]) for page in pages)
        self.logger.info(
            "OCR document %s (%s %s): %s pages require OCR",
            row["document_id"],
            row["ticker"],
            row["report_year"],
            ocr_total,
        )
        languages = "+".join(self.config.ocr.languages)
        tessdata = str(self.config.ocr.tessdata) if self.config.ocr.tessdata else None
        with pymupdf.open(self.storage.resolve(pdf["local_path"])) as document:
            if document.page_count != len(pages):
                raise ValueError("PDF and native JSONL page counts differ")
            ocr_processed = 0
            last_page_progress = time.monotonic()
            for index, page in enumerate(document):
                current = by_page[index + 1]
                if not current["needs_ocr"]:
                    continue
                with _silence_native_stderr():
                    textpage = page.get_textpage_ocr(
                        language=languages,
                        dpi=self.config.ocr.dpi,
                        full=True,
                        tessdata=tessdata,
                    )
                text = page.get_text("text", textpage=textpage, sort=True)
                quality = _quality(
                    text, self.config.extraction.minimum_characters_per_page
                )
                replacement = _page_dict(
                    row, index + 1, text, "ocr", page.rect, quality
                )
                if replacement["quality_score"] >= current["quality_score"]:
                    by_page[index + 1] = replacement
                ocr_processed += 1
                now = time.monotonic()
                if now - last_page_progress >= 30 or ocr_processed == ocr_total:
                    self.logger.info(
                        "OCR document %s: %s/%s OCR pages completed",
                        row["document_id"],
                        ocr_processed,
                        ocr_total,
                    )
                    last_page_progress = now
        selected = [by_page[number] for number in sorted(by_page)]
        relative = self.storage.pages_path(
            self.config.ocr.profile,
            row["ticker"],
            row["report_year"],
            row["document_id"],
        )
        stored = self.storage.write_atomic(relative, _jsonl_chunks(selected))
        self.repository.save_artifact(
            row["document_id"],
            ArtifactKind.PAGES_OCR,
            self.config.ocr.profile,
            relative.as_posix(),
            stored.file_size,
            stored.checksum,
        )
        self.repository.save_pages(
            row["document_id"], self.config.ocr.profile, selected
        )
        unresolved = sum(bool(page["needs_ocr"]) for page in selected)
        if unresolved:
            self.logger.warning(
                "OCR completed for %s with %s unresolved blank or sparse pages",
                row["document_id"],
                unresolved,
            )
        return {"low_quality_pages": unresolved}

    def _normalize_one(self, row: dict[str, Any]) -> dict[str, Any]:
        artifact = self.repository.artifact(
            row["document_id"], ArtifactKind.PAGES_OCR, self.config.ocr.profile
        ) or self.repository.artifact(
            row["document_id"],
            ArtifactKind.PAGES_NATIVE,
            self.config.extraction.native_profile,
        )
        if artifact is None:
            raise ValueError("No page JSONL artifact is available")
        pages = _read_jsonl(self.storage.resolve(artifact["local_path"]))
        texts = [str(page["text"]) for page in pages]
        normalized = _normalize_pages(texts, self.config)
        chunks = []
        for number, text in enumerate(normalized, 1):
            chunks.append(f"<<<PAGE {number}>>>\n{text.strip()}\n\n".encode("utf-8"))
        relative = self.storage.text_path(
            self.config.normalization.profile,
            row["ticker"],
            row["report_year"],
            row["document_id"],
        )
        stored = self.storage.write_atomic(relative, chunks)
        self.repository.save_artifact(
            row["document_id"],
            ArtifactKind.TEXT,
            self.config.normalization.profile,
            relative.as_posix(),
            stored.file_size,
            stored.checksum,
        )
        return {}

    def _profile(self, stage: Stage) -> str:
        return {
            Stage.DOWNLOAD: "",
            Stage.EXTRACTION: self.config.extraction.native_profile,
            Stage.OCR: self.config.ocr.profile,
            Stage.NORMALIZATION: self.config.normalization.profile,
        }[stage]


def _pymupdf():
    try:
        import pymupdf
    except ImportError as error:
        raise RuntimeError(
            "PyMuPDF is required; reinstall the project environment"
        ) from error
    pymupdf.TOOLS.mupdf_display_errors(False)
    pymupdf.TOOLS.mupdf_display_warnings(False)
    return pymupdf


def _duration(seconds: float) -> str:
    total = max(0, int(seconds + 0.5))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:d}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes:d}m {secs:02d}s"
    return f"{secs:d}s"


@contextmanager
def _silence_native_stderr():
    try:
        stderr_fd = sys.stderr.fileno()
        saved_fd = os.dup(stderr_fd)
        null_fd = os.open(os.devnull, os.O_WRONLY)
    except (AttributeError, OSError):
        yield
        return
    try:
        os.dup2(null_fd, stderr_fd)
        yield
    finally:
        os.dup2(saved_fd, stderr_fd)
        os.close(saved_fd)
        os.close(null_fd)


def _quality(text: str, minimum: int) -> dict[str, Any]:
    stripped = "".join(character for character in text if not character.isspace())
    count = len(stripped)
    printable = sum(character.isprintable() for character in stripped) / max(1, count)
    replacement = sum(
        character == "\ufffd" or unicodedata.category(character) == "Cc"
        for character in stripped
    ) / max(1, count)
    length_score = min(1.0, count / max(1, minimum))
    score = max(0.0, min(1.0, length_score * printable * (1 - replacement)))
    needs_ocr = count < minimum or printable < 0.85 or replacement > 0.02
    return {
        "character_count": count,
        "printable_ratio": printable,
        "replacement_ratio": replacement,
        "quality_score": score,
        "needs_ocr": needs_ocr,
    }


def _page_dict(row, number, text, method, rect, quality) -> dict[str, Any]:
    return {
        "document_id": row["document_id"],
        "ticker": row["ticker"],
        "report_year": int(row["report_year"]),
        "page_number": number,
        "text": text,
        "method": method,
        "character_count": quality["character_count"],
        "printable_ratio": quality["printable_ratio"],
        "replacement_ratio": quality["replacement_ratio"],
        "quality_score": quality["quality_score"],
        "needs_ocr": quality["needs_ocr"],
        "width": float(rect.width),
        "height": float(rect.height),
        "error": None,
    }


def _jsonl_chunks(pages):
    for page in pages:
        yield (
            json.dumps(page, ensure_ascii=False, separators=(",", ":")) + "\n"
        ).encode("utf-8")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    pages = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            try:
                page = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"Invalid JSONL at line {line_number}") from error
            if int(page.get("page_number", 0)) != line_number:
                raise ValueError("JSONL pages are not unique and sequential")
            pages.append(page)
    if not pages:
        raise ValueError("Page JSONL is empty")
    return pages


def _normalize_pages(texts: list[str], config: AppConfig) -> list[str]:
    normalized = []
    for text in texts:
        text = unicodedata.normalize(config.normalization.unicode_form, text)
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = "".join(
            ch for ch in text if ch in "\n\t" or unicodedata.category(ch) != "Cc"
        )
        if config.normalization.repair_line_wrap_hyphenation:
            text = re.sub(r"(?<=\w)-\n(?=\w)", "", text)
        normalized.append(text)
    if config.normalization.remove_repeated_headers_footers and len(normalized) >= 3:
        edge_lines = []
        for text in normalized:
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            edge_lines.append((lines[0] if lines else "", lines[-1] if lines else ""))
        threshold = max(3, int(len(normalized) * 0.6 + 0.999))
        repeated = {
            line
            for line, count in Counter(
                value for pair in edge_lines for value in set(pair) if value
            ).items()
            if count >= threshold
        }
        if repeated:
            normalized = [
                "\n".join(
                    line for line in text.splitlines() if line.strip() not in repeated
                )
                for text in normalized
            ]
    return normalized
