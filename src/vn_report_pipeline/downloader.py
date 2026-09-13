from __future__ import annotations

import logging
import shutil
import stat
import time
import zipfile
from pathlib import PurePosixPath
from typing import Any

from vn_report_pipeline.config import AppConfig
from vn_report_pipeline.http_client import HttpClient
from vn_report_pipeline.logging_config import log_event
from vn_report_pipeline.metadata import MetadataRepository
from vn_report_pipeline.models import ArtifactKind, RunStatus, Stage, StageStatus
from vn_report_pipeline.storage import LocalStorage, validate_pdf


class DownloadService:
    def __init__(
        self,
        config: AppConfig,
        repository: MetadataRepository,
        storage: LocalStorage,
        client: HttpClient,
    ) -> None:
        self.config = config
        self.repository = repository
        self.storage = storage
        self.client = client
        self.logger = logging.getLogger("vn_report_pipeline")

    def run(self, run_id: int, archive_name: str | None = None) -> tuple[int, int]:
        run = self.repository.get_run(run_id)
        self.repository.recover_stale(run_id)
        self.repository.set_run_status(run_id, RunStatus.RUNNING)
        sources = {
            row["name"]: row
            for row in self.repository.source_files(int(run["dataset_id"]), "ARCHIVE")
        }
        documents = self.repository.documents(run_id)
        required = sorted({row["archive_name"] for row in documents})
        if archive_name:
            if archive_name not in required:
                raise ValueError(f"Archive is not required by this run: {archive_name}")
            required = [archive_name]
        success = failed = 0
        try:
            for name in required:
                source = sources.get(name)
                if source is None:
                    raise ValueError(f"Archive is missing from Zenodo metadata: {name}")
                archive = self._ensure_archive(int(run["dataset_id"]), source)
                good, bad = self._extract_archive(run_id, name, archive)
                success += good
                failed += bad
        except KeyboardInterrupt:
            self.repository.set_run_status(run_id, RunStatus.INTERRUPTED)
            raise
        except Exception as error:
            self.repository.set_run_status(run_id, RunStatus.PARTIAL, str(error))
            raise
        self.repository.set_run_status(
            run_id,
            RunStatus.PARTIAL if failed else RunStatus.PENDING,
            f"{failed} PDF extraction failures" if failed else None,
        )
        return success, failed

    def _ensure_archive(self, dataset_id: int, source: dict[str, Any]):
        name = str(source["name"])
        digest = source.get("expected_sha256")
        if not digest:
            raise ValueError(f"Archive has no published SHA-256: {name}")
        relative = self.storage.archive_path(
            self.config.dataset.record_id, self.config.dataset.dataset_version, name
        )
        destination = self.storage.resolve(relative)
        destination.parent.mkdir(parents=True, exist_ok=True)
        part = destination.with_name(destination.name + ".part")
        remaining = int(source["expected_size"]) - (
            part.stat().st_size if part.exists() else 0
        )
        safety_margin = max(1024**3, int(source["expected_size"]) // 20)
        free = shutil.disk_usage(destination.parent).free
        if free < remaining + safety_margin:
            raise ValueError(
                f"Insufficient disk space for {name}: need at least "
                f"{remaining + safety_margin} bytes, have {free}"
            )
        try:
            size, checksum, attempts = self.client.download(
                str(source["url"]),
                destination,
                int(source["expected_size"]),
                str(digest),
                "sha256",
                self.config.download.resume_partial_archives,
            )
            self.repository.save_source_result(
                dataset_id,
                name,
                StageStatus.SUCCESS,
                relative.as_posix(),
                size,
                checksum,
                attempts,
            )
            return self.storage.resolve(relative)
        except Exception as error:
            self.repository.save_source_result(
                dataset_id, name, StageStatus.FAILED, None, None, None, 1, str(error)
            )
            raise

    def _extract_archive(
        self, run_id: int, archive_name: str, archive_path
    ) -> tuple[int, int]:
        rows = self.repository.documents_for_archive(run_id, archive_name)
        pending = [row for row in rows if row["download_status"] == StageStatus.PENDING]
        if not pending:
            return 0, 0
        total = len(pending)
        success = failed = 0
        started = time.monotonic()
        last_progress = started
        self.logger.info("Extracting %s selected PDFs from %s", total, archive_name)
        with zipfile.ZipFile(archive_path) as archive:
            infos = _safe_members(archive)
            for index, row in enumerate(pending, 1):
                execution = self.repository.record_stage_start(
                    run_id,
                    row["document_id"],
                    Stage.DOWNLOAD,
                    config_snapshot=self.config.snapshot(),
                )
                try:
                    info = _find_member(infos, row["archive_member"])
                    if info.file_size != int(row["expected_size"]):
                        raise ValueError(
                            f"Archive member size mismatch: {info.file_size} != {row['expected_size']}"
                        )
                    relative = self.storage.pdf_path(
                        row["ticker"], int(row["report_year"]), row["document_id"]
                    )
                    with archive.open(info, "r") as member:
                        stored = self.storage.write_atomic(
                            relative,
                            iter(
                                lambda: member.read(
                                    self.config.download.chunk_size_bytes
                                ),
                                b"",
                            ),
                        )
                    if stored.checksum.lower() != str(row["expected_sha256"]).lower():
                        self.storage.resolve(relative).unlink(missing_ok=True)
                        raise ValueError("Extracted PDF SHA-256 mismatch")
                    validate_pdf(
                        self.storage.resolve(relative), int(row["expected_size"])
                    )
                    self.repository.save_artifact(
                        row["document_id"],
                        ArtifactKind.PDF,
                        "",
                        relative.as_posix(),
                        stored.file_size,
                        stored.checksum,
                    )
                    self.repository.record_stage_result(
                        execution,
                        row["document_id"],
                        Stage.DOWNLOAD,
                        StageStatus.SUCCESS,
                    )
                    success += 1
                except Exception as error:
                    self.repository.record_stage_result(
                        execution,
                        row["document_id"],
                        Stage.DOWNLOAD,
                        StageStatus.FAILED,
                        str(error),
                    )
                    failed += 1
                    log_event(
                        self.logger,
                        logging.ERROR,
                        f"PDF extraction failed for {row['document_id']}: {error}",
                        run_id=run_id,
                        stage=Stage.DOWNLOAD.value,
                        archive=archive_name,
                        document_id=row["document_id"],
                        error=str(error),
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
                    log_event(
                        self.logger,
                        logging.INFO,
                        (
                            "PDF extraction from %s: %s/%s (%.1f%%), "
                            "successful %s, failed %s, %.2f documents/s, ETA %sm"
                            % (
                                archive_name,
                                f"{index:,}",
                                f"{total:,}",
                                index / total * 100,
                                f"{success:,}",
                                f"{failed:,}",
                                rate,
                                int(eta // 60),
                            )
                        ),
                        run_id=run_id,
                        stage=Stage.DOWNLOAD.value,
                        archive=archive_name,
                        processed=index,
                        total=total,
                        successful=success,
                        failed=failed,
                        percent=round(index / total * 100, 3),
                        rate_documents_per_second=round(rate, 4),
                        eta_seconds=round(eta, 3),
                    )
                    last_progress = now
        return success, failed


def _safe_members(archive: zipfile.ZipFile) -> dict[str, zipfile.ZipInfo]:
    result: dict[str, zipfile.ZipInfo] = {}
    for info in archive.infolist():
        name = info.filename.replace(chr(92), "/")
        path = PurePosixPath(name)
        if (
            path.is_absolute()
            or ".." in path.parts
            or any(":" in p for p in path.parts)
        ):
            raise ValueError(f"Unsafe ZIP member: {info.filename}")
        unix_mode = (info.external_attr >> 16) & 0xFFFF
        if unix_mode and stat.S_ISLNK(unix_mode):
            raise ValueError(f"ZIP links are not allowed: {info.filename}")
        if info.is_dir():
            continue
        if not name.lower().endswith(".pdf"):
            continue
        if (
            info.file_size > 0
            and info.compress_size > 0
            and info.file_size / info.compress_size > 1000
        ):
            raise ValueError(f"Suspicious compression ratio: {info.filename}")
        if name in result:
            raise ValueError(f"Duplicate ZIP member: {name}")
        result[name] = info
    return result


def _find_member(infos: dict[str, zipfile.ZipInfo], expected: str) -> zipfile.ZipInfo:
    if expected in infos:
        return infos[expected]
    matches = [info for name, info in infos.items() if name.endswith("/" + expected)]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(f"PDF not found in archive: {expected}")
    raise ValueError(f"Ambiguous PDF member in archive: {expected}")
