from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import PurePosixPath
from typing import Any

from sec_edgar_pipeline.checkpoint import recover_run
from sec_edgar_pipeline.config import AppConfig
from sec_edgar_pipeline.logging_config import log_event
from sec_edgar_pipeline.metadata import MetadataRepository
from sec_edgar_pipeline.models import (
    ArtifactKind,
    ArtifactResult,
    FilingDownloadResult,
    RunStatus,
    Status,
)
from sec_edgar_pipeline.sec_client import SecClient, SecRequestError
from sec_edgar_pipeline.storage import StorageBackend
from sec_edgar_pipeline.validator import extract_submission_metadata, validate_artifact


HTML_WRAPPER_DETECTION_LIMIT = 256 * 1024
HTML_WRAPPER_TAIL_SIZE = 64 * 1024


def normalize_primary_html_chunks(chunks: Iterable[bytes]) -> Iterator[bytes]:
    """Remove an SEC SGML document wrapper while preserving the HTML payload."""
    iterator = iter(chunks)
    prefix = bytearray()

    for chunk in iterator:
        if not chunk:
            continue
        prefix.extend(chunk)
        stripped = bytes(prefix).lstrip()
        if len(stripped) < len(b"<DOCUMENT>"):
            continue
        if not stripped.upper().startswith(b"<DOCUMENT>"):
            yield bytes(prefix)
            yield from iterator
            return

        text_start = bytes(prefix).upper().find(b"<TEXT>")
        if text_start >= 0:
            content = bytes(prefix[text_start + len(b"<TEXT>") :])
            break
        if len(prefix) > HTML_WRAPPER_DETECTION_LIMIT:
            raise ValueError("Wrapped SEC HTML response has no <TEXT> marker")
    else:
        if prefix:
            yield bytes(prefix)
        return

    tail = bytearray(content)
    for chunk in iterator:
        if not chunk:
            continue
        tail.extend(chunk)
        if len(tail) > HTML_WRAPPER_TAIL_SIZE:
            emit_length = len(tail) - HTML_WRAPPER_TAIL_SIZE
            yield bytes(tail[:emit_length])
            del tail[:emit_length]

    text_end = bytes(tail).upper().rfind(b"</TEXT>")
    if text_end < 0:
        raise ValueError("Wrapped SEC HTML response has no closing </TEXT> marker")
    if text_end:
        yield bytes(tail[:text_end])


class FilingDownloader:
    def __init__(
        self,
        config: AppConfig,
        client: SecClient,
        storage: StorageBackend,
        logger: logging.Logger,
    ) -> None:
        self.config = config
        self.client = client
        self.storage = storage
        self.logger = logger

    def download(self, item: dict[str, Any], run_id: int, batch_number: int) -> FilingDownloadResult:
        started_at = time.monotonic()
        accession = str(item["accession_number"])
        cik = str(item["cik"])
        filing_year = int(str(item["filing_date"])[:4])
        base_form = str(item["base_form"])
        filing_type = str(item["filing_type"])

        log_event(
            self.logger,
            logging.DEBUG,
            "download_started",
            f"Download started for {accession}",
            run_id=run_id,
            batch_id=batch_number,
            accession=accession,
            cik=cik,
        )

        txt_path = self.storage.artifact_path(
            base_form, cik, filing_year, accession, ArtifactKind.TXT
        )
        txt_result = self._reuse_or_download(
            item=item,
            kind=ArtifactKind.TXT,
            relative_path=txt_path,
            url=str(item["submission_txt_url"]),
            source_filename=f"{accession}.txt",
            prior_size=item.get("txt_file_size"),
            prior_checksum=item.get("txt_checksum"),
            run_id=run_id,
            batch_number=batch_number,
        )

        report_date = item.get("report_date")
        html_result: ArtifactResult
        if txt_result.status == Status.SUCCESS:
            try:
                primary = extract_submission_metadata(
                    self.storage,
                    txt_path,
                    filing_type,
                )
                primary_filename = primary.filename
                report_date = report_date or primary.report_date
                # The quarterly master index points the complete submission TXT at
                # /data/{cik}/{accession}.txt, while filing documents live under
                # /data/{cik}/{accession-without-dashes}/.  The filing index URL is
                # already rooted in that document directory.
                html_url = str(item["filing_url"]).rsplit("/", 1)[0]
                html_url = f"{html_url}/{primary_filename}"
                if primary.is_html:
                    html_path = self.storage.artifact_path(
                        base_form, cik, filing_year, accession, ArtifactKind.HTML
                    )
                    html_result = self._reuse_or_download(
                        item=item,
                        kind=ArtifactKind.HTML,
                        relative_path=html_path,
                        url=html_url,
                        source_filename=primary_filename,
                        prior_size=item.get("html_file_size"),
                        prior_checksum=item.get("html_checksum"),
                        run_id=run_id,
                        batch_number=batch_number,
                    )
                else:
                    html_result = ArtifactResult(
                        kind=ArtifactKind.HTML,
                        status=Status.NOT_AVAILABLE,
                        source_filename=primary_filename,
                        url=html_url,
                        local_path=None,
                        file_size=None,
                        checksum=None,
                        retry_count=0,
                    )
                    log_event(
                        self.logger,
                        logging.INFO,
                        "primary_html_not_available",
                        f"Primary document is text-only for {accession}",
                        run_id=run_id,
                        batch_id=batch_number,
                        accession=accession,
                        cik=cik,
                        source_filename=primary_filename,
                        status=Status.NOT_AVAILABLE,
                    )
            except Exception as error:
                html_result = self._existing_html_or_failure(item, error, base_form, filing_year)
        else:
            html_result = self._existing_html_or_failure(
                item,
                RuntimeError("Complete submission TXT is unavailable"),
                base_form,
                filing_year,
            )

        success = txt_result.status == Status.SUCCESS and html_result.status in {
            Status.SUCCESS,
            Status.NOT_AVAILABLE,
        }
        duration = time.monotonic() - started_at
        level = logging.INFO if success else logging.ERROR
        log_event(
            self.logger,
            level,
            "download_completed" if success else "download_failed",
            f"Download {'completed' if success else 'failed'} for {accession}",
            run_id=run_id,
            batch_id=batch_number,
            accession=accession,
            cik=cik,
            status=Status.SUCCESS if success else Status.FAILED,
            duration_seconds=round(duration, 3),
        )
        return FilingDownloadResult(
            accession_number=accession,
            cik=cik,
            report_date=report_date,
            artifacts=(txt_result, html_result),
            error=None if success else "One or more required artifacts failed",
        )

    def _existing_html_or_failure(
        self,
        item: dict[str, Any],
        error: Exception,
        base_form: str,
        filing_year: int,
    ) -> ArtifactResult:
        accession = str(item["accession_number"])
        path = self.storage.artifact_path(
            base_form,
            str(item["cik"]),
            filing_year,
            accession,
            ArtifactKind.HTML,
        )
        existing = self._reuse_existing(
            item,
            ArtifactKind.HTML,
            path,
            str(item.get("html_url") or item["filing_url"]),
            item.get("html_source_filename"),
            item.get("html_file_size"),
            item.get("html_checksum"),
        )
        if existing is not None:
            return existing
        return ArtifactResult(
            kind=ArtifactKind.HTML,
            status=Status.FAILED,
            source_filename=item.get("html_source_filename"),
            url=str(item.get("html_url") or item["filing_url"]),
            local_path=None,
            file_size=None,
            checksum=None,
            retry_count=0,
            error=str(error),
        )

    def _reuse_or_download(
        self,
        item: dict[str, Any],
        kind: ArtifactKind,
        relative_path: PurePosixPath,
        url: str,
        source_filename: str,
        prior_size: int | None,
        prior_checksum: str | None,
        run_id: int,
        batch_number: int,
    ) -> ArtifactResult:
        existing = self._reuse_existing(
            item,
            kind,
            relative_path,
            url,
            source_filename,
            prior_size,
            prior_checksum,
        )
        if existing is not None:
            return existing
        if self.storage.exists(relative_path):
            self.storage.remove(relative_path)

        attempts = 0
        try:
            stream_response = self.client.open_stream(url)
            attempts = stream_response.attempts
            response = stream_response.response
            try:
                chunks = self.client.iter_content(response)
                if kind == ArtifactKind.HTML:
                    chunks = normalize_primary_html_chunks(chunks)
                stored = self.storage.write_atomic(
                    relative_path,
                    chunks,
                    self.config.download.checksum_algorithm,
                )
            finally:
                response.close()
            validation = validate_artifact(
                self.storage,
                relative_path,
                kind,
                str(item["accession_number"]),
            )
            if not validation.valid:
                self.storage.remove(relative_path)
                raise ValueError(validation.error)
            return ArtifactResult(
                kind=kind,
                status=Status.SUCCESS,
                source_filename=source_filename,
                url=url,
                local_path=str(stored.relative_path),
                file_size=stored.file_size,
                checksum=stored.checksum,
                retry_count=max(0, attempts - 1),
            )
        except Exception as error:
            retry_count = max(0, attempts - 1)
            if isinstance(error, SecRequestError):
                retry_count = max(0, error.attempts - 1)
            log_event(
                self.logger,
                logging.ERROR,
                "artifact_failed",
                f"{kind} failed for {item['accession_number']}: {error}",
                run_id=run_id,
                batch_id=batch_number,
                accession=item["accession_number"],
                cik=item["cik"],
                artifact_kind=kind,
                retry_count=retry_count,
                error=str(error),
            )
            return ArtifactResult(
                kind=kind,
                status=Status.FAILED,
                source_filename=source_filename,
                url=url,
                local_path=None,
                file_size=None,
                checksum=None,
                retry_count=retry_count,
                error=str(error),
            )

    def _reuse_existing(
        self,
        item: dict[str, Any],
        kind: ArtifactKind,
        relative_path: PurePosixPath,
        url: str,
        source_filename: str | None,
        prior_size: int | None,
        prior_checksum: str | None,
    ) -> ArtifactResult | None:
        if not self.storage.exists(relative_path):
            return None
        validation = validate_artifact(
            self.storage,
            relative_path,
            kind,
            str(item["accession_number"]),
        )
        if not validation.valid:
            return None
        current_size = self.storage.size(relative_path)
        if prior_size is not None and current_size != int(prior_size):
            return None
        checksum = prior_checksum
        algorithm = self.config.download.checksum_algorithm
        if algorithm:
            current_checksum = self.storage.checksum(relative_path, algorithm)
            if prior_checksum is not None and current_checksum != prior_checksum:
                return None
            checksum = current_checksum
        return ArtifactResult(
            kind=kind,
            status=Status.SUCCESS,
            source_filename=source_filename,
            url=url,
            local_path=str(relative_path),
            file_size=current_size,
            checksum=checksum,
            retry_count=0,
        )


class DownloadService:
    def __init__(
        self,
        config: AppConfig,
        repository: MetadataRepository,
        downloader: FilingDownloader,
        logger: logging.Logger,
    ) -> None:
        self.config = config
        self.repository = repository
        self.downloader = downloader
        self.logger = logger

    def download_run(self, run_id: int) -> RunStatus:
        run = self.repository.get_run(run_id)
        if run["status"] == RunStatus.DISCOVERING:
            raise ValueError(f"Run {run_id} has not completed discovery")
        recover_run(self.repository, run_id, self.logger)
        self.repository.set_run_status(run_id, RunStatus.RUNNING)
        batch_numbers = self.repository.batch_numbers(run_id)
        total_batches = len(batch_numbers)

        try:
            for batch_index, batch_number in enumerate(batch_numbers, start=1):
                items = self.repository.download_items(run_id, batch_number)
                if not items:
                    continue
                log_event(
                    self.logger,
                    logging.INFO,
                    "batch_started",
                    f"Collecting {run['base_form']} (+{run['base_form']}/A): "
                    f"batch {batch_index}/{total_batches}, {len(items)} pending",
                    run_id=run_id,
                    batch_id=batch_number,
                    batch_index=batch_index,
                    total_batches=total_batches,
                    pending=len(items),
                )
                self._process_batch(run_id, batch_number, batch_index, total_batches, items)
        except KeyboardInterrupt:
            self.repository.set_run_status(run_id, RunStatus.INTERRUPTED, "Interrupted by user")
            log_event(
                self.logger,
                logging.WARNING,
                "run_interrupted",
                f"Run {run_id} interrupted; rerun the same command to resume",
                run_id=run_id,
            )
            raise
        except Exception as error:
            self.repository.set_run_status(run_id, RunStatus.INTERRUPTED, str(error))
            raise

        status = self.repository.finish_run_from_counts(run_id)
        summary = self.repository.run_summary(run_id)
        log_event(
            self.logger,
            logging.INFO if status == RunStatus.SUCCESS else logging.WARNING,
            "run_completed",
            f"Run {run_id} completed with status {status}: "
            f"{summary['successful'] or 0}/{summary['discovered']} filings successful",
            run_id=run_id,
            status=status,
            successful=summary["successful"] or 0,
            failed=summary["failed"] or 0,
            discovered=summary["discovered"],
        )
        return status

    def _process_batch(
        self,
        run_id: int,
        batch_number: int,
        batch_index: int,
        total_batches: int,
        items: list[dict[str, Any]],
    ) -> None:
        item_iterator = iter(items)
        pending: dict[Future[FilingDownloadResult], dict[str, Any]] = {}
        completed = 0
        successful = 0
        failed = 0
        executor = ThreadPoolExecutor(max_workers=self.config.download.max_workers)

        def submit_next() -> bool:
            try:
                item = next(item_iterator)
            except StopIteration:
                return False
            self.repository.mark_running((str(item["accession_number"]),))
            future = executor.submit(
                self.downloader.download,
                item,
                run_id,
                batch_number,
            )
            pending[future] = item
            return True

        try:
            for _ in range(self.config.download.max_workers * 2):
                if not submit_next():
                    break

            while pending:
                finished, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in finished:
                    item = pending.pop(future)
                    try:
                        result = future.result()
                    except Exception as error:
                        result = self._unexpected_failure(item, error)
                    self.repository.save_download_result(result)
                    completed += 1
                    if result.error is None:
                        successful += 1
                    else:
                        failed += 1
                    if (
                        completed % self.config.logging.progress_every == 0
                        or completed == len(items)
                    ):
                        log_event(
                            self.logger,
                            logging.INFO,
                            "batch_progress",
                            f"Collecting {item['base_form']} (+{item['base_form']}/A): "
                            f"batch {batch_index}/{total_batches}, "
                            f"processed {completed:,}/{len(items):,}, "
                            f"successful {successful:,}, failed {failed:,}",
                            run_id=run_id,
                            batch_id=batch_number,
                            completed=completed,
                            batch_total=len(items),
                            successful=successful,
                            failed=failed,
                        )
                    submit_next()
        except KeyboardInterrupt:
            for future in pending:
                future.cancel()
            raise
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

    @staticmethod
    def _unexpected_failure(
        item: dict[str, Any],
        error: Exception,
    ) -> FilingDownloadResult:
        artifacts = tuple(
            ArtifactResult(
                kind=kind,
                status=Status.FAILED,
                source_filename=None,
                url=str(item["submission_txt_url"] if kind == ArtifactKind.TXT else item["filing_url"]),
                local_path=None,
                file_size=None,
                checksum=None,
                retry_count=0,
                error=str(error),
            )
            for kind in (ArtifactKind.TXT, ArtifactKind.HTML)
        )
        return FilingDownloadResult(
            accession_number=str(item["accession_number"]),
            cik=str(item["cik"]),
            report_date=item.get("report_date"),
            artifacts=artifacts,
            error=str(error),
        )
