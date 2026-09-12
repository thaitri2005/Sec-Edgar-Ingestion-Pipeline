from __future__ import annotations

import argparse
import json
import shutil
import signal
import sys
from pathlib import Path

from vn_report_pipeline.catalog import CatalogService
from vn_report_pipeline.config import ConfigurationError, load_config
from vn_report_pipeline.downloader import DownloadService
from vn_report_pipeline.http_client import HttpClient
from vn_report_pipeline.logging_config import configure_logging
from vn_report_pipeline.metadata import MetadataRepository
from vn_report_pipeline.models import Stage
from vn_report_pipeline.operations import verify_run
from vn_report_pipeline.processing import ProcessingService
from vn_report_pipeline.storage import LocalStorage


DEFAULT_CONFIG = Path("configs/vn_reports.yaml")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vn-reports")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    sub = parser.add_subparsers(dest="command", required=True)
    catalog = sub.add_parser("catalog", help="Catalog the fixed Zenodo dataset")
    catalog.add_argument("--start-year", type=int)
    catalog.add_argument("--end-year", type=int)
    catalog.add_argument("--new-run", action="store_true")
    download = sub.add_parser(
        "download", help="Download archives and extract selected PDFs"
    )
    download.add_argument("--run-id", type=int, required=True)
    download.add_argument("--archive-name", help="Process one required archive only")
    for command, help_text in (
        ("extract", "Extract native page text"),
        ("ocr", "OCR low-quality pages"),
        ("normalize", "Create normalized document TXT"),
    ):
        stage = sub.add_parser(command, help=help_text)
        stage.add_argument("--run-id", type=int, required=True)
        stage.add_argument(
            "--limit", type=int, help="Process at most this many documents"
        )
    stats = sub.add_parser("stats", help="Show stage and coverage statistics")
    stats.add_argument("--run-id", type=int)
    stats.add_argument("--json", action="store_true", dest="as_json")
    verify = sub.add_parser("verify", help="Verify local artifacts against SQLite")
    verify.add_argument("--run-id", type=int)
    verify.add_argument("--full-checksum", action="store_true")
    verify.add_argument("--mark-failed", action="store_true")
    retry = sub.add_parser("retry-failed", help="Reset and retry one failed stage")
    retry.add_argument("--run-id", type=int)
    retry.add_argument(
        "--stage", choices=[stage.value for stage in Stage], required=True
    )
    retry.add_argument("--limit", type=int)
    doctor = sub.add_parser(
        "doctor", help="Check Python, storage, PDF, and OCR prerequisites"
    )
    doctor.add_argument("--require-ocr", action="store_true")
    run = sub.add_parser("run", help="Run the complete historical pipeline")
    run.add_argument("--start-year", type=int)
    run.add_argument("--end-year", type=int)
    return parser


def _termination(_signum: int, _frame: object) -> None:
    raise KeyboardInterrupt


def _run_id(repository: MetadataRepository, value: int | None) -> int:
    return value if value is not None else repository.latest_run_id()


def _print_stats(stats: dict, as_json: bool) -> None:
    if as_json:
        print(json.dumps(stats, indent=2, default=str))
        return
    summary = stats["summary"]
    print(f"Run: {summary['id']} ({summary['status']})")
    print(
        f"Years: {summary['start_year']}-{summary['end_year']} | Documents: {summary['documents']} | Tickers: {summary['tickers']}"
    )
    print(
        f"Downloaded: {summary['downloaded']} | Extracted: {summary['extracted']} | OCR: {summary['ocr_success']} | Normalized: {summary['normalized']} | Failed: {summary['failed']}"
    )
    print(f"Expected PDF bytes: {summary['expected_pdf_bytes']}")
    print("\nBy Stage:")
    for row in stats["by_stage"]:
        print(f"  {row['stage']}: status={row['status']}, documents={row['documents']}")
    print("\nBy Year:")
    for row in stats["by_year"]:
        print("  " + ", ".join(f"{key}={value}" for key, value in row.items()))


def _doctor(config) -> tuple[bool, list[str]]:
    messages = [f"Storage root: {config.storage.root_directory}"]
    try:
        import pymupdf

        messages.append(f"PyMuPDF: {pymupdf.__version__}")
    except ImportError:
        return False, messages + ["PyMuPDF: MISSING"]
    tessdata_ok = True
    if config.ocr.tessdata:
        messages.append(f"Tesseract language data: {config.ocr.tessdata}")
        for language in config.ocr.languages:
            if not (config.ocr.tessdata / f"{language}.traineddata").is_file():
                messages.append(f"OCR language missing: {language}")
                tessdata_ok = False
    else:
        messages.append("Tesseract language data: not configured")
        tessdata_ok = False
    executable = shutil.which("tesseract")
    messages.append(
        f"Standalone Tesseract: {executable}"
        if executable
        else "Standalone Tesseract: not installed (PyMuPDF integrated OCR will be used)"
    )
    return tessdata_ok, messages


def main(argv: list[str] | None = None) -> int:
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _termination)
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        storage = LocalStorage(config.storage.root_directory)
        storage.ensure_layout()
        configure_logging(config.storage.root_directory, config.logging)
        if args.command == "doctor":
            ocr_ok, messages = _doctor(config)
            for message in messages:
                print(message)
            return 0 if ocr_ok or not args.require_ocr else 2
        client = HttpClient(config.download)
        with MetadataRepository(config.database_path) as repository:
            repository.initialize()
            catalog = CatalogService(config, repository, storage, client)
            download = DownloadService(config, repository, storage, client)
            processing = ProcessingService(config, repository, storage)
            if args.command == "catalog":
                run_id, count, resumed = catalog.catalog(
                    args.start_year, args.end_year, args.new_run
                )
                print(f"Run ID: {run_id} | Documents: {count} | Resumed: {resumed}")
                return 0
            if args.command == "stats":
                _print_stats(
                    repository.stats(_run_id(repository, args.run_id)), args.as_json
                )
                return 0
            if args.command == "verify":
                run_id = _run_id(repository, args.run_id)
                valid, total, issues = verify_run(
                    repository, storage, run_id, args.full_checksum, args.mark_failed
                )
                print(
                    f"Run {run_id}: {valid}/{total} documents are free of artifact inconsistencies"
                )
                for issue in issues:
                    print(f"ERROR {issue.document_id} {issue.kind}: {issue.reason}")
                return 0 if not issues else 2
            if args.command == "download":
                good, bad = download.run(args.run_id, args.archive_name)
            elif args.command == "extract":
                good, bad = processing.extract(args.run_id, args.limit)
            elif args.command == "ocr":
                good, bad = processing.ocr(args.run_id, args.limit)
            elif args.command == "normalize":
                good, bad = processing.normalize(args.run_id, args.limit)
                repository.finish_run(args.run_id)
            elif args.command == "retry-failed":
                run_id = _run_id(repository, args.run_id)
                stage = Stage(args.stage)
                reset = repository.reset_failed(run_id, stage)
                print(f"Reset {reset} documents for {stage.value}")
                if stage == Stage.DOWNLOAD:
                    good, bad = download.run(run_id)
                elif stage == Stage.EXTRACTION:
                    good, bad = processing.extract(run_id, args.limit)
                elif stage == Stage.OCR:
                    good, bad = processing.ocr(run_id, args.limit)
                else:
                    good, bad = processing.normalize(run_id, args.limit)
                    repository.finish_run(run_id)
            elif args.command == "run":
                run_id, _, _ = catalog.catalog(args.start_year, args.end_year)
                good, bad = download.run(run_id)
                if bad == 0:
                    e_good, e_bad = processing.extract(run_id)
                    o_good, o_bad = processing.ocr(run_id)
                    n_good, n_bad = processing.normalize(run_id)
                    good += e_good + o_good + n_good
                    bad += e_bad + o_bad + n_bad
                repository.finish_run(run_id)
            else:
                raise AssertionError(args.command)
            print(f"Completed: {good} successful, {bad} failed")
            return 0 if bad == 0 else 2
    except KeyboardInterrupt:
        return 130
    except (ConfigurationError, ValueError) as error:
        parser.error(str(error))
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 1
