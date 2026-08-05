from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
from pathlib import Path

from sec_edgar_pipeline.config import ConfigurationError, load_config
from sec_edgar_pipeline.discovery import DiscoveryService, build_selection
from sec_edgar_pipeline.downloader import DownloadService, FilingDownloader
from sec_edgar_pipeline.logging_config import configure_logging, log_event
from sec_edgar_pipeline.metadata import MetadataRepository
from sec_edgar_pipeline.models import RunStatus
from sec_edgar_pipeline.operations import verify_run
from sec_edgar_pipeline.sec_client import SecClient
from sec_edgar_pipeline.storage import LocalStorageBackend
from sec_edgar_pipeline.validator import BASE_FORMS


DEFAULT_CONFIG = Path("configs/config.yaml")


def _handle_termination(_signum: int, _frame: object) -> None:
    raise KeyboardInterrupt


def _add_selection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--form", choices=BASE_FORMS, help="Base filing form")
    parser.add_argument("--start-year", type=int, help="Inclusive start year")
    parser.add_argument("--end-year", type=int, help="Inclusive end year")
    cik_group = parser.add_mutually_exclusive_group()
    cik_group.add_argument("--cik-file", type=Path, help="CSV containing a cik column")
    cik_group.add_argument("--all-ciks", action="store_true", help="Process every discovered CIK")
    parser.add_argument("--new-run", action="store_true", help="Do not resume a matching run")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sec-edgar")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Discover and download all batches")
    _add_selection_arguments(run_parser)

    discover_parser = subparsers.add_parser("discover", help="Discover filings without downloading")
    _add_selection_arguments(discover_parser)

    download_parser = subparsers.add_parser("download", help="Download an existing run")
    download_parser.add_argument("--run-id", type=int, required=True)

    stats_parser = subparsers.add_parser("stats", help="Show operational statistics")
    stats_parser.add_argument("--run-id", type=int)
    stats_parser.add_argument("--json", action="store_true", dest="as_json")

    verify_parser = subparsers.add_parser("verify", help="Verify metadata and local files")
    verify_parser.add_argument("--run-id", type=int)
    verify_parser.add_argument("--full-checksum", action="store_true")
    verify_parser.add_argument("--mark-failed", action="store_true")

    retry_parser = subparsers.add_parser("retry-failed", help="Retry incomplete filings")
    retry_parser.add_argument("--run-id", type=int)
    return parser


def _interactive_form() -> str:
    if not sys.stdin.isatty():
        raise ValueError("--form is required in non-interactive execution")
    print("Select a filing type:")
    for index, form in enumerate(BASE_FORMS, start=1):
        print(f"  {index}. {form} (+{form}/A)")
    while True:
        answer = input("Selection [1-3]: ").strip()
        if answer in {"1", "2", "3"}:
            return BASE_FORMS[int(answer) - 1]
        print("Enter 1, 2, or 3.")


def _resolve_run_id(repository: MetadataRepository, run_id: int | None) -> int:
    return run_id if run_id is not None else repository.latest_run_id()


def _print_stats(stats: dict[str, object], as_json: bool) -> None:
    if as_json:
        print(json.dumps(stats, indent=2, default=str))
        return
    summary = stats["summary"]
    assert isinstance(summary, dict)
    print(f"Run: {summary['id']} ({summary['status']})")
    print(f"Form: {summary['base_form']} | Years: {summary['start_year']}-{summary['end_year']}")
    print(
        f"Targets: {summary['target_companies']} | Discovered: {summary['discovered']} | "
        f"Success: {summary['successful']} | Failed: {summary['failed']} | "
        f"Pending: {summary['pending']} | Bytes: {summary['bytes']}"
    )
    for section in ("by_status", "by_form", "by_year", "by_batch"):
        print(f"\n{section.replace('_', ' ').title()}:")
        for row in stats[section]:
            print("  " + ", ".join(f"{key}={value}" for key, value in row.items()))


def main(argv: list[str] | None = None) -> int:
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_termination)
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        storage = LocalStorageBackend(config.storage.root_directory)
        storage.ensure_layout()
        logger = configure_logging(config.storage.root_directory, config.logging)

        with MetadataRepository(config.database_path) as repository:
            repository.initialize()
            if args.command == "stats":
                run_id = _resolve_run_id(repository, args.run_id)
                _print_stats(repository.detailed_stats(run_id), args.as_json)
                return 0

            if args.command == "verify":
                run_id = _resolve_run_id(repository, args.run_id)
                report = verify_run(
                    repository,
                    storage,
                    run_id,
                    full_checksum=args.full_checksum,
                    mark_failed=args.mark_failed,
                )
                print(
                    f"Run {run_id}: {report.valid_filings}/{report.expected_filings} "
                    "filings valid"
                )
                for issue in report.issues:
                    print(f"ERROR {issue.accession_number} {issue.kind or 'FILING'}: {issue.reason}")
                for cik in report.target_ciks_without_filings:
                    print(f"WARNING target CIK has no matching filings: {cik}")
                if report.marked_failed:
                    print(f"Marked {report.marked_failed} filings FAILED")
                return 0 if report.valid else 2

            client = SecClient(config.sec, config.download)
            downloader = FilingDownloader(config, client, storage, logger)
            download_service = DownloadService(config, repository, downloader, logger)

            if args.command == "download":
                status = download_service.download_run(args.run_id)
                return 0 if status == RunStatus.SUCCESS else 2

            if args.command == "retry-failed":
                run_id = _resolve_run_id(repository, args.run_id)
                reset = repository.reset_failed(run_id)
                print(f"Reset {reset} failed filings for run {run_id}")
                status = download_service.download_run(run_id)
                return 0 if status == RunStatus.SUCCESS else 2

            form = args.form or _interactive_form()
            selection = build_selection(
                config,
                form,
                args.start_year,
                args.end_year,
                args.cik_file,
                args.all_ciks,
            )
            discovery = DiscoveryService(config, repository, client, logger)
            run_id = discovery.discover(selection, force_new=args.new_run)
            print(f"Run ID: {run_id}")
            if args.command == "discover":
                return 0
            status = download_service.download_run(run_id)
            return 0 if status == RunStatus.SUCCESS else 2
    except KeyboardInterrupt:
        return 130
    except (ConfigurationError, ValueError) as error:
        parser.error(str(error))
    except Exception as error:
        if "logger" in locals():
            log_event(
                logger,
                logging.ERROR,
                "command_failed",
                str(error),
                command=args.command,
                error=str(error),
            )
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 1
