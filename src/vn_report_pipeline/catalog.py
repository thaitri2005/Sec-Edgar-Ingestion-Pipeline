from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import PurePosixPath
from typing import Any

from vn_report_pipeline.config import AppConfig
from vn_report_pipeline.http_client import HttpClient
from vn_report_pipeline.metadata import MetadataRepository
from vn_report_pipeline.models import CatalogDocument, StageStatus
from vn_report_pipeline.storage import LocalStorage


METADATA_FILES = {
    "file_index_full.csv",
    "checksums_pdf_sha256.csv",
    "checksums_archives_sha256.csv",
    "coverage_by_year.csv",
    "coverage_by_period.csv",
    "needs_review.csv",
    "data_dictionary.csv",
}
ARCHIVE_BY_PERIOD = {
    "2000_2005": "vn_bctn_2000_2005.zip",
    "2006_2010": "vn_bctn_2006_2010.zip",
    "2011_2015": "vn_bctn_2011_2015.zip",
    "2016_2020": "vn_bctn_2016_2020.zip",
    "2021_2025": "vn_bctn_2021_2025.zip",
}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class CatalogService:
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

    def catalog(
        self,
        start_year: int | None = None,
        end_year: int | None = None,
        force_new: bool = False,
    ) -> tuple[int, int, bool]:
        start = start_year if start_year is not None else self.config.dataset.start_year
        end = end_year if end_year is not None else self.config.dataset.end_year
        if start < 2000 or end < start:
            raise ValueError("Catalog year range is invalid")

        record_url = f"https://zenodo.org/api/records/{self.config.dataset.record_id}"
        record = self.client.get_json(record_url)
        metadata = dict(record.get("metadata") or {})
        actual_version = str(metadata.get("version", ""))
        if actual_version != self.config.dataset.dataset_version:
            raise ValueError(
                f"Zenodo version mismatch: {actual_version!r} != {self.config.dataset.dataset_version!r}"
            )
        metadata["doi"] = record.get("doi") or metadata.get("doi")
        dataset_id = self.repository.upsert_dataset(
            metadata,
            self.config.dataset.provider,
            self.config.dataset.record_id,
            actual_version,
        )
        self.storage.write_bytes(
            self.storage.metadata_path(
                self.config.dataset.record_id, actual_version, "record.json"
            ),
            json.dumps(record, ensure_ascii=False, sort_keys=True, indent=2).encode(
                "utf-8"
            ),
        )

        files = record.get("files")
        if not isinstance(files, list):
            raise ValueError("Zenodo record has no file list")
        by_name: dict[str, dict[str, Any]] = {}
        for item in files:
            if not isinstance(item, dict):
                continue
            name = str(item.get("key", ""))
            url = str((item.get("links") or {}).get("self", ""))
            checksum = str(item.get("checksum", ""))
            md5 = checksum.removeprefix("md5:") if checksum.startswith("md5:") else None
            kind = "ARCHIVE" if name.endswith(".zip") else "METADATA"
            if not name or not url or not str(item.get("size", "")).isdigit():
                raise ValueError("Malformed Zenodo file metadata")
            self.repository.upsert_source_file(
                dataset_id, name, kind, url, int(item["size"]), md5
            )
            by_name[name] = item

        missing = METADATA_FILES - by_name.keys()
        if missing:
            raise ValueError(
                f"Zenodo record is missing metadata files: {sorted(missing)}"
            )
        for name in sorted(METADATA_FILES):
            self._download_metadata(dataset_id, actual_version, name, by_name[name])

        self._load_archive_hashes(dataset_id, actual_version)
        fingerprint = hashlib.sha256(
            f"zenodo|{self.config.dataset.record_id}|{actual_version}|{start}|{end}".encode()
        ).hexdigest()
        run_id, resumed = self.repository.create_or_resume_run(
            dataset_id, start, end, fingerprint, self.config.snapshot(), force_new
        )
        count = self._load_documents(dataset_id, run_id, actual_version, start, end)
        if (
            start == self.config.dataset.start_year
            and end == self.config.dataset.end_year
            and count != self.config.dataset.expected_documents
        ):
            raise ValueError(
                f"Selected document count {count} does not match expected "
                f"{self.config.dataset.expected_documents}"
            )
        self.repository.complete_catalog(run_id)
        return run_id, count, resumed

    def _download_metadata(
        self, dataset_id: int, version: str, name: str, item: dict[str, Any]
    ) -> None:
        checksum = str(item["checksum"])
        if not checksum.startswith("md5:"):
            raise ValueError(f"Zenodo metadata file has no MD5: {name}")
        relative = self.storage.metadata_path(
            self.config.dataset.record_id, version, name
        )
        try:
            size, digest, attempts = self.client.download(
                str(item["links"]["self"]),
                self.storage.resolve(relative),
                int(item["size"]),
                checksum.removeprefix("md5:"),
                "md5",
                resume=False,
            )
            self.repository.save_source_result(
                dataset_id,
                name,
                StageStatus.SUCCESS,
                relative.as_posix(),
                size,
                digest,
                attempts,
            )
        except Exception as error:
            self.repository.save_source_result(
                dataset_id, name, StageStatus.FAILED, None, None, None, 1, str(error)
            )
            raise

    def _load_archive_hashes(self, dataset_id: int, version: str) -> None:
        path = self.storage.resolve(
            self.storage.metadata_path(
                self.config.dataset.record_id, version, "checksums_archives_sha256.csv"
            )
        )
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                name = row.get("archive_name", "").strip()
                digest = row.get("sha256", "").strip().lower()
                if not name or not SHA256_RE.fullmatch(digest):
                    raise ValueError("Malformed archive checksum manifest row")
                self.repository.set_source_sha256(dataset_id, name, digest)

    def _load_documents(
        self, dataset_id: int, run_id: int, version: str, start: int, end: int
    ) -> int:
        path = self.storage.resolve(
            self.storage.metadata_path(
                self.config.dataset.record_id, version, "file_index_full.csv"
            )
        )
        pdf_manifest = self._pdf_manifest(version)
        seen_ids: set[str] = set()
        seen_members: set[tuple[str, str]] = set()
        count = 0
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            required = {
                "record_id",
                "ticker_folder",
                "ticker_file",
                "year_full",
                "archive_period",
                "file_name",
                "relative_path",
                "file_size_bytes",
                "sha256",
                "status",
                "notes",
            }
            if not reader.fieldnames or not required.issubset(reader.fieldnames):
                raise ValueError("file_index_full.csv has an unexpected schema")
            for row in reader:
                try:
                    year = int(row["year_full"])
                except (TypeError, ValueError) as error:
                    raise ValueError("Invalid report year in file index") from error
                if not start <= year <= end:
                    continue
                source_id = row["record_id"].strip()
                ticker = _normalize_ticker(row["ticker_folder"])
                source_ticker = row["ticker_file"].strip()
                member = _safe_member(row["relative_path"])
                digest = row["sha256"].strip().lower()
                period = row["archive_period"].strip()
                archive = ARCHIVE_BY_PERIOD.get(period)
                if not source_id or not SAFE_ID_RE.fullmatch(source_id):
                    raise ValueError(f"Unsafe source record ID: {source_id!r}")
                if not ticker or not source_ticker or archive is None:
                    raise ValueError(f"Malformed identity/archive for {source_id}")
                if not SHA256_RE.fullmatch(digest):
                    raise ValueError(f"Invalid PDF SHA-256 for {source_id}")
                key = (archive, member)
                if source_id in seen_ids or key in seen_members:
                    raise ValueError(
                        f"Duplicate document identity in index: {source_id}"
                    )
                seen_ids.add(source_id)
                seen_members.add(key)
                try:
                    size = int(row["file_size_bytes"])
                except (TypeError, ValueError) as error:
                    raise ValueError(f"Invalid PDF size for {source_id}") from error
                manifest_value = pdf_manifest.get(member)
                if manifest_value is None:
                    raise ValueError(f"PDF is absent from checksum manifest: {member}")
                if manifest_value != (digest, size):
                    raise ValueError(f"PDF manifest disagreement for {source_id}")
                item = CatalogDocument(
                    document_id=source_id,
                    source_record_id=source_id,
                    ticker=ticker,
                    source_ticker=source_ticker,
                    report_year=year,
                    archive_name=archive,
                    archive_member=member,
                    source_filename=row["file_name"].strip(),
                    expected_size=size,
                    expected_sha256=digest,
                    source_status=row["status"].strip(),
                    source_notes=row["notes"].strip() or None,
                )
                self.repository.add_document(dataset_id, run_id, item, count)
                count += 1
        return count

    def _pdf_manifest(self, version: str) -> dict[str, tuple[str, int]]:
        path = self.storage.resolve(
            self.storage.metadata_path(
                self.config.dataset.record_id, version, "checksums_pdf_sha256.csv"
            )
        )
        result: dict[str, tuple[str, int]] = {}
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames or not {
                "sha256",
                "relative_path",
                "file_size_bytes",
            }.issubset(reader.fieldnames):
                raise ValueError("checksums_pdf_sha256.csv has an unexpected schema")
            for row in reader:
                member = _safe_member(row["relative_path"])
                digest = row["sha256"].strip().lower()
                if not SHA256_RE.fullmatch(digest):
                    raise ValueError(f"Invalid checksum manifest hash: {member}")
                if member in result:
                    raise ValueError(f"Duplicate checksum manifest path: {member}")
                try:
                    size = int(row["file_size_bytes"])
                except (TypeError, ValueError) as error:
                    raise ValueError(
                        f"Invalid checksum manifest size: {member}"
                    ) from error
                result[member] = (digest, size)
        return result


def _normalize_ticker(value: str) -> str:
    ticker = value.strip().upper()
    if not ticker or not SAFE_ID_RE.fullmatch(ticker):
        raise ValueError(f"Unsafe ticker: {value!r}")
    return ticker


def _safe_member(value: str) -> str:
    """Normalize and validate a path from the source manifest."""
    normalized = value.strip().replace(chr(92), "/")
    path = PurePosixPath(normalized)
    if (
        not normalized
        or path.is_absolute()
        or ".." in path.parts
        or any(":" in p for p in path.parts)
    ):
        raise ValueError(f"Unsafe archive path: {value!r}")
    return path.as_posix()
