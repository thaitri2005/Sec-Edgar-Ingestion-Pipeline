from __future__ import annotations

import hashlib
import sqlite3
import zipfile
from pathlib import Path

import pymupdf
import pytest

from vn_report_pipeline.catalog import _normalize_ticker, _safe_member
from vn_report_pipeline.config import ConfigurationError, load_config
from vn_report_pipeline.downloader import DownloadService, _safe_members
from vn_report_pipeline.http_client import HttpClient
from vn_report_pipeline.config import DownloadConfig
from vn_report_pipeline.metadata import MetadataRepository
from vn_report_pipeline.models import CatalogDocument, RunStatus
from vn_report_pipeline.operations import verify_run
from vn_report_pipeline.processing import ProcessingService
from vn_report_pipeline.storage import LocalStorage


def _config(tmp_path: Path, minimum_characters: int = 10):
    path = tmp_path / "configs" / "vn.yaml"
    path.parent.mkdir()
    path.write_text(
        f"""
storage:
  root_directory: ../VN_DATA
dataset:
  record_id: "20949551"
  dataset_version: "1.0.0"
  expected_documents: 1
download:
  chunk_size_bytes: 4096
extraction:
  minimum_characters_per_page: {minimum_characters}
ocr:
  enabled: false
normalization:
  profile: test-normalize-v1
""",
        encoding="utf-8",
    )
    return load_config(path)


def _pdf_bytes(text: str) -> bytes:
    document = pymupdf.open()
    page = document.new_page()
    page.insert_text((72, 72), text)
    result = document.tobytes()
    document.close()
    return result


class FakeClient:
    def __init__(self, source: Path) -> None:
        self.source = source

    def download(
        self, _url, destination, expected_size, expected_hash, algorithm, _resume
    ):
        data = self.source.read_bytes()
        digest = hashlib.new(algorithm, data).hexdigest()
        assert len(data) == expected_size
        assert digest == expected_hash
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        return len(data), digest, 1


class FakeResponse:
    def __init__(self, status_code: int, data: bytes, content_range: str = "") -> None:
        self.status_code = status_code
        self.data = data
        self.headers = {"Content-Range": content_range} if content_range else {}

    def iter_content(self, _chunk_size):
        yield self.data

    def close(self) -> None:
        return None


def test_incomplete_body_retries_from_partial_file(tmp_path: Path, monkeypatch) -> None:
    client = HttpClient(
        DownloadConfig(retry_limit=1, backoff_base_seconds=0.001, chunk_size_bytes=4096)
    )
    responses = [
        FakeResponse(200, b"ab"),
        FakeResponse(206, b"cd", "bytes 2-3/4"),
    ]
    monkeypatch.setattr(client, "_request", lambda *_args, **_kwargs: responses.pop(0))
    monkeypatch.setattr("vn_report_pipeline.http_client.random.random", lambda: 0)
    destination = tmp_path / "archive.zip"
    digest = hashlib.sha256(b"abcd").hexdigest()
    size, checksum, attempts = client.download(
        "fixture://archive", destination, 4, digest, "sha256", True
    )
    assert (size, checksum, attempts) == (4, digest, 2)
    assert destination.read_bytes() == b"abcd"


def test_config_resolves_vn_data_beside_configs(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.storage.root_directory == tmp_path / "VN_DATA"
    assert config.dataset.start_year == 2008


def test_invalid_config_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("storage: {root_directory: data}\ndownload: {max_workers: 0}\n")
    with pytest.raises(ConfigurationError, match="worker"):
        load_config(path)


def test_schema_one_migrates_stage_configuration_snapshot(tmp_path: Path) -> None:
    database = tmp_path / "metadata.db"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE schema_version(version INTEGER NOT NULL);
        INSERT INTO schema_version VALUES(1);
        CREATE TABLE stage_executions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            document_id TEXT,
            stage TEXT NOT NULL,
            profile TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL,
            attempt INTEGER NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            error TEXT
        );
        """
    )
    connection.close()
    with MetadataRepository(database) as repository:
        repository.initialize()
        assert (
            repository.connection.execute(
                "SELECT version FROM schema_version"
            ).fetchone()["version"]
            == 2
        )
        columns = {
            row["name"]
            for row in repository.connection.execute(
                "PRAGMA table_info(stage_executions)"
            )
        }
        assert "config_json" in columns


def test_storage_and_catalog_paths_reject_traversal(tmp_path: Path) -> None:
    storage = LocalStorage(tmp_path)
    with pytest.raises(ValueError, match="Unsafe"):
        storage.resolve("../escape")
    with pytest.raises(ValueError, match="Unsafe"):
        _safe_member("../../report.pdf")
    assert _normalize_ticker(" acb ") == "ACB"


def test_zip_member_validation_rejects_traversal(tmp_path: Path) -> None:
    archive_path = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("../escape.pdf", b"%PDF-bad")
    with zipfile.ZipFile(archive_path) as archive:
        with pytest.raises(ValueError, match="Unsafe ZIP"):
            _safe_members(archive)


def test_complete_pdf_to_normalized_text_pipeline(tmp_path: Path) -> None:
    config = _config(tmp_path)
    storage = LocalStorage(config.storage.root_directory)
    storage.ensure_layout()
    pdf = _pdf_bytes("Bao cao thuong nien ACB 2024 with sufficient native text")
    pdf_sha = hashlib.sha256(pdf).hexdigest()
    archive_path = tmp_path / "source.zip"
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("ACB/ACB_2024.pdf", pdf)
    archive_sha = hashlib.sha256(archive_path.read_bytes()).hexdigest()

    with MetadataRepository(config.database_path) as repository:
        repository.initialize()
        dataset_id = repository.upsert_dataset(
            {"title": "fixture", "version": "1.0.0", "license": {"id": "cc-by-4.0"}},
            "zenodo",
            "20949551",
            "1.0.0",
        )
        repository.upsert_source_file(
            dataset_id,
            "vn_bctn_2021_2025.zip",
            "ARCHIVE",
            "fixture://archive",
            archive_path.stat().st_size,
            None,
        )
        repository.set_source_sha256(dataset_id, "vn_bctn_2021_2025.zip", archive_sha)
        run_id, _ = repository.create_or_resume_run(
            dataset_id, 2024, 2024, "fixture", config.snapshot(), True
        )
        repository.add_document(
            dataset_id,
            run_id,
            CatalogDocument(
                document_id="ACB_2024_fixture",
                source_record_id="ACB_2024_fixture",
                ticker="ACB",
                source_ticker="ACB",
                report_year=2024,
                archive_name="vn_bctn_2021_2025.zip",
                archive_member="ACB/ACB_2024.pdf",
                source_filename="ACB_2024.pdf",
                expected_size=len(pdf),
                expected_sha256=pdf_sha,
                source_status="ok",
                source_notes=None,
            ),
            0,
        )
        repository.complete_catalog(run_id)

        downloaded, failures = DownloadService(
            config, repository, storage, FakeClient(archive_path)
        ).run(run_id)
        assert (downloaded, failures) == (1, 0)

        extracted, failures = ProcessingService(config, repository, storage).extract(
            run_id
        )
        assert (extracted, failures) == (1, 0)
        row = repository.documents(run_id)[0]
        assert row["extraction_status"] == "SUCCESS"
        assert row["ocr_status"] == "NOT_REQUIRED"

        normalized, failures = ProcessingService(config, repository, storage).normalize(
            run_id
        )
        assert (normalized, failures) == (1, 0)
        assert repository.finish_run(run_id) == RunStatus.SUCCESS
        valid, total, issues = verify_run(
            repository, storage, run_id, full_checksum=True
        )
        assert (valid, total, issues) == (1, 1, [])
        text_artifact = repository.artifact(
            "ACB_2024_fixture", "TEXT", config.normalization.profile
        )
        assert text_artifact is not None
        text = storage.resolve(text_artifact["local_path"]).read_text(encoding="utf-8")
        assert text.startswith("<<<PAGE 1>>>")
        assert "Bao cao thuong nien" in text
