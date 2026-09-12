from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable
from pathlib import Path, PurePosixPath

from vn_report_pipeline.models import StoredFile


class LocalStorage:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def ensure_layout(self) -> None:
        for name in (
            "source_metadata",
            "archives",
            "raw",
            "processed",
            "metadata",
            "logs",
            "tmp",
        ):
            (self.root / name).mkdir(parents=True, exist_ok=True)

    def resolve(self, relative: PurePosixPath | str) -> Path:
        value = PurePosixPath(relative)
        if (
            value.is_absolute()
            or ".." in value.parts
            or any(":" in part for part in value.parts)
        ):
            raise ValueError(f"Unsafe storage path: {relative}")
        result = (self.root / Path(*value.parts)).resolve()
        if result != self.root and self.root not in result.parents:
            raise ValueError(f"Storage path escapes root: {relative}")
        return result

    def metadata_path(self, record_id: str, version: str, name: str) -> PurePosixPath:
        return PurePosixPath("source_metadata", "zenodo", record_id, version, name)

    def archive_path(self, record_id: str, version: str, name: str) -> PurePosixPath:
        return PurePosixPath("archives", "zenodo", record_id, version, name)

    def pdf_path(self, ticker: str, year: int, document_id: str) -> PurePosixPath:
        return PurePosixPath(
            "raw", "annual-reports", ticker, str(year), f"{document_id}.pdf"
        )

    def pages_path(
        self, profile: str, ticker: str, year: int, document_id: str
    ) -> PurePosixPath:
        return PurePosixPath(
            "processed", "pages", profile, ticker, str(year), f"{document_id}.jsonl"
        )

    def text_path(
        self, profile: str, ticker: str, year: int, document_id: str
    ) -> PurePosixPath:
        return PurePosixPath(
            "processed", "text", profile, ticker, str(year), f"{document_id}.txt"
        )

    def write_atomic(
        self,
        relative: PurePosixPath | str,
        chunks: Iterable[bytes],
        algorithm: str = "sha256",
    ) -> StoredFile:
        destination = self.resolve(relative)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(destination.name + ".part")
        digest = hashlib.new(algorithm)
        size = 0
        try:
            with temporary.open("wb") as handle:
                for chunk in chunks:
                    if not chunk:
                        continue
                    handle.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        return StoredFile(PurePosixPath(relative), size, digest.hexdigest())

    def write_bytes(self, relative: PurePosixPath | str, data: bytes) -> StoredFile:
        return self.write_atomic(relative, (data,))

    def exists(self, relative: PurePosixPath | str) -> bool:
        return self.resolve(relative).is_file()

    def size(self, relative: PurePosixPath | str) -> int:
        return self.resolve(relative).stat().st_size

    def checksum(self, relative: PurePosixPath | str, algorithm: str = "sha256") -> str:
        return checksum_file(self.resolve(relative), algorithm)


def checksum_file(path: Path, algorithm: str = "sha256") -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_pdf(path: Path, expected_size: int | None = None) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError("PDF is missing or empty")
    if expected_size is not None and path.stat().st_size != expected_size:
        raise ValueError(f"PDF size mismatch: {path.stat().st_size} != {expected_size}")
    with path.open("rb") as handle:
        if handle.read(5) != b"%PDF-":
            raise ValueError("File does not begin with a PDF signature")
