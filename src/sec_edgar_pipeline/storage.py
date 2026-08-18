from __future__ import annotations

import hashlib
import os
import threading
from collections.abc import Iterable
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import BinaryIO, ContextManager, Iterator, Protocol

from sec_edgar_pipeline.models import ArtifactKind, StoredFile


class StorageBackend(Protocol):
    def ensure_layout(self) -> None: ...

    def artifact_path(
        self,
        base_form: str,
        cik: str,
        filing_year: int,
        accession_number: str,
        kind: ArtifactKind,
    ) -> PurePosixPath: ...

    def exists(self, relative_path: PurePosixPath) -> bool: ...

    def size(self, relative_path: PurePosixPath) -> int: ...

    def write_atomic(
        self,
        relative_path: PurePosixPath,
        chunks: Iterable[bytes],
        checksum_algorithm: str | None,
    ) -> StoredFile: ...

    def open_binary(self, relative_path: PurePosixPath) -> ContextManager[BinaryIO]: ...

    def read_prefix(self, relative_path: PurePosixPath, length: int) -> bytes: ...

    def checksum(self, relative_path: PurePosixPath, algorithm: str) -> str: ...

    def remove(self, relative_path: PurePosixPath) -> None: ...


class LocalStorageBackend:
    def __init__(self, root_directory: Path) -> None:
        self.root_directory = root_directory.resolve()
        self._path_lock = threading.RLock()

    def ensure_layout(self) -> None:
        for relative in ("raw", "metadata", "logs", "tmp"):
            (self.root_directory / relative).mkdir(parents=True, exist_ok=True)

    def artifact_path(
        self,
        base_form: str,
        cik: str,
        filing_year: int,
        accession_number: str,
        kind: ArtifactKind,
    ) -> PurePosixPath:
        extension = "txt" if kind == ArtifactKind.TXT else "htm"
        return PurePosixPath(
            "raw",
            base_form,
            cik,
            str(filing_year),
            f"{accession_number}.{extension}",
        )

    def _resolve(self, relative_path: PurePosixPath) -> Path:
        with self._path_lock:
            if relative_path.is_absolute() or ".." in relative_path.parts:
                raise ValueError(f"Unsafe storage path: {relative_path}")
            resolved = (self.root_directory / Path(*relative_path.parts)).resolve()
            if resolved != self.root_directory and self.root_directory not in resolved.parents:
                raise ValueError(f"Storage path escapes root: {relative_path}")
            return resolved

    def exists(self, relative_path: PurePosixPath) -> bool:
        return self._resolve(relative_path).is_file()

    def size(self, relative_path: PurePosixPath) -> int:
        return self._resolve(relative_path).stat().st_size

    def write_atomic(
        self,
        relative_path: PurePosixPath,
        chunks: Iterable[bytes],
        checksum_algorithm: str | None,
    ) -> StoredFile:
        with self._path_lock:
            destination = self._resolve(relative_path)
            destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(destination.name + ".part")
        digest = hashlib.new(checksum_algorithm) if checksum_algorithm else None
        file_size = 0

        try:
            with temporary.open("wb") as handle:
                for chunk in chunks:
                    if not chunk:
                        continue
                    handle.write(chunk)
                    file_size += len(chunk)
                    if digest is not None:
                        digest.update(chunk)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise

        return StoredFile(
            relative_path=relative_path,
            file_size=file_size,
            checksum=digest.hexdigest() if digest is not None else None,
        )

    @contextmanager
    def open_binary(self, relative_path: PurePosixPath) -> Iterator[BinaryIO]:
        with self._resolve(relative_path).open("rb") as handle:
            yield handle

    def read_prefix(self, relative_path: PurePosixPath, length: int) -> bytes:
        with self._resolve(relative_path).open("rb") as handle:
            return handle.read(length)

    def checksum(self, relative_path: PurePosixPath, algorithm: str) -> str:
        digest = hashlib.new(algorithm)
        with self._resolve(relative_path).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def remove(self, relative_path: PurePosixPath) -> None:
        self._resolve(relative_path).unlink(missing_ok=True)
