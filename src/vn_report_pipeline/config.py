from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml


class ConfigurationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class StorageConfig:
    root_directory: Path


@dataclass(frozen=True, slots=True)
class DatasetConfig:
    provider: str = "zenodo"
    record_id: str = "20949551"
    dataset_version: str = "1.0.0"
    start_year: int = 2008
    end_year: int = 2025
    expected_documents: int = 13_884


@dataclass(frozen=True, slots=True)
class DownloadConfig:
    max_workers: int = 2
    retry_limit: int = 5
    request_timeout_seconds: float = 120.0
    backoff_base_seconds: float = 2.0
    chunk_size_bytes: int = 1024 * 1024
    resume_partial_archives: bool = True


@dataclass(frozen=True, slots=True)
class ExtractionConfig:
    max_workers: int = 4
    native_engine: str = "pymupdf"
    native_profile: str = "native-v1"
    minimum_characters_per_page: int = 100
    minimum_document_text_coverage: float = 0.70


@dataclass(frozen=True, slots=True)
class OcrConfig:
    enabled: bool = True
    engine: str = "tesseract"
    languages: tuple[str, ...] = ("vie", "eng")
    profile: str = "ocr-v1"
    dpi: int = 200
    tessdata: Path | None = None


@dataclass(frozen=True, slots=True)
class NormalizationConfig:
    profile: str = "normalize-v1"
    unicode_form: str = "NFC"
    remove_repeated_headers_footers: bool = True
    repair_line_wrap_hyphenation: bool = True


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    console_level: str = "INFO"
    file_level: str = "DEBUG"
    max_bytes: int = 10_485_760
    backup_count: int = 10
    progress_every: int = 100


@dataclass(frozen=True, slots=True)
class AppConfig:
    storage: StorageConfig
    dataset: DatasetConfig
    download: DownloadConfig
    extraction: ExtractionConfig
    ocr: OcrConfig
    normalization: NormalizationConfig
    logging: LoggingConfig

    @property
    def database_path(self) -> Path:
        return self.storage.root_directory / "metadata" / "metadata.db"

    def snapshot(self) -> dict[str, Any]:
        result = asdict(self)
        result["storage"]["root_directory"] = str(self.storage.root_directory)
        if self.ocr.tessdata is not None:
            result["ocr"]["tessdata"] = str(self.ocr.tessdata)
        return result


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigurationError(f"{name} must be a YAML mapping")
    return value


def _resolve(value: str | Path, base: Path) -> Path:
    path = Path(value).expanduser()
    return (base / path).resolve() if not path.is_absolute() else path.resolve()


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).expanduser().resolve()
    if not config_path.is_file():
        raise ConfigurationError(f"Configuration file does not exist: {config_path}")
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ConfigurationError("Configuration root must be a YAML mapping")

    storage = _mapping(raw.get("storage"), "storage")
    dataset = _mapping(raw.get("dataset"), "dataset")
    download = _mapping(raw.get("download"), "download")
    extraction = _mapping(raw.get("extraction"), "extraction")
    ocr = _mapping(raw.get("ocr"), "ocr")
    normalization = _mapping(raw.get("normalization"), "normalization")
    logging = _mapping(raw.get("logging"), "logging")
    root = storage.get("root_directory")
    if not root:
        raise ConfigurationError("storage.root_directory is required")
    tessdata_value = ocr.get("tessdata")

    config = AppConfig(
        storage=StorageConfig(_resolve(root, config_path.parent)),
        dataset=DatasetConfig(
            provider=str(dataset.get("provider", "zenodo")).lower(),
            record_id=str(dataset.get("record_id", "20949551")),
            dataset_version=str(dataset.get("dataset_version", "1.0.0")),
            start_year=int(dataset.get("start_year", 2008)),
            end_year=int(dataset.get("end_year", 2025)),
            expected_documents=int(dataset.get("expected_documents", 13_884)),
        ),
        download=DownloadConfig(
            max_workers=int(download.get("max_workers", 2)),
            retry_limit=int(download.get("retry_limit", 5)),
            request_timeout_seconds=float(download.get("request_timeout_seconds", 120)),
            backoff_base_seconds=float(download.get("backoff_base_seconds", 2)),
            chunk_size_bytes=int(download.get("chunk_size_bytes", 1024 * 1024)),
            resume_partial_archives=bool(download.get("resume_partial_archives", True)),
        ),
        extraction=ExtractionConfig(
            max_workers=int(extraction.get("max_workers", 4)),
            native_engine=str(extraction.get("native_engine", "pymupdf")),
            native_profile=str(extraction.get("native_profile", "native-v1")),
            minimum_characters_per_page=int(
                extraction.get("minimum_characters_per_page", 100)
            ),
            minimum_document_text_coverage=float(
                extraction.get("minimum_document_text_coverage", 0.70)
            ),
        ),
        ocr=OcrConfig(
            enabled=bool(ocr.get("enabled", True)),
            engine=str(ocr.get("engine", "tesseract")),
            languages=tuple(str(v) for v in ocr.get("languages", ["vie", "eng"])),
            profile=str(ocr.get("profile", "ocr-v1")),
            dpi=int(ocr.get("dpi", 200)),
            tessdata=_resolve(tessdata_value, config_path.parent)
            if tessdata_value
            else None,
        ),
        normalization=NormalizationConfig(
            profile=str(normalization.get("profile", "normalize-v1")),
            unicode_form=str(normalization.get("unicode_form", "NFC")),
            remove_repeated_headers_footers=bool(
                normalization.get("remove_repeated_headers_footers", True)
            ),
            repair_line_wrap_hyphenation=bool(
                normalization.get("repair_line_wrap_hyphenation", True)
            ),
        ),
        logging=LoggingConfig(
            console_level=str(logging.get("console_level", "INFO")).upper(),
            file_level=str(logging.get("file_level", "DEBUG")).upper(),
            max_bytes=int(logging.get("max_bytes", 10_485_760)),
            backup_count=int(logging.get("backup_count", 10)),
            progress_every=int(logging.get("progress_every", 100)),
        ),
    )
    validate_config(config)
    return config


def validate_config(config: AppConfig) -> None:
    if config.dataset.provider != "zenodo":
        raise ConfigurationError("dataset.provider must be zenodo")
    if not config.dataset.record_id.isdigit():
        raise ConfigurationError("dataset.record_id must be numeric")
    if (
        config.dataset.start_year < 2000
        or config.dataset.end_year < config.dataset.start_year
    ):
        raise ConfigurationError("dataset year range is invalid")
    if config.download.max_workers < 1 or config.extraction.max_workers < 1:
        raise ConfigurationError("worker counts must be at least 1")
    if config.download.retry_limit < 0 or config.download.request_timeout_seconds <= 0:
        raise ConfigurationError("download retry/timeout settings are invalid")
    if (
        config.download.backoff_base_seconds <= 0
        or config.download.chunk_size_bytes < 4096
    ):
        raise ConfigurationError("download backoff/chunk settings are invalid")
    if config.extraction.native_engine != "pymupdf":
        raise ConfigurationError("extraction.native_engine must be pymupdf")
    if config.extraction.minimum_characters_per_page < 0:
        raise ConfigurationError("minimum_characters_per_page cannot be negative")
    if not 0 <= config.extraction.minimum_document_text_coverage <= 1:
        raise ConfigurationError(
            "minimum_document_text_coverage must be between 0 and 1"
        )
    if (
        config.ocr.engine != "tesseract"
        or not config.ocr.languages
        or config.ocr.dpi < 72
    ):
        raise ConfigurationError("OCR engine, languages, or DPI are invalid")
    if config.normalization.unicode_form not in {"NFC", "NFKC", "NFD", "NFKD"}:
        raise ConfigurationError("normalization.unicode_form is invalid")
    if config.logging.progress_every < 1:
        raise ConfigurationError("logging.progress_every must be at least 1")
