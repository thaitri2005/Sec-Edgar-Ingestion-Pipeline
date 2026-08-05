from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml


class ConfigurationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class StorageConfig:
    root_directory: Path


@dataclass(frozen=True, slots=True)
class DownloadConfig:
    max_workers: int = 3
    batch_size: int = 10_000
    retry_limit: int = 5
    request_delay_seconds: float = 0.35
    request_timeout_seconds: float = 60.0
    backoff_base_seconds: float = 2.0
    checksum_algorithm: str | None = "sha256"


@dataclass(frozen=True, slots=True)
class DiscoveryConfig:
    start_year: int = 2009
    end_year: int | None = None
    cik_file: Path | None = None
    all_ciks: bool = False


@dataclass(frozen=True, slots=True)
class SecConfig:
    user_agent: str


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
    download: DownloadConfig
    discovery: DiscoveryConfig
    sec: SecConfig
    logging: LoggingConfig

    @property
    def database_path(self) -> Path:
        return self.storage.root_directory / "metadata" / "metadata.db"

    def snapshot(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["storage"]["root_directory"] = str(self.storage.root_directory)
        if self.discovery.cik_file is not None:
            payload["discovery"]["cik_file"] = str(self.discovery.cik_file)
        return payload


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigurationError(f"{name} must be a YAML mapping")
    return value


def _resolve_path(value: str | Path, config_directory: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = config_directory / path
    return path.resolve()


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).expanduser().resolve()
    if not config_path.exists():
        raise ConfigurationError(f"Configuration file does not exist: {config_path}")

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ConfigurationError("Configuration root must be a YAML mapping")

    storage_raw = _mapping(raw.get("storage"), "storage")
    download_raw = _mapping(raw.get("download"), "download")
    discovery_raw = _mapping(raw.get("discovery"), "discovery")
    sec_raw = _mapping(raw.get("sec"), "sec")
    logging_raw = _mapping(raw.get("logging"), "logging")

    root_value = storage_raw.get("root_directory")
    if not root_value:
        raise ConfigurationError("storage.root_directory is required")

    cik_file_value = discovery_raw.get("cik_file")
    end_year_value = discovery_raw.get("end_year")
    user_agent = os.environ.get("SEC_USER_AGENT", sec_raw.get("user_agent", "")).strip()

    config = AppConfig(
        storage=StorageConfig(
            root_directory=_resolve_path(root_value, config_path.parent),
        ),
        download=DownloadConfig(
            max_workers=int(download_raw.get("max_workers", 3)),
            batch_size=int(download_raw.get("batch_size", 10_000)),
            retry_limit=int(download_raw.get("retry_limit", 5)),
            request_delay_seconds=float(download_raw.get("request_delay_seconds", 0.35)),
            request_timeout_seconds=float(download_raw.get("request_timeout_seconds", 60)),
            backoff_base_seconds=float(download_raw.get("backoff_base_seconds", 2)),
            checksum_algorithm=download_raw.get("checksum_algorithm", "sha256"),
        ),
        discovery=DiscoveryConfig(
            start_year=int(discovery_raw.get("start_year", 2009)),
            end_year=(int(end_year_value) if end_year_value is not None else None),
            cik_file=(
                _resolve_path(cik_file_value, config_path.parent)
                if cik_file_value
                else None
            ),
            all_ciks=bool(discovery_raw.get("all_ciks", False)),
        ),
        sec=SecConfig(user_agent=user_agent),
        logging=LoggingConfig(
            console_level=str(logging_raw.get("console_level", "INFO")).upper(),
            file_level=str(logging_raw.get("file_level", "DEBUG")).upper(),
            max_bytes=int(logging_raw.get("max_bytes", 10_485_760)),
            backup_count=int(logging_raw.get("backup_count", 10)),
            progress_every=int(logging_raw.get("progress_every", 100)),
        ),
    )
    validate_config(config)
    return config


def effective_end_year(config: AppConfig) -> int:
    return config.discovery.end_year or datetime.now(UTC).year


def validate_config(config: AppConfig) -> None:
    if config.download.max_workers < 1:
        raise ConfigurationError("download.max_workers must be at least 1")
    if config.download.batch_size < 1:
        raise ConfigurationError("download.batch_size must be at least 1")
    if config.download.retry_limit < 0:
        raise ConfigurationError("download.retry_limit cannot be negative")
    if config.download.request_delay_seconds < 0.1:
        raise ConfigurationError(
            "download.request_delay_seconds must be at least 0.1 to respect the SEC limit"
        )
    if config.download.request_timeout_seconds <= 0:
        raise ConfigurationError("download.request_timeout_seconds must be positive")
    if config.download.backoff_base_seconds <= 0:
        raise ConfigurationError("download.backoff_base_seconds must be positive")
    if config.download.checksum_algorithm not in {None, "sha256"}:
        raise ConfigurationError("download.checksum_algorithm must be sha256 or null")
    if config.discovery.start_year < 1993:
        raise ConfigurationError("discovery.start_year cannot be earlier than 1993")
    if effective_end_year(config) < config.discovery.start_year:
        raise ConfigurationError("discovery.end_year cannot be earlier than start_year")
    if not config.sec.user_agent or "@" not in config.sec.user_agent:
        raise ConfigurationError(
            "sec.user_agent must identify an organization and contact email"
        )
    if config.logging.progress_every < 1:
        raise ConfigurationError("logging.progress_every must be at least 1")

