from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from vn_report_pipeline.config import LoggingConfig


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        context = getattr(record, "context", None)
        if isinstance(context, dict):
            payload.update(context)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(root: Path, config: LoggingConfig) -> logging.Logger:
    logger = logging.getLogger("vn_report_pipeline")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    console = logging.StreamHandler()
    console.setLevel(config.console_level)
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log_path = root / "logs" / "pipeline.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=config.max_bytes,
        backupCount=config.backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(config.file_level)
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger


def log_event(logger: logging.Logger, level: int, message: str, **context: Any) -> None:
    logger.log(level, message, extra={"context": context})
