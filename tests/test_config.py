from __future__ import annotations

from pathlib import Path

import pytest

from sec_edgar_pipeline.config import ConfigurationError, load_config


def test_load_config_resolves_relative_paths(tmp_path: Path) -> None:
    config_path = tmp_path / "configs" / "config.yaml"
    config_path.parent.mkdir()
    config_path.write_text(
        """
storage:
  root_directory: ../data
sec:
  user_agent: "Research Team team@example.com"
discovery:
  cik_file: ciks.csv
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.storage.root_directory == (tmp_path / "data").resolve()
    assert config.discovery.cik_file == (config_path.parent / "ciks.csv").resolve()
    assert config.download.batch_size == 10_000
    assert config.download.max_workers == 3


def test_config_rejects_unidentified_user_agent(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "storage:\n  root_directory: data\nsec:\n  user_agent: anonymous\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="contact email"):
        load_config(config_path)


def test_config_rejects_request_rate_above_sec_limit(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
storage:
  root_directory: data
sec:
  user_agent: "Research Team team@example.com"
download:
  request_delay_seconds: 0.05
""",
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="at least 0.1"):
        load_config(config_path)

