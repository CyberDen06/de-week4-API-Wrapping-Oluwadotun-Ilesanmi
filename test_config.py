"""Tests for ConfigManager using a temporary pipeline.cfg file."""

import pytest
from pathlib import Path

from config import ConfigManager


def test_config_manager_reads_values(tmp_path):
    cfg_content = """[API]
base_url = https://example.org
limit = 7
"""
    cfg_file = tmp_path / "pipeline.cfg"
    cfg_file.write_text(cfg_content)

    cfg = ConfigManager(str(cfg_file))

    assert cfg.base_url == "https://example.org"
    assert cfg.limit == 7


def test_config_manager_missing_file_raises(tmp_path):
    missing = tmp_path / "does_not_exist.cfg"
    with pytest.raises(FileNotFoundError):
        ConfigManager(str(missing))
