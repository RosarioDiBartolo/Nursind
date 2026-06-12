from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.config import load_pipeline_config
from core.errors import ConfigurationError


def test_config_resolves_relative_output_from_repository_root(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        json.dumps(
            {
                "pipeline": {"name": "demo", "base_output_dir": "runs"},
                "drive": {"root_id": "drive-root"},
                "steps": {"scan": {"workers": 3}},
            }
        ),
        encoding="utf-8",
    )

    config = load_pipeline_config(config_path)

    assert config.paths.pipeline_root == tmp_path / "runs" / "demo"
    assert config.paths.scan_included_index == tmp_path / "runs" / "demo" / "scan" / "included.index.json"
    assert config.step("scan") == {"workers": 3}


def test_config_rejects_missing_required_values(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    path = tmp_path / "pipeline.json"
    path.write_text('{"pipeline": {}, "drive": {}}', encoding="utf-8")

    with pytest.raises(ConfigurationError, match="pipeline.name"):
        load_pipeline_config(path)


def test_paths_create_only_canonical_stage_directories(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    path = tmp_path / "pipeline.json"
    path.write_text(
        '{"pipeline":{"name":"demo","base_output_dir":"runs"},"drive":{"root_id":"x"}}',
        encoding="utf-8",
    )
    paths = load_pipeline_config(path).paths

    paths.ensure_stage_dirs()

    assert {item.name for item in paths.pipeline_root.iterdir()} == {
        "scan",
        "documents",
        "events",
        "shifts",
        "enrichment",
        "aggregation",
    }
