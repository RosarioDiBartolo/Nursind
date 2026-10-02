from __future__ import annotations

import json
from pathlib import Path

import pytest

import core.config
from core.config import load_pipeline_config
from core.errors import ConfigurationError


def test_config_resolves_relative_output_from_repository_root(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        json.dumps(
            {
                "pipeline": {"name": "demo"},
                "drive": {"root_id": "drive-root", "folder_name": "drive folder"},
                "steps": {"scan": {"workers": 3}},
            }
        ),
        encoding="utf-8",
    )

    config = load_pipeline_config(config_path)

    assert config.paths.pipeline_root == tmp_path / "demo.out_dir"
    assert (
        config.paths.scan_included_index
        == tmp_path / "demo.out_dir" / "scan" / "included.index.json"
    )
    assert config.step("scan") == {"workers": 3}


def test_config_defaults_pipeline_name_from_drive_folder(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        json.dumps(
            {
                "pipeline": {},
                "drive": {"root_id": "drive-root", "folder_name": "Drive Folder"},
            }
        ),
        encoding="utf-8",
    )

    config = load_pipeline_config(config_path)

    assert config.name == "Drive Folder"
    assert config.paths.pipeline_root == tmp_path / "Drive Folder.out_dir"


def test_config_fetches_drive_folder_name_when_pipeline_block_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".git").mkdir()
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        json.dumps({"drive": {"root_id": "drive-root"}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        core.config,
        "_fetch_drive_folder_name",
        lambda root_id: "Fetched Drive Folder",
    )

    config = load_pipeline_config(config_path)

    assert config.name == "Fetched Drive Folder"
    assert config.paths.pipeline_root == tmp_path / "Fetched Drive Folder.out_dir"


def test_pipeline_name_overrides_drive_folder_name(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(
        json.dumps(
            {
                "pipeline": {"name": "manual name"},
                "drive": {"root_id": "drive-root", "folder_name": "Drive Folder"},
            }
        ),
        encoding="utf-8",
    )

    config = load_pipeline_config(config_path)

    assert config.name == "manual name"
    assert config.paths.pipeline_root == tmp_path / "manual name.out_dir"


def test_config_rejects_missing_required_values(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    path = tmp_path / "pipeline.json"
    path.write_text('{"pipeline": {}, "drive": {}}', encoding="utf-8")

    with pytest.raises(ConfigurationError, match="drive.root_id"):
        load_pipeline_config(path)


def test_paths_create_only_canonical_stage_directories(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    path = tmp_path / "pipeline.json"
    path.write_text(
        '{"pipeline":{"name":"demo"},"drive":{"root_id":"x"}}',
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
