from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from src.pipeline_paths import PipelineStep, build_pipelines_paths


def test_pipeline_paths_ensure_creates_requested_stage_dir(tmp_path: Path) -> None:
    paths = build_pipelines_paths(
        root_prefix="unit-test",
        base_output=tmp_path,
        create_dirs=False,
    )

    assert not paths.scan_output.exists()

    paths.ensure("scan")

    assert paths.scan_output.is_dir()


def test_pipeline_paths_ensure_rejects_unknown_step(tmp_path: Path) -> None:
    paths = build_pipelines_paths(
        root_prefix="unit-test",
        base_output=tmp_path,
        create_dirs=False,
    )

    with pytest.raises(ValueError, match="Unknown pipeline step 'unknown'"):
        paths.ensure(cast(PipelineStep, "unknown"))
