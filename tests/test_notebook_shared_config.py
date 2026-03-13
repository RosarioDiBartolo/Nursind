from __future__ import annotations

import json
from pathlib import Path

from src.notebooks.shared_config import load_notebook_context


def test_load_notebook_context_resolves_canonical_pipeline_paths(tmp_path: Path) -> None:
    config_path = tmp_path / "shared_config.json"
    config_path.write_text(
        json.dumps(
            {
                "root_id": "drive-root-123",
                "base_output_dir": "custom-output",
            }
        ),
        encoding="utf-8",
    )

    ctx = load_notebook_context(config_path)

    assert ctx.root_id == "drive-root-123"
    assert ctx.root_prefix == "drive-root-123"
    assert ctx.paths.scan_output.name == "scan"
    assert ctx.paths.documents_output.name == "documents"
    assert ctx.paths.events_output.name == "events"
    assert ctx.paths.shifts_output.name == "shifts"
    assert ctx.paths.enrichment_output.name == "enrichment"
    assert ctx.paths.aggregation_output.name == "aggregation"
    assert Path(ctx.base_output_dir).name == "custom-output"
