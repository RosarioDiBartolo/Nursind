from __future__ import annotations

import json
from pathlib import Path

from cartellino_parser.notebooks.shared_config import load_notebook_context


def test_load_notebook_context_resolves_canonical_pipeline_paths(tmp_path: Path) -> None:
    config_path = tmp_path / "pipeline_config.json"
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
    assert ctx.paths.layout.scan_dir.name == "scan"
    assert ctx.paths.layout.documents_dir.name == "documents"
    assert ctx.paths.layout.events_dir.name == "events"
    assert ctx.paths.layout.shifts_dir.name == "shifts"
    assert ctx.paths.layout.enrichment_dir.name == "enrichment"
    assert ctx.paths.layout.aggregation_dir.name == "aggregation"
    assert Path(ctx.base_output_dir).name == "custom-output"
    assert not ctx.paths.layout.pipeline_root.exists()

    scan_cfg = ctx.step("scan")
    assert scan_cfg["included_name"] == "included.index.json"
    assert scan_cfg["filtered_name"] == "filtered.index.json"
    assert scan_cfg["report_name"] == "scan_directory.report.json"

