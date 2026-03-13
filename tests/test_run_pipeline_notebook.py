from __future__ import annotations

import json
from pathlib import Path


def test_run_pipeline_notebook_uses_shared_config_and_canonical_paths() -> None:
    notebook_path = Path(__file__).resolve().parents[1] / "src" / "notebooks" / "run_pipeline.ipynb"
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))

    source = "\n".join("".join(cell.get("source", [])) for cell in notebook["cells"])

    assert "load_notebook_context" in source
    assert 'ctx.step("timbrature_missing_report")' in source
    assert "paths.documents_output" in source
    assert "TimbratureMissingReportOptions" in source
    assert "text_extraction_output" not in source
