from __future__ import annotations

import importlib
from pathlib import Path

from src.pipeline_paths import build_pipeline_paths


def test_extract_documents_parse_options_uses_central_defaults() -> None:
    from src.extract_documents_from_index.options import parse_options

    options = parse_options([])
    defaults = build_pipeline_paths().extract_documents

    assert options.out == str(defaults.dir)
    assert options.index == str(defaults.input_index)
    assert options.included == str(defaults.included_index)
    assert options.excluded == str(defaults.excluded_index)
    assert options.report == str(defaults.report_json)


def test_extract_events_parse_options_resolves_report_relative_to_output_dir(tmp_path: Path) -> None:
    from src.extract_events_from_documents.options import parse_options

    output_dir = tmp_path / "events"
    options = parse_options(
        [
            "--input-dir",
            str(tmp_path / "documents"),
            "--output-dir",
            str(output_dir),
            "--report-json",
            "custom.report.json",
        ]
    )

    assert options.output_dir == str(output_dir)
    assert options.report_json == str(output_dir / "custom.report.json")


def test_timbrature_parse_options_resolves_relative_outputs_from_pipeline_dir(tmp_path: Path) -> None:
    from src.timbrature_missing_report.options import parse_options

    pipeline_dir = tmp_path / "pipeline"
    options = parse_options(
        [
            "--pipeline-dir",
            str(pipeline_dir),
            "--report-json",
            "audit.json",
            "--summary-csv",
            "summary.csv",
            "--findings-csv",
            "findings.csv",
            "--coverage-csv",
            "coverage.csv",
        ]
    )

    assert options.pipeline_dir == str(pipeline_dir)
    assert options.report_json == str(pipeline_dir / "audit.json")
    assert options.summary_csv == str(pipeline_dir / "summary.csv")
    assert options.findings_csv == str(pipeline_dir / "findings.csv")
    assert options.coverage_csv == str(pipeline_dir / "coverage.csv")


def test_extract_documents_options_import_is_side_effect_free(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OUTPUT_BASE_DIR", "import-safe-output")
    monkeypatch.delenv("OUTPUT_ROOT_PREFIX", raising=False)
    monkeypatch.delenv("PIPELINE_ROOT_PREFIX", raising=False)
    monkeypatch.delenv("DRIVE_ROOT_FOLDER_ID", raising=False)

    module = importlib.import_module("src.extract_documents_from_index.options")
    importlib.reload(module)

    assert not (tmp_path / "import-safe-output").exists()
