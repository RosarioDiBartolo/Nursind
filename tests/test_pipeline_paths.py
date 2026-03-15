from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from src.pipeline_path_types import PipelineStage
from src.pipeline_paths import (
    build_pipeline_paths,
    with_extract_events_overrides,
    with_scan_overrides,
)


def test_build_pipeline_paths_resolves_canonical_graph(tmp_path: Path) -> None:
    paths = build_pipeline_paths(
        root_prefix="unit-test",
        base_output=tmp_path,
        create_dirs=False,
    )

    assert paths.layout.pipeline_root == tmp_path / "unit-test"
    assert paths.layout.scan_dir == tmp_path / "unit-test" / "scan"
    assert paths.layout.documents_dir == tmp_path / "unit-test" / "documents"
    assert paths.layout.events_dir == tmp_path / "unit-test" / "events"
    assert paths.layout.shifts_dir == tmp_path / "unit-test" / "shifts"
    assert paths.layout.enrichment_dir == tmp_path / "unit-test" / "enrichment"
    assert paths.layout.aggregation_dir == tmp_path / "unit-test" / "aggregation"

    assert paths.scan.included_index == paths.layout.scan_dir / "included.index.json"
    assert paths.extract_documents.input_index == paths.scan.included_index
    assert paths.extract_events.input_dir == paths.extract_documents.dir
    assert paths.filter_midnight.dir == paths.extract_events.dir
    assert paths.filter_midnight.input_dir == paths.extract_events.dir
    assert paths.filter_midnight.events_csv == paths.extract_events.events_csv
    assert paths.pair_employee.input_dir == paths.filter_midnight.dir
    assert paths.pair_employee.events_csv == paths.filter_midnight.cleaned_events_csv
    assert paths.turni_enrichment.input_dir == paths.pair_employee.dir
    assert paths.turni_employee_summary.input_dir == paths.turni_enrichment.dir


def test_build_pipeline_paths_resolves_special_scopes(tmp_path: Path) -> None:
    paths = build_pipeline_paths(
        root_prefix="unit-test",
        base_output=tmp_path,
        create_dirs=False,
    )

    assert paths.parser_recall_audit.root_dir == tmp_path
    assert paths.parser_recall_audit.suspicious_csv == tmp_path / "suspicious_pages.csv"
    assert paths.timbrature_missing_report.pipeline_dir == tmp_path / "unit-test"
    assert paths.timbrature_missing_report.report_json == (
        tmp_path / "unit-test" / "missing_timbrature.report.json"
    )


def test_pipeline_paths_ensure_creates_requested_stage_dir(tmp_path: Path) -> None:
    paths = build_pipeline_paths(
        root_prefix="unit-test",
        base_output=tmp_path,
        create_dirs=False,
    )

    assert not paths.layout.scan_dir.exists()

    paths.ensure("scan")

    assert paths.layout.scan_dir.is_dir()


def test_pipeline_paths_ensure_rejects_unknown_stage(tmp_path: Path) -> None:
    paths = build_pipeline_paths(
        root_prefix="unit-test",
        base_output=tmp_path,
        create_dirs=False,
    )

    with pytest.raises(ValueError, match="Unknown pipeline stage 'unknown'"):
        paths.ensure(cast(PipelineStage, "unknown"))


def test_step_dir_override_moves_owned_artifacts_and_downstream_inputs(tmp_path: Path) -> None:
    base = build_pipeline_paths(
        root_prefix="unit-test",
        base_output=tmp_path,
        create_dirs=False,
    )

    overridden = with_extract_events_overrides(
        base,
        dir=tmp_path / "custom-events",
    )

    assert overridden.extract_events.dir == tmp_path / "custom-events"
    assert overridden.extract_events.report_json == tmp_path / "custom-events" / "extract_events.report.json"
    assert overridden.filter_midnight.dir == tmp_path / "custom-events"
    assert overridden.filter_midnight.input_dir == tmp_path / "custom-events"
    assert overridden.pair_employee.input_dir == tmp_path / "custom-events"


def test_artifact_override_wins_over_step_dir_override(tmp_path: Path) -> None:
    base = build_pipeline_paths(
        root_prefix="unit-test",
        base_output=tmp_path,
        create_dirs=False,
    )

    overridden = with_scan_overrides(
        base,
        dir=tmp_path / "custom-scan",
        report_json=tmp_path / "outside" / "scan.json",
    )

    assert overridden.scan.dir == tmp_path / "custom-scan"
    assert overridden.scan.included_index == tmp_path / "custom-scan" / "included.index.json"
    assert overridden.scan.report_json == tmp_path / "outside" / "scan.json"


def test_overrides_do_not_persist_between_independent_builds(tmp_path: Path) -> None:
    base = build_pipeline_paths(
        root_prefix="unit-test",
        base_output=tmp_path,
        create_dirs=False,
    )
    overridden = with_extract_events_overrides(
        base,
        dir=tmp_path / "custom-events",
    )
    fresh = build_pipeline_paths(
        root_prefix="unit-test",
        base_output=tmp_path,
        create_dirs=False,
    )

    assert overridden.extract_events.dir == tmp_path / "custom-events"
    assert fresh.extract_events.dir == tmp_path / "unit-test" / "events"
