from __future__ import annotations

from importlib import import_module
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from cartellino_parser.models import StageReport, TurniAfternoonLongExportRequest  # noqa: E402


def test_turni_afternoon_long_export_cli_delegates_to_public_pipeline_client(
    monkeypatch,
    tmp_path: Path,
) -> None:
    module = import_module("cartellino_parser.turni_afternoon_long_export.cli")
    captured: dict[str, object] = {}

    class FakeClient:
        def export_afternoon_long_shifts(self, request):
            captured["request"] = request
            return StageReport(
                stage="turni_afternoon_long_export",
                stats={
                    "employees_total": 1,
                    "files_total": 1,
                    "files_processed": 1,
                    "files_error": 0,
                    "rows_total": 2,
                    "rows_selected": 1,
                },
            )

    monkeypatch.setattr(module, "PipelineClient", FakeClient)
    monkeypatch.setattr(module, "setup_logging", lambda _verbose: None)

    rc = module.main(
        [
            "--enriched-dir",
            str(tmp_path / "enrichment"),
            "--pairs-dir",
            str(tmp_path / "shifts"),
            "--out-dir",
            str(tmp_path / "afternoon_long"),
            "--report-json",
            str(tmp_path / "afternoon_long" / "turni_afternoon_long_export.report.json"),
        ]
    )

    assert rc == 0
    request = captured["request"]
    assert isinstance(request, TurniAfternoonLongExportRequest)
    assert request.enriched_dir == str(tmp_path / "enrichment")
    assert request.pairs_dir == str(tmp_path / "shifts")
    assert request.output_dir == str(tmp_path / "afternoon_long")
    assert request.report_json == str(
        tmp_path / "afternoon_long" / "turni_afternoon_long_export.report.json"
    )
